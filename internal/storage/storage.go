package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/config"
)

var (
	ErrOffsetOutOfRange  = errors.New("offset out of range")
	ErrTimestampNotFound = errors.New("timestamp not found")
	ErrStorageClosed     = errors.New("storage is closed")
)

// Storage is the persistence interface for a single partition replica.
// Append is called only from the Partition FSM's Update(), which dragonboat
// guarantees is never concurrent with itself. Read, ReadByTime, EarliestOffset,
// and LatestOffset may be called concurrently from Lookup() and fetch goroutines.
// EnforceRetention and SetRetentionConfig are called from the background goroutine.
// Close must be called only after all other callers have stopped.
type Storage interface {
	// Append writes batch to the active segment and returns the base offset
	// assigned to this batch. Storage overwrites batch[0:8] with the assigned
	// base_offset (big-endian int64) before writing to disk.
	Append(batch []byte) (baseOffset int64, err error)

	// Read returns up to maxBytes of serialised batch data starting at the batch
	// whose range [base_offset, base_offset+record_count) contains offset.
	Read(offset int64, maxBytes int) (records []byte, nextOffset int64, err error)

	// ReadByTime returns up to maxBytes of batch data starting at the first batch
	// whose base_timestamp >= timestampMs.
	ReadByTime(timestampMs int64, maxBytes int) (records []byte, nextOffset int64, err error)

	// EarliestOffset returns the base_offset of the oldest available batch.
	EarliestOffset() int64

	// LatestOffset returns the next offset to be assigned.
	LatestOffset() int64

	// EnforceRetention deletes the oldest sealed segments that violate the
	// retention policy.
	EnforceRetention(retentionMs int64, retentionBytes int64) (deletedSegments int, err error)

	// NewDataCh returns a channel closed by the next successful Append.
	NewDataCh() <-chan struct{}

	// SetRetentionConfig updates the retention parameters used by the background
	// goroutine.
	SetRetentionConfig(retentionMs int64, retentionBytes int64)

	// Sync fsyncs the active log file.
	Sync() error

	// TruncateTo removes all batch data at or after the given base_offset.
	TruncateTo(offset int64) error

	// Close msyncs active index files, fsyncs the active log file, and closes
	// all file descriptors.
	Close() error
}

// OpenOption configures a storageImpl during Open.
type OpenOption func(*storageImpl)

// WithMetrics wires a StorageMetrics instance and partition labels into the storage.
func WithMetrics(m *StorageMetrics, topic, partitionID string) OpenOption {
	return func(s *storageImpl) {
		s.metrics = m
		s.topic = topic
		s.partitionID = partitionID
	}
}

// WithLogger wires a *zap.Logger into the storage. A child logger scoped with
// module, topic, and partition_id fields is created inside Open.
func WithLogger(logger *zap.Logger) OpenOption {
	return func(s *storageImpl) {
		s.logger = logger
	}
}

type storageImpl struct {
	dir         string
	topic       string
	partitionID string
	metrics     *StorageMetrics
	logger      *zap.Logger
	segments    []*SegmentStorage
	nextOffset  int64
	segMu       sync.RWMutex
	newDataCh   chan struct{}
	chanMu      sync.Mutex
	config      *config.StorageConfig
	retCancel   context.CancelFunc
	retentionMs    atomic.Int64
	retentionBytes atomic.Int64
	closed         bool
	closeMu        sync.Mutex
}

var _ Storage = (*storageImpl)(nil)

// Open enumerates and recovers segments in dir, starts the retention goroutine,
// and returns a ready Storage. Optional OpenOption values (e.g. WithMetrics,
// WithLogger) may be passed to attach observability without changing existing callers.
func Open(dir string, cfg *config.StorageConfig, opts ...OpenOption) (*storageImpl, error) {
	s := &storageImpl{
		dir:       dir,
		newDataCh: make(chan struct{}),
		config:    cfg,
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.logger == nil {
		s.logger = zap.NewNop()
	}
	loggerFields := []zap.Field{zap.String("module", "storage")}
	if s.topic != "" {
		loggerFields = append(loggerFields,
			zap.String("topic", s.topic),
			zap.String("partition_id", s.partitionID),
		)
	}
	s.logger = s.logger.With(loggerFields...)

	recoveryStart := time.Now()
	segments, nextOffset, crcErrors, err := recoverStorage(dir, cfg, s.logger)
	if err != nil {
		return nil, err
	}
	recoveryDur := time.Since(recoveryStart)

	s.segments = segments
	s.nextOffset = nextOffset

	var earliestOff int64
	if len(segments) > 0 {
		earliestOff = segments[0].BaseOffset()
	}
	s.metrics.recordRecovery(s.topic, s.partitionID, recoveryDur, len(segments), earliestOff, nextOffset, crcErrors)

	s.logger.Info("storage opened",
		zap.Int("segment_count", len(segments)),
		zap.Int64("earliest_offset", earliestOff),
		zap.Int64("latest_offset", nextOffset),
	)

	ctx, cancel := context.WithCancel(context.Background())
	s.retCancel = cancel
	s.retentionMs.Store(cfg.DefaultRetentionMs)
	s.retentionBytes.Store(cfg.DefaultRetentionBytes)

	interval := time.Duration(cfg.RetentionCheckIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	go s.startRetentionLoop(ctx, interval)

	return s, nil
}

func (s *storageImpl) active() *SegmentStorage {
	return s.segments[len(s.segments)-1]
}

func (s *storageImpl) roll() error {
	if err := s.active().Seal(); err != nil {
		s.logger.Error("index msync failed on segment seal", zap.Error(err))
		return err
	}
	newSeg, err := NewSegmentStorage(s.dir, s.nextOffset, s.config, s.logger)
	if err != nil {
		return err
	}
	s.segments = append(s.segments, newSeg)
	return nil
}

func (s *storageImpl) Append(batch []byte) (int64, error) {
	s.closeMu.Lock()
	closed := s.closed
	s.closeMu.Unlock()
	if closed {
		return 0, ErrStorageClosed
	}

	start := time.Now()
	s.segMu.Lock()
	baseOffset := s.nextOffset
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	if _, err := s.active().Append(batch); err != nil {
		s.segMu.Unlock()
		s.logger.Error("log write failed", zap.Error(err))
		return 0, err
	}
	recordCount := int64(binary.BigEndian.Uint32(batch[12:16]))
	s.nextOffset += recordCount
	rolled := false
	var oldBase, newBase, bytesWritten int64
	if s.active().LogSize() >= s.config.SegmentMaxBytes {
		oldBase = s.active().BaseOffset()
		bytesWritten = s.active().LogSize()
		if err := s.roll(); err != nil {
			s.segMu.Unlock()
			return 0, err
		}
		newBase = s.active().BaseOffset()
		rolled = true
	}
	activeBytes := s.active().LogSize()
	segCount := len(s.segments)
	latestOff := s.nextOffset
	s.segMu.Unlock()

	dur := time.Since(start)
	s.metrics.recordAppend(s.topic, s.partitionID, len(batch), dur)
	s.metrics.recordAppendGauges(s.topic, s.partitionID, latestOff, activeBytes)
	if rolled {
		s.metrics.recordRoll(s.topic, s.partitionID, segCount)
		s.logger.Info("segment rolled",
			zap.Int64("old_base_offset", oldBase),
			zap.Int64("new_base_offset", newBase),
			zap.Int64("bytes_written", bytesWritten),
		)
	}

	s.logger.Debug("batch appended",
		zap.Int64("base_offset", baseOffset),
		zap.Int("batch_bytes", len(batch)),
	)

	s.chanMu.Lock()
	old := s.newDataCh
	s.newDataCh = make(chan struct{})
	s.chanMu.Unlock()
	close(old)

	return baseOffset, nil
}

func (s *storageImpl) Read(offset int64, maxBytes int) ([]byte, int64, error) {
	start := time.Now()
	defer func() { s.metrics.recordRead(s.topic, s.partitionID, time.Since(start)) }()

	s.segMu.RLock()
	segs := make([]*SegmentStorage, len(s.segments))
	copy(segs, s.segments)
	nextOffset := s.nextOffset
	s.segMu.RUnlock()

	if len(segs) == 0 {
		return nil, offset, nil
	}
	if offset < segs[0].BaseOffset() {
		return nil, 0, ErrOffsetOutOfRange
	}
	if offset >= nextOffset {
		return nil, offset, nil
	}

	// Find last segment whose BaseOffset <= offset.
	lo, hi := 0, len(segs)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if segs[mid].BaseOffset() <= offset {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	records, next, err := segs[lo].Read(offset, maxBytes)
	s.logger.Debug("read request served",
		zap.Int64("offset", offset),
		zap.Int("bytes_returned", len(records)),
	)
	return records, next, err
}

func (s *storageImpl) ReadByTime(timestampMs int64, maxBytes int) ([]byte, int64, error) {
	s.segMu.RLock()
	segs := make([]*SegmentStorage, len(s.segments))
	copy(segs, s.segments)
	s.segMu.RUnlock()

	for _, seg := range segs {
		data, next, err := seg.ReadByTime(timestampMs, maxBytes)
		if err == ErrTimestampNotFound {
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		if data != nil {
			return data, next, nil
		}
	}
	return nil, 0, ErrTimestampNotFound
}

func (s *storageImpl) EarliestOffset() int64 {
	s.segMu.RLock()
	defer s.segMu.RUnlock()
	if len(s.segments) == 0 {
		return 0
	}
	return s.segments[0].BaseOffset()
}

func (s *storageImpl) LatestOffset() int64 {
	s.segMu.RLock()
	defer s.segMu.RUnlock()
	return s.nextOffset
}

func (s *storageImpl) NewDataCh() <-chan struct{} {
	s.chanMu.Lock()
	defer s.chanMu.Unlock()
	return s.newDataCh
}

func (s *storageImpl) SetRetentionConfig(retentionMs int64, retentionBytes int64) {
	s.retentionMs.Store(retentionMs)
	s.retentionBytes.Store(retentionBytes)
}

func (s *storageImpl) Sync() error {
	s.segMu.RLock()
	defer s.segMu.RUnlock()
	return s.active().log.Sync()
}

func (s *storageImpl) TruncateTo(offset int64) error {
	s.segMu.Lock()
	defer s.segMu.Unlock()

	if offset == s.nextOffset {
		return nil
	}
	if offset > s.nextOffset {
		return fmt.Errorf("TruncateTo(%d) > LatestOffset(%d)", offset, s.nextOffset)
	}

	segs := s.segments
	if len(segs) == 0 {
		return nil
	}

	// Find last segment whose BaseOffset <= offset.
	lo := len(segs) - 1
	for lo > 0 && segs[lo].BaseOffset() > offset {
		lo--
	}

	// Close and delete segments after lo.
	for i := lo + 1; i < len(segs); i++ {
		_ = segs[i].Close()
		base := segmentBasePath(s.dir, segs[i].BaseOffset())
		_ = os.Remove(base + ".log")
		_ = os.Remove(base + ".index")
		_ = os.Remove(base + ".timeindex")
	}
	s.segments = segs[:lo+1]
	activeSeg := s.segments[lo]

	// Sealed segments must be reopened for writing.
	if activeSeg.sealed {
		basePath := segmentBasePath(s.dir, activeSeg.baseOffset)
		if err := activeSeg.log.Close(); err != nil {
			return err
		}
		newLog, err := OpenLogSegment(basePath+".log", activeSeg.baseOffset, true)
		if err != nil {
			return err
		}
		activeSeg.log = newLog
		activeSeg.sealed = false
	}

	// Find byte position for offset by scanning the log.
	truncPos, err := findBytePosForOffset(activeSeg, offset)
	if err != nil {
		return err
	}
	if err := activeSeg.log.Truncate(truncPos); err != nil {
		return err
	}

	// Rebuild indexes after truncation.
	if err := rebuildSegmentIndexes(s.dir, activeSeg, s.config, s.logger); err != nil {
		return err
	}

	s.nextOffset = offset
	return nil
}

func (s *storageImpl) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	s.retCancel()

	s.segMu.Lock()
	segs := s.segments
	s.segMu.Unlock()

	var firstErr error
	for i, seg := range segs {
		if i == len(segs)-1 && !seg.sealed {
			if err := seg.Seal(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
