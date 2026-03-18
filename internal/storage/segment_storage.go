package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/config"
	"golang.org/x/sys/unix"
)

// SegmentStorage composes a LogSegment, OffsetIndexSegment, and TimeIndexSegment
// into a single unit for one segment triple (.log + .index + .timeindex).
type SegmentStorage struct {
	log                 *LogSegment
	offsetIdx           *OffsetIndexSegment
	timeIdx             *TimeIndexSegment
	baseOffset          int64
	sealed              bool
	bytesSinceLastIndex int64
	config              *config.StorageConfig
}

func segmentBasePath(dir string, baseOffset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d", baseOffset))
}

// NewSegmentStorage creates new .log, .index, and .timeindex files for an
// active segment with the given base offset. logger receives fallocate-fallback warns.
func NewSegmentStorage(dir string, baseOffset int64, cfg *config.StorageConfig, logger *zap.Logger) (*SegmentStorage, error) {
	base := segmentBasePath(dir, baseOffset)

	log, err := OpenLogSegment(base+".log", baseOffset, true)
	if err != nil {
		return nil, err
	}

	offsetIdx, err := OpenOffsetIndex(base+".index", baseOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes, logger)
	if err != nil {
		_ = log.Close()
		return nil, err
	}

	timeIdx, err := OpenTimeIndex(base+".timeindex", baseOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes, logger)
	if err != nil {
		_ = log.Close()
		_ = offsetIdx.Close()
		return nil, err
	}

	return &SegmentStorage{
		log:        log,
		offsetIdx:  offsetIdx,
		timeIdx:    timeIdx,
		baseOffset: baseOffset,
		config:     cfg,
	}, nil
}

// OpenSegmentStorage opens existing segment files. readonly=true opens for a
// sealed segment (PROT_READ index mmap, O_RDONLY log). readonly=false opens
// for an active segment (pre-allocated index mmap, O_RDWR log).
// logger receives fallocate-fallback warns on non-readonly open.
func OpenSegmentStorage(dir string, baseOffset int64, cfg *config.StorageConfig, readonly bool, logger *zap.Logger) (*SegmentStorage, error) {
	base := segmentBasePath(dir, baseOffset)

	log, err := OpenLogSegment(base+".log", baseOffset, !readonly)
	if err != nil {
		return nil, err
	}

	var offsetIdx *OffsetIndexSegment
	var timeIdx *TimeIndexSegment

	if readonly {
		offsetIdx, err = openOffsetIndexReadOnly(base+".index", baseOffset)
		if err != nil {
			_ = log.Close()
			return nil, err
		}
		timeIdx, err = openTimeIndexReadOnly(base+".timeindex", baseOffset)
		if err != nil {
			_ = log.Close()
			_ = offsetIdx.Close()
			return nil, err
		}
	} else {
		offsetIdx, err = OpenOffsetIndex(base+".index", baseOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes, logger)
		if err != nil {
			_ = log.Close()
			return nil, err
		}
		timeIdx, err = OpenTimeIndex(base+".timeindex", baseOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes, logger)
		if err != nil {
			_ = log.Close()
			_ = offsetIdx.Close()
			return nil, err
		}
	}

	return &SegmentStorage{
		log:        log,
		offsetIdx:  offsetIdx,
		timeIdx:    timeIdx,
		baseOffset: baseOffset,
		sealed:     readonly,
		config:     cfg,
	}, nil
}

// openOffsetIndexReadOnly opens a sealed .index file with O_RDONLY and maps
// it PROT_READ without preallocating (file is already truncated to actual size).
func openOffsetIndexReadOnly(path string, baseOffset int64) (*OffsetIndexSegment, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := info.Size()
	var data []byte
	if size > 0 {
		data, err = unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	seg := &OffsetIndexSegment{
		file:       f,
		data:       data,
		baseOffset: baseOffset,
		maxEntries: size / 8,
	}
	seg.entryCount.Store(size / 8)
	return seg, nil
}

// openTimeIndexReadOnly opens a sealed .timeindex file with O_RDONLY and maps
// it PROT_READ without preallocating (file is already truncated to actual size).
func openTimeIndexReadOnly(path string, baseOffset int64) (*TimeIndexSegment, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := info.Size()
	var data []byte
	if size > 0 {
		data, err = unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	seg := &TimeIndexSegment{
		file:       f,
		data:       data,
		baseOffset: baseOffset,
		maxEntries: size / 12,
	}
	seg.entryCount.Store(size / 12)
	return seg, nil
}

// Append writes batch to the log and conditionally records index entries when
// bytesSinceLastIndex crosses IndexSampleBytes. Returns the byte position
// at which the batch was written.
func (s *SegmentStorage) Append(batch []byte) (startPos int64, err error) {
	startPos, err = s.log.Append(batch)
	if err != nil {
		return 0, err
	}

	batchBaseOffset := int64(binary.BigEndian.Uint64(batch[0:8]))
	maxTimestamp := int64(binary.BigEndian.Uint64(batch[30:38]))
	relativeOffset := int32(batchBaseOffset - s.baseOffset)

	if s.bytesSinceLastIndex >= int64(s.config.IndexSampleBytes) {
		if err := s.offsetIdx.Append(relativeOffset, int32(startPos)); err != nil {
			return startPos, err
		}
		if err := s.timeIdx.Append(maxTimestamp, relativeOffset); err != nil {
			return startPos, err
		}
		s.bytesSinceLastIndex = 0
	} else {
		s.bytesSinceLastIndex += int64(len(batch))
	}

	return startPos, nil
}

// Read returns up to maxBytes of batch data starting at the batch whose range
// [base_offset, base_offset+record_count) contains offset. Returns
// (nil, offset, nil) when offset is past the end of this segment's data.
func (s *SegmentStorage) Read(offset int64, maxBytes int) ([]byte, int64, error) {
	relOffset := int32(offset - s.baseOffset)

	scanPos := int64(0)
	if pos, found := s.offsetIdx.Lookup(relOffset); found {
		scanPos = int64(pos)
	}

	var result []byte
	nextOffset := offset

	_ = s.log.ScanFrom(scanPos, func(batchBytes []byte, _ int64) bool {
		batchBaseOffset := int64(binary.BigEndian.Uint64(batchBytes[0:8]))
		recordCount := int64(binary.BigEndian.Uint32(batchBytes[12:16]))
		batchEndOffset := batchBaseOffset + recordCount

		if batchEndOffset <= offset {
			return true // batch ends before offset; keep scanning
		}

		// This batch covers offset. Stop if adding it would exceed maxBytes
		// (but always include at least one batch).
		if len(result) > 0 && len(result)+len(batchBytes) > maxBytes {
			return false
		}

		result = append(result, batchBytes...)
		nextOffset = batchEndOffset
		return true
	})

	return result, nextOffset, nil
}

// ReadByTime returns up to maxBytes of batch data starting at the first batch
// whose max_timestamp >= timestampMs. Returns (nil, 0, ErrTimestampNotFound)
// when no such batch is indexed in this segment.
func (s *SegmentStorage) ReadByTime(timestampMs int64, maxBytes int) ([]byte, int64, error) {
	relOffset, found := s.timeIdx.Lookup(timestampMs)
	if !found {
		return nil, 0, ErrTimestampNotFound
	}
	absoluteOffset := s.baseOffset + int64(relOffset)
	return s.Read(absoluteOffset, maxBytes)
}

// Seal executes the 6-step seal procedure: truncate+msync+remap the offset
// index, then the time index, then fsync the log.
func (s *SegmentStorage) Seal() error {
	if err := s.offsetIdx.Seal(); err != nil {
		return err
	}
	if err := s.timeIdx.Seal(); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.sealed = true
	return nil
}

// LogSize returns the number of bytes written to the log file.
func (s *SegmentStorage) LogSize() int64 {
	return s.log.logSizeVal.Load()
}

// BaseOffset returns the base offset of this segment.
func (s *SegmentStorage) BaseOffset() int64 {
	return s.baseOffset
}

// Close closes all underlying file descriptors and unmaps index files.
func (s *SegmentStorage) Close() error {
	if err := s.log.Close(); err != nil {
		return err
	}
	if err := s.offsetIdx.Close(); err != nil {
		return err
	}
	return s.timeIdx.Close()
}
