package storage

import "errors"

var (
	ErrOffsetOutOfRange  = errors.New("offset out of range")
	ErrTimestampNotFound = errors.New("timestamp not found")
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

// FileStorage is a stub implementation of Storage backed by the segmented log.
type FileStorage struct{}

var _ Storage = (*FileStorage)(nil)

func (s *FileStorage) Append(batch []byte) (int64, error) {
	return 0, errors.New("not implemented")
}

func (s *FileStorage) Read(offset int64, maxBytes int) ([]byte, int64, error) {
	return nil, 0, errors.New("not implemented")
}

func (s *FileStorage) ReadByTime(timestampMs int64, maxBytes int) ([]byte, int64, error) {
	return nil, 0, errors.New("not implemented")
}

func (s *FileStorage) EarliestOffset() int64 { return 0 }

func (s *FileStorage) LatestOffset() int64 { return 0 }

func (s *FileStorage) EnforceRetention(retentionMs int64, retentionBytes int64) (int, error) {
	return 0, errors.New("not implemented")
}

func (s *FileStorage) NewDataCh() <-chan struct{} { return nil }

func (s *FileStorage) SetRetentionConfig(retentionMs int64, retentionBytes int64) {}

func (s *FileStorage) Sync() error { return errors.New("not implemented") }

func (s *FileStorage) TruncateTo(offset int64) error { return errors.New("not implemented") }

func (s *FileStorage) Close() error { return errors.New("not implemented") }
