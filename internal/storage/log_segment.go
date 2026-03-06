package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

var ErrSegmentReadOnly = errors.New("segment is read-only")

// LogSegment wraps a single .log file for append and read operations.
type LogSegment struct {
	file       *os.File
	logSize    int64
	baseOffset int64
	readonly   bool
}

// OpenLogSegment opens or creates a .log file at path with the given baseOffset.
// create=true opens O_WRONLY|O_APPEND|O_CREATE (active segment).
// create=false opens O_RDONLY (sealed segment).
func OpenLogSegment(path string, baseOffset int64, create bool) (*LogSegment, error) {
	var (
		f   *os.File
		err error
	)
	if create {
		// O_RDWR so ReadAt (pread) works on the active segment while O_APPEND
		// preserves kernel-level atomic write semantics.
		f, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	} else {
		f, err = os.OpenFile(path, os.O_RDONLY, 0o644)
	}
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &LogSegment{
		file:       f,
		logSize:    info.Size(),
		baseOffset: baseOffset,
		readonly:   !create,
	}, nil
}

// Append writes batch atomically to the log file using O_APPEND semantics.
// Returns the byte position at which the batch starts (i.e., logSize before the write).
func (s *LogSegment) Append(batch []byte) (int64, error) {
	if s.readonly {
		return 0, ErrSegmentReadOnly
	}
	startPos := s.logSize
	if _, err := s.file.Write(batch); err != nil {
		return 0, err
	}
	s.logSize += int64(len(batch))
	return startPos, nil
}

// ReadAt reads exactly length bytes starting at pos using pread semantics.
func (s *LogSegment) ReadAt(pos int64, length int) ([]byte, error) {
	buf := make([]byte, length)
	n, err := s.file.ReadAt(buf, pos)
	if n == length {
		return buf, nil
	}
	if err == io.EOF || err == nil {
		return buf[:n], io.ErrUnexpectedEOF
	}
	return nil, err
}

// ScanFrom reads batches sequentially from startPos to end of file, calling
// callback with each batch's raw bytes and its start position. Stops early if
// callback returns false.
func (s *LogSegment) ScanFrom(startPos int64, callback func(batchBytes []byte, pos int64) bool) error {
	pos := startPos
	for pos < s.logSize {
		// Read enough to get batch_length at bytes [8,12].
		header, err := s.ReadAt(pos, 12)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		batchLen := int(binary.BigEndian.Uint32(header[8:12]))
		if batchLen < 12 || pos+int64(batchLen) > s.logSize {
			break
		}
		batch, err := s.ReadAt(pos, batchLen)
		if err != nil {
			return err
		}
		if !callback(batch, pos) {
			return nil
		}
		pos += int64(batchLen)
	}
	return nil
}

// Truncate truncates the log file to pos bytes and updates logSize.
func (s *LogSegment) Truncate(pos int64) error {
	if err := s.file.Truncate(pos); err != nil {
		return err
	}
	s.logSize = pos
	return nil
}

// Sync fsyncs the log file.
func (s *LogSegment) Sync() error {
	return s.file.Sync()
}

// Close closes the underlying file.
func (s *LogSegment) Close() error {
	return s.file.Close()
}
