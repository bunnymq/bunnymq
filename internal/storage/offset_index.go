package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

var ErrIndexFull = errors.New("index full")

type offsetEntry struct {
	relativeOffset int32
	position       int32
}

// OffsetIndexSegment wraps a mmap'd .index file with 8-byte entries
// (relative_offset int32 + position int32, big-endian). Active segments use
// PROT_READ|PROT_WRITE; sealed segments are remapped PROT_READ.
type OffsetIndexSegment struct {
	file       *os.File
	data       []byte
	entryCount atomic.Int64
	baseOffset int64
	maxEntries int64
}

// OpenOffsetIndex creates or opens a .index file at path. The file is
// pre-allocated to ceil(segMaxBytes/indexSampleBytes)*8 bytes rounded up to
// the OS page size, then mmap'd PROT_READ|PROT_WRITE|MAP_SHARED.
// logger receives a warn when fallocate is unavailable and truncate is used instead.
func OpenOffsetIndex(path string, baseOffset int64, segMaxBytes int64, indexSampleBytes int, logger *zap.Logger) (*OffsetIndexSegment, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	existingSize := info.Size()

	maxEntries := (segMaxBytes + int64(indexSampleBytes) - 1) / int64(indexSampleBytes)
	entryBytes := maxEntries * 8
	pageSize := int64(os.Getpagesize())
	allocSize := (entryBytes + pageSize - 1) / pageSize * pageSize

	if existingSize < allocSize {
		fallback, err := preallocateIndex(f, allocSize)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if fallback {
			logger.Warn("fallocate not supported; fell back to write")
		}
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(allocSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	seg := &OffsetIndexSegment{
		file:       f,
		data:       data,
		baseOffset: baseOffset,
		maxEntries: maxEntries,
	}
	seg.entryCount.Store(existingSize / 8)
	return seg, nil
}

// Append writes an 8-byte entry at the next available slot. Data bytes are
// written before entryCount is incremented so concurrent Lookup callers never
// observe a slot before its bytes are visible.
func (s *OffsetIndexSegment) Append(relativeOffset int32, position int32) error {
	count := s.entryCount.Load()
	if count >= s.maxEntries {
		return ErrIndexFull
	}
	off := count * 8
	binary.BigEndian.PutUint32(s.data[off:off+4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(s.data[off+4:off+8], uint32(position))
	s.entryCount.Add(1)
	return nil
}

// Lookup binary-searches for the floor entry whose relativeOffset <= the given
// value and returns its log-file position. Returns (0, false) when no such
// entry exists (empty index or all entries exceed relativeOffset).
func (s *OffsetIndexSegment) Lookup(relativeOffset int32) (position int32, found bool) {
	count := s.entryCount.Load()
	if count == 0 {
		return 0, false
	}
	lo, hi := int64(0), count-1
	result := int64(-1)
	for lo <= hi {
		mid := (lo + hi) / 2
		off := mid * 8
		entryOff := int32(binary.BigEndian.Uint32(s.data[off : off+4]))
		if entryOff <= relativeOffset {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if result < 0 {
		return 0, false
	}
	off := result * 8
	return int32(binary.BigEndian.Uint32(s.data[off+4 : off+8])), true
}

// Seal truncates the file to entryCount×8 bytes, msyncs, munmaps, and
// remaps PROT_READ. After Seal, Append returns ErrIndexFull.
func (s *OffsetIndexSegment) Seal() error {
	count := s.entryCount.Load()
	actualSize := count * 8
	if err := s.file.Truncate(actualSize); err != nil {
		return err
	}
	if actualSize > 0 {
		if err := unix.Msync(s.data[:actualSize], unix.MS_SYNC); err != nil {
			return err
		}
	}
	if err := unix.Munmap(s.data); err != nil {
		return err
	}
	s.data = nil
	if actualSize > 0 {
		data, err := unix.Mmap(int(s.file.Fd()), 0, int(actualSize), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			return err
		}
		s.data = data
	}
	s.maxEntries = count
	return nil
}

// Rebuild resets entryCount to zero and writes all provided entries into the
// mmap region. Used by crash recovery to reconstruct the active segment index.
func (s *OffsetIndexSegment) Rebuild(entries []offsetEntry) {
	s.entryCount.Store(0)
	for i, e := range entries {
		off := int64(i) * 8
		binary.BigEndian.PutUint32(s.data[off:off+4], uint32(e.relativeOffset))
		binary.BigEndian.PutUint32(s.data[off+4:off+8], uint32(e.position))
	}
	s.entryCount.Store(int64(len(entries)))
}

// Close unmaps the mmap region and closes the file.
func (s *OffsetIndexSegment) Close() error {
	if s.data != nil {
		if err := unix.Munmap(s.data); err != nil {
			return err
		}
		s.data = nil
	}
	return s.file.Close()
}
