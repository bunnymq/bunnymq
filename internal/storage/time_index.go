package storage

import (
	"encoding/binary"
	"os"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

type timeEntry struct {
	timestampMs    int64
	relativeOffset int32
}

// TimeIndexSegment wraps a mmap'd .timeindex file with 12-byte entries
// (timestamp_ms int64 + relative_offset int32, big-endian). Active segments use
// PROT_READ|PROT_WRITE; sealed segments are remapped PROT_READ.
type TimeIndexSegment struct {
	file       *os.File
	data       []byte
	entryCount atomic.Int64
	baseOffset int64
	maxEntries int64
}

// OpenTimeIndex creates or opens a .timeindex file at path. The file is
// pre-allocated to ceil(segMaxBytes/indexSampleBytes)*12 bytes rounded up to
// the OS page size, then mmap'd PROT_READ|PROT_WRITE|MAP_SHARED.
func OpenTimeIndex(path string, baseOffset int64, segMaxBytes int64, indexSampleBytes int) (*TimeIndexSegment, error) {
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
	entryBytes := maxEntries * 12
	pageSize := int64(os.Getpagesize())
	allocSize := (entryBytes + pageSize - 1) / pageSize * pageSize

	if existingSize < allocSize {
		if err := preallocateIndex(f, allocSize); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(allocSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	seg := &TimeIndexSegment{
		file:       f,
		data:       data,
		baseOffset: baseOffset,
		maxEntries: maxEntries,
	}
	seg.entryCount.Store(existingSize / 12)
	return seg, nil
}

// Append writes a 12-byte entry at the next available slot. Data bytes are
// written before entryCount is incremented so concurrent Lookup callers never
// observe a slot before its bytes are visible.
func (s *TimeIndexSegment) Append(timestampMs int64, relativeOffset int32) error {
	count := s.entryCount.Load()
	if count >= s.maxEntries {
		return ErrIndexFull
	}
	off := count * 12
	binary.BigEndian.PutUint64(s.data[off:off+8], uint64(timestampMs))
	binary.BigEndian.PutUint32(s.data[off+8:off+12], uint32(relativeOffset))
	s.entryCount.Add(1)
	return nil
}

// Lookup binary-searches for the ceiling entry — the smallest entry whose
// timestampMs >= the given value. Returns (relativeOffset, true) if found,
// or (0, false) if all entries are older than timestampMs.
//
// Ceiling semantics are required because ReadByTime must start at the first
// batch that could contain records with timestamp >= the query, not the last
// batch before it.
func (s *TimeIndexSegment) Lookup(timestampMs int64) (relativeOffset int32, found bool) {
	count := s.entryCount.Load()
	if count == 0 {
		return 0, false
	}
	lo, hi := int64(0), count-1
	result := int64(-1)
	for lo <= hi {
		mid := (lo + hi) / 2
		off := mid * 12
		entryTs := int64(binary.BigEndian.Uint64(s.data[off : off+8]))
		if entryTs >= timestampMs {
			result = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	if result < 0 {
		return 0, false
	}
	off := result * 12
	return int32(binary.BigEndian.Uint32(s.data[off+8 : off+12])), true
}

// Seal truncates the file to entryCount×12 bytes, msyncs, munmaps, and
// remaps PROT_READ. After Seal, Append returns ErrIndexFull.
func (s *TimeIndexSegment) Seal() error {
	count := s.entryCount.Load()
	actualSize := count * 12
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
func (s *TimeIndexSegment) Rebuild(entries []timeEntry) {
	s.entryCount.Store(0)
	for i, e := range entries {
		off := int64(i) * 12
		binary.BigEndian.PutUint64(s.data[off:off+8], uint64(e.timestampMs))
		binary.BigEndian.PutUint32(s.data[off+8:off+12], uint32(e.relativeOffset))
	}
	s.entryCount.Store(int64(len(entries)))
}

// Close unmaps the mmap region and closes the file.
func (s *TimeIndexSegment) Close() error {
	if s.data != nil {
		if err := unix.Munmap(s.data); err != nil {
			return err
		}
		s.data = nil
	}
	return s.file.Close()
}
