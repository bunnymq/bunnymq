package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRecovery_CleanShutdown(t *testing.T) {
	dir := t.TempDir()
	cfg := storageTestConfig(128*1024*1024, 4096)

	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := range 5 {
		if _, err := s.Append(makeBatch(t, fmt.Sprintf("msg%d", i), int64(i*1000))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	wantLatest := s.LatestOffset()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open after clean shutdown: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got := s2.LatestOffset(); got != wantLatest {
		t.Fatalf("LatestOffset = %d, want %d", got, wantLatest)
	}

	data, next, err := s2.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if data == nil {
		t.Fatal("Read returned nil")
	}
	if next != wantLatest {
		t.Fatalf("Read nextOffset = %d, want %d", next, wantLatest)
	}
}

func TestRecovery_PartialHeader(t *testing.T) {
	dir := t.TempDir()
	cfg := storageTestConfig(128*1024*1024, 4096)

	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 4 {
		if _, err := s.Append(makeBatch(t, fmt.Sprintf("msg%d", i), int64(i*1000))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	wantLatest := s.LatestOffset()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate crash: append 10 bytes (partial header — less than the 38-byte minimum).
	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open log for corruption: %v", err)
	}
	_, werr := f.Write(make([]byte, 10))
	_ = f.Close()
	if werr != nil {
		t.Fatalf("write partial header: %v", werr)
	}

	s2, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open after partial header: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got := s2.LatestOffset(); got != wantLatest {
		t.Fatalf("LatestOffset = %d, want %d", got, wantLatest)
	}
}

func TestRecovery_CRCMismatch(t *testing.T) {
	dir := t.TempDir()
	cfg := storageTestConfig(128*1024*1024, 4096)

	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 3 {
		if _, err := s.Append(makeBatch(t, fmt.Sprintf("msg%d", i), int64(i*1000))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	wantLatest := s.LatestOffset()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Craft a batch whose records[] don't match the stored CRC (flip first byte of records[]).
	rawBatch := makeBatch(t, "corrupt", 4000)
	corruptBatch := make([]byte, len(rawBatch))
	copy(corruptBatch, rawBatch)
	binary.BigEndian.PutUint64(corruptBatch[0:8], uint64(wantLatest))
	corruptBatch[38] ^= 0xFF // flip first byte of records[]; CRC in header no longer matches

	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open log for corruption: %v", err)
	}
	_, werr := f.Write(corruptBatch)
	_ = f.Close()
	if werr != nil {
		t.Fatalf("write corrupt batch: %v", werr)
	}

	s2, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open after CRC mismatch: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got := s2.LatestOffset(); got != wantLatest {
		t.Fatalf("LatestOffset = %d, want %d", got, wantLatest)
	}

	data, next, err := s2.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if data == nil {
		t.Fatal("Read returned nil")
	}
	if next != wantLatest {
		t.Fatalf("Read nextOffset = %d, want %d", next, wantLatest)
	}
}

func TestRecovery_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	cfg := storageTestConfig(128*1024*1024, 4096)

	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open on empty dir: %v", err)
	}
	defer func() { _ = s.Close() }()

	if got := s.LatestOffset(); got != 0 {
		t.Fatalf("LatestOffset = %d, want 0", got)
	}

	s.segMu.RLock()
	segCount := len(s.segments)
	s.segMu.RUnlock()
	if segCount != 1 {
		t.Fatalf("segment count = %d, want 1", segCount)
	}
}

func TestRecovery_MultipleSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := storageTestConfig(128*1024*1024, 4096)

	// Sealed segment 0: batches at base_offsets 0, 1, 2.
	seg0, err := NewSegmentStorage(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegmentStorage 0: %v", err)
	}
	for i := range 3 {
		b := writeOffset(makeBatch(t, fmt.Sprintf("s0-%d", i), int64(i*1000)), int64(i))
		if _, err := seg0.Append(b); err != nil {
			t.Fatalf("seg0.Append %d: %v", i, err)
		}
	}
	if err := seg0.Seal(); err != nil {
		t.Fatalf("seg0.Seal: %v", err)
	}
	_ = seg0.Close()

	// Sealed segment 1: batches at base_offsets 3, 4, 5.
	seg1, err := NewSegmentStorage(dir, 3, cfg)
	if err != nil {
		t.Fatalf("NewSegmentStorage 3: %v", err)
	}
	for i := range 3 {
		b := writeOffset(makeBatch(t, fmt.Sprintf("s1-%d", i), int64((3+i)*1000)), int64(3+i))
		if _, err := seg1.Append(b); err != nil {
			t.Fatalf("seg1.Append %d: %v", i, err)
		}
	}
	if err := seg1.Seal(); err != nil {
		t.Fatalf("seg1.Seal: %v", err)
	}
	_ = seg1.Close()

	// Active segment 2: one clean batch at base_offset 6.
	seg2, err := NewSegmentStorage(dir, 6, cfg)
	if err != nil {
		t.Fatalf("NewSegmentStorage 6: %v", err)
	}
	b6 := writeOffset(makeBatch(t, "active", 6000), 6)
	if _, err := seg2.Append(b6); err != nil {
		t.Fatalf("seg2.Append: %v", err)
	}
	_ = seg2.Close()

	// Simulate crash: append partial bytes after the clean batch.
	logPath := filepath.Join(dir, "00000000000000000006.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open active log: %v", err)
	}
	_, werr := f.Write(make([]byte, 10))
	_ = f.Close()
	if werr != nil {
		t.Fatalf("write partial bytes: %v", werr)
	}

	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	// base_offset 6 + 1 record = next offset 7.
	const wantLatest = int64(7)
	if got := s.LatestOffset(); got != wantLatest {
		t.Fatalf("LatestOffset = %d, want %d", got, wantLatest)
	}

	// All sealed data and the active batch must be readable.
	off := int64(0)
	for off < wantLatest {
		data, next, err := s.Read(off, 1<<20)
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		if data == nil {
			t.Fatalf("Read(%d) returned nil before LatestOffset", off)
		}
		if next <= off {
			t.Fatalf("Read(%d) made no progress: next=%d", off, next)
		}
		off = next
	}
}

func TestRecovery_IndexSizeValidation(t *testing.T) {
	dir := t.TempDir()
	// Small segment and index thresholds so at least one index entry is written.
	cfg := storageTestConfig(4096, 1)

	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 3 {
		if _, err := s.Append(makeBatch(t, fmt.Sprintf("msg%d", i), int64(i*1000))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	wantLatest := s.LatestOffset()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Truncate .index to a non-multiple-of-8 size to simulate a crash during ftruncate.
	idxPath := filepath.Join(dir, "00000000000000000000.index")
	info, err := os.Stat(idxPath)
	if err != nil {
		t.Fatalf("stat index: %v", err)
	}
	oddSize := int64(7)
	if info.Size() < oddSize {
		oddSize = 1
	}
	if err := os.Truncate(idxPath, oddSize); err != nil {
		t.Fatalf("truncate index to odd size: %v", err)
	}

	s2, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open after index corruption: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got := s2.LatestOffset(); got != wantLatest {
		t.Fatalf("LatestOffset = %d, want %d", got, wantLatest)
	}

	data, next, err := s2.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if data == nil {
		t.Fatal("Read returned nil after index rebuild")
	}
	if next != wantLatest {
		t.Fatalf("Read nextOffset = %d, want %d", next, wantLatest)
	}
}

func TestRecovery_AppendAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := storageTestConfig(128*1024*1024, 4096)

	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 4 {
		if _, err := s.Append(makeBatch(t, fmt.Sprintf("msg%d", i), int64(i*1000))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	wantAfterCrash := s.LatestOffset()

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate crash: append 10 bytes (partial header).
	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	_, werr := f.Write(make([]byte, 10))
	_ = f.Close()
	if werr != nil {
		t.Fatalf("write partial bytes: %v", werr)
	}

	s2, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got := s2.LatestOffset(); got != wantAfterCrash {
		t.Fatalf("LatestOffset after recovery = %d, want %d", got, wantAfterCrash)
	}

	// Append one more batch; it must land at nextOffset with correct base_offset.
	baseOff, err := s2.Append(makeBatch(t, "after-recovery", 9000))
	if err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if baseOff != wantAfterCrash {
		t.Fatalf("Append base_offset = %d, want %d", baseOff, wantAfterCrash)
	}
	if got := s2.LatestOffset(); got != wantAfterCrash+1 {
		t.Fatalf("LatestOffset after append = %d, want %d", got, wantAfterCrash+1)
	}

	// The new batch must be readable at its base_offset.
	data, next, err := s2.Read(baseOff, 1<<20)
	if err != nil {
		t.Fatalf("Read after recovery append: %v", err)
	}
	if data == nil {
		t.Fatalf("Read(%d) returned nil", baseOff)
	}
	if next != wantAfterCrash+1 {
		t.Fatalf("Read nextOffset = %d, want %d", next, wantAfterCrash+1)
	}
}
