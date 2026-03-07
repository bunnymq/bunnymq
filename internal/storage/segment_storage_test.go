package storage

import (
	"bytes"
	"os"
	"testing"

	"github.com/bunnymq/bunnymq/internal/config"
)

func testConfig(indexSampleBytes int) *config.StorageConfig {
	return &config.StorageConfig{
		SegmentMaxBytes:  128 * 1024 * 1024,
		IndexSampleBytes: indexSampleBytes,
	}
}

func makeBatch(t *testing.T, value string, timestampMs int64) []byte {
	t.Helper()
	b, err := EncodeBatch([]Record{{TimestampMs: timestampMs, Value: []byte(value)}})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	return b
}

// writeOffset stamps batch[0:8] with the given base offset (mimicking what
// Storage.Append does before writing to disk).
func writeOffset(batch []byte, baseOffset int64) []byte {
	out := make([]byte, len(batch))
	copy(out, batch)
	putInt64BE(out[0:8], baseOffset)
	return out
}

func putInt64BE(b []byte, v int64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func newTestSegment(t *testing.T, indexSampleBytes int) (*SegmentStorage, string) {
	t.Helper()
	dir := t.TempDir()
	seg, err := NewSegmentStorage(dir, 0, testConfig(indexSampleBytes))
	if err != nil {
		t.Fatalf("NewSegmentStorage: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })
	return seg, dir
}

func TestSegmentStorage_AppendRead(t *testing.T) {
	seg, _ := newTestSegment(t, 4096)

	b1 := writeOffset(makeBatch(t, "a", 1000), 0)
	b2 := writeOffset(makeBatch(t, "b", 2000), 1)
	b3 := writeOffset(makeBatch(t, "c", 3000), 2)

	if _, err := seg.Append(b1); err != nil {
		t.Fatalf("Append b1: %v", err)
	}
	if _, err := seg.Append(b2); err != nil {
		t.Fatalf("Append b2: %v", err)
	}
	if _, err := seg.Append(b3); err != nil {
		t.Fatalf("Append b3: %v", err)
	}

	want := append(append(b1, b2...), b3...)
	got, nextOff, err := seg.Read(0, len(want)+1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Read bytes mismatch: got %d bytes, want %d bytes", len(got), len(want))
	}
	if nextOff != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOff)
	}
}

func TestSegmentStorage_ReadFromMiddle(t *testing.T) {
	seg, _ := newTestSegment(t, 4096)

	b1 := writeOffset(makeBatch(t, "a", 1000), 0)
	b2 := writeOffset(makeBatch(t, "b", 2000), 1)
	b3 := writeOffset(makeBatch(t, "c", 3000), 2)

	for _, b := range [][]byte{b1, b2, b3} {
		if _, err := seg.Append(b); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	want := append(b2, b3...)
	got, nextOff, err := seg.Read(1, len(want)+1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Read from middle: got %d bytes, want %d", len(got), len(want))
	}
	if nextOff != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOff)
	}
}

func TestSegmentStorage_MaxBytesLimit(t *testing.T) {
	seg, _ := newTestSegment(t, 4096)

	b1 := writeOffset(makeBatch(t, "first", 1000), 0)
	b2 := writeOffset(makeBatch(t, "second", 2000), 1)
	b3 := writeOffset(makeBatch(t, "third", 3000), 2)

	for _, b := range [][]byte{b1, b2, b3} {
		if _, err := seg.Append(b); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	got, nextOff, err := seg.Read(0, len(b1))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, b1) {
		t.Errorf("MaxBytes: got %d bytes, want %d", len(got), len(b1))
	}
	if nextOff != 1 {
		t.Errorf("nextOffset = %d, want 1", nextOff)
	}
}

func TestSegmentStorage_IndexSampling(t *testing.T) {
	// Set a small threshold so we cross it within 10 batches.
	const threshold = 100
	seg, _ := newTestSegment(t, threshold)

	// Append 10 batches. Each batch is ~50 bytes, so we cross the threshold
	// around every 2 batches.
	for i := range 10 {
		b := writeOffset(makeBatch(t, "v", int64(i*1000)), int64(i))
		if _, err := seg.Append(b); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Index must not contain an entry for every batch.
	count := seg.offsetIdx.entryCount.Load()
	if count == 0 {
		t.Error("expected at least one index entry")
	}
	if count == 10 {
		t.Errorf("expected sparse index (< 10 entries), got %d", count)
	}

	// Verify the same sparseness on the time index.
	tCount := seg.timeIdx.entryCount.Load()
	if tCount == 0 {
		t.Error("expected at least one time index entry")
	}
	if tCount == 10 {
		t.Errorf("expected sparse time index (< 10 entries), got %d", tCount)
	}

	// Both indexes must agree on entry count.
	if count != tCount {
		t.Errorf("offset index entries (%d) != time index entries (%d)", count, tCount)
	}
}

func TestSegmentStorage_Seal(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(4096)
	seg, err := NewSegmentStorage(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegmentStorage: %v", err)
	}

	b1 := writeOffset(makeBatch(t, "x", 1000), 0)
	if _, err := seg.Append(b1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	logSizeBefore := seg.LogSize()

	if err := seg.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if !seg.sealed {
		t.Error("sealed flag not set after Seal")
	}

	// Log size must be unchanged.
	if seg.LogSize() != logSizeBefore {
		t.Errorf("logSize changed after Seal: %d → %d", logSizeBefore, seg.LogSize())
	}

	// Index files must be truncated to their actual entry sizes.
	idxInfo, err := os.Stat(dir + "/00000000000000000000.index")
	if err != nil {
		t.Fatalf("stat index: %v", err)
	}
	offsetEntries := seg.offsetIdx.entryCount.Load()
	wantIdxSize := offsetEntries * 8
	if idxInfo.Size() != wantIdxSize {
		t.Errorf("index file size = %d, want %d (%d entries)", idxInfo.Size(), wantIdxSize, offsetEntries)
	}

	timeInfo, err := os.Stat(dir + "/00000000000000000000.timeindex")
	if err != nil {
		t.Fatalf("stat timeindex: %v", err)
	}
	timeEntries := seg.timeIdx.entryCount.Load()
	wantTimeSize := timeEntries * 12
	if timeInfo.Size() != wantTimeSize {
		t.Errorf("timeindex file size = %d, want %d (%d entries)", timeInfo.Size(), wantTimeSize, timeEntries)
	}

	_ = seg.Close()
}

func TestSegmentStorage_ReadByTime(t *testing.T) {
	// Use a small threshold so we get index entries for our batches.
	const threshold = 1
	seg, _ := newTestSegment(t, threshold)

	b1 := writeOffset(makeBatch(t, "early", 1000), 0)
	b2 := writeOffset(makeBatch(t, "mid", 2000), 1)
	b3 := writeOffset(makeBatch(t, "late", 3000), 2)

	for _, b := range [][]byte{b1, b2, b3} {
		if _, err := seg.Append(b); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// ReadByTime at 2000 should return b2 and b3 (first batch with max_timestamp >= 2000).
	got, _, err := seg.ReadByTime(2000, 1<<20)
	if err != nil {
		t.Fatalf("ReadByTime: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("ReadByTime returned empty result")
	}
	// The result must start with b2 (not b1).
	if !bytes.HasPrefix(got, b2) {
		t.Errorf("ReadByTime should start with b2; got prefix %x, b2=%x", got[:min(len(got), len(b2))], b2)
	}
}

func TestSegmentStorage_ReadEmptyReturnsNil(t *testing.T) {
	seg, _ := newTestSegment(t, 4096)

	b1 := writeOffset(makeBatch(t, "only", 1000), 0)
	if _, err := seg.Append(b1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// nextOffset after one record-per-batch is 1.
	got, retOff, err := seg.Read(1, 4096)
	if err != nil {
		t.Fatalf("Read at nextOffset: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil bytes for out-of-range read, got %d bytes", len(got))
	}
	if retOff != 1 {
		t.Errorf("returned offset = %d, want 1", retOff)
	}
}

