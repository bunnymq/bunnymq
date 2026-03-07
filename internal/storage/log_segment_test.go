package storage

import (
	"path/filepath"
	"testing"
)

func makeTestBatch(t *testing.T, value string) []byte {
	t.Helper()
	b, err := EncodeBatch([]Record{{TimestampMs: 1000, Value: []byte(value)}})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	return b
}

func TestLogSegment_AppendRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	seg, err := OpenLogSegment(path, 0, true)
	if err != nil {
		t.Fatalf("OpenLogSegment: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })

	b1 := makeTestBatch(t, "hello")
	b2 := makeTestBatch(t, "world")

	pos1, err := seg.Append(b1)
	if err != nil {
		t.Fatalf("Append b1: %v", err)
	}
	pos2, err := seg.Append(b2)
	if err != nil {
		t.Fatalf("Append b2: %v", err)
	}

	got1, err := seg.ReadAt(pos1, len(b1))
	if err != nil {
		t.Fatalf("ReadAt b1: %v", err)
	}
	if string(got1) != string(b1) {
		t.Errorf("b1 mismatch")
	}

	got2, err := seg.ReadAt(pos2, len(b2))
	if err != nil {
		t.Fatalf("ReadAt b2: %v", err)
	}
	if string(got2) != string(b2) {
		t.Errorf("b2 mismatch")
	}
}

func TestLogSegment_AppendReturnsPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	seg, err := OpenLogSegment(path, 0, true)
	if err != nil {
		t.Fatalf("OpenLogSegment: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })

	b1 := makeTestBatch(t, "first")

	pos1, err := seg.Append(b1)
	if err != nil {
		t.Fatalf("Append b1: %v", err)
	}
	if pos1 != 0 {
		t.Errorf("expected first append at position 0, got %d", pos1)
	}

	b2 := makeTestBatch(t, "second")
	pos2, err := seg.Append(b2)
	if err != nil {
		t.Fatalf("Append b2: %v", err)
	}
	if pos2 != int64(len(b1)) {
		t.Errorf("expected second append at position %d, got %d", len(b1), pos2)
	}
}

func TestLogSegment_ScanFrom(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	seg, err := OpenLogSegment(path, 0, true)
	if err != nil {
		t.Fatalf("OpenLogSegment: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })

	batches := [][]byte{
		makeTestBatch(t, "a"),
		makeTestBatch(t, "bb"),
		makeTestBatch(t, "ccc"),
	}
	for _, b := range batches {
		if _, err := seg.Append(b); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	var visited []string
	err = seg.ScanFrom(0, func(batchBytes []byte, pos int64) bool {
		b, err := DecodeBatch(batchBytes)
		if err != nil {
			t.Errorf("DecodeBatch at pos %d: %v", pos, err)
			return false
		}
		visited = append(visited, string(b.Records[0].Value))
		return true
	})
	if err != nil {
		t.Fatalf("ScanFrom: %v", err)
	}
	if len(visited) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(visited))
	}
	for i, v := range []string{"a", "bb", "ccc"} {
		if visited[i] != v {
			t.Errorf("batch[%d]: expected %q, got %q", i, v, visited[i])
		}
	}
}

func TestLogSegment_ScanFrom_PartialStart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	seg, err := OpenLogSegment(path, 0, true)
	if err != nil {
		t.Fatalf("OpenLogSegment: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })

	b1 := makeTestBatch(t, "first")
	b2 := makeTestBatch(t, "second")
	b3 := makeTestBatch(t, "third")

	if _, err := seg.Append(b1); err != nil {
		t.Fatalf("Append b1: %v", err)
	}
	pos2, err := seg.Append(b2)
	if err != nil {
		t.Fatalf("Append b2: %v", err)
	}
	if _, err := seg.Append(b3); err != nil {
		t.Fatalf("Append b3: %v", err)
	}

	var visited []string
	err = seg.ScanFrom(pos2, func(batchBytes []byte, pos int64) bool {
		b, err := DecodeBatch(batchBytes)
		if err != nil {
			t.Errorf("DecodeBatch at pos %d: %v", pos, err)
			return false
		}
		visited = append(visited, string(b.Records[0].Value))
		return true
	})
	if err != nil {
		t.Fatalf("ScanFrom: %v", err)
	}
	if len(visited) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(visited))
	}
	if visited[0] != "second" || visited[1] != "third" {
		t.Errorf("unexpected batches: %v", visited)
	}
}

func TestLogSegment_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	seg, err := OpenLogSegment(path, 0, true)
	if err != nil {
		t.Fatalf("OpenLogSegment: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })

	b1 := makeTestBatch(t, "keep")
	b2 := makeTestBatch(t, "discard")

	if _, err := seg.Append(b1); err != nil {
		t.Fatalf("Append b1: %v", err)
	}
	if _, err := seg.Append(b2); err != nil {
		t.Fatalf("Append b2: %v", err)
	}

	if err := seg.Truncate(int64(len(b1))); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if seg.logSizeVal.Load() != int64(len(b1)) {
		t.Errorf("logSize = %d, want %d", seg.logSizeVal.Load(), len(b1))
	}

	var visited []string
	err = seg.ScanFrom(0, func(batchBytes []byte, pos int64) bool {
		b, err := DecodeBatch(batchBytes)
		if err != nil {
			t.Errorf("DecodeBatch: %v", err)
			return false
		}
		visited = append(visited, string(b.Records[0].Value))
		return true
	})
	if err != nil {
		t.Fatalf("ScanFrom after Truncate: %v", err)
	}
	if len(visited) != 1 || visited[0] != "keep" {
		t.Errorf("expected only batch 'keep', got %v", visited)
	}
}

func TestLogSegment_ReadOnlyRejectsAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	f, err := OpenLogSegment(path, 0, true)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	seg, err := OpenLogSegment(path, 0, false)
	if err != nil {
		t.Fatalf("OpenLogSegment read-only: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })

	_, err = seg.Append(makeTestBatch(t, "x"))
	if err != ErrSegmentReadOnly {
		t.Errorf("expected ErrSegmentReadOnly, got %v", err)
	}
}

func TestLogSegment_Sync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	seg, err := OpenLogSegment(path, 0, true)
	if err != nil {
		t.Fatalf("OpenLogSegment: %v", err)
	}
	t.Cleanup(func() { _ = seg.Close() })

	if _, err := seg.Append(makeTestBatch(t, "sync-test")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}
