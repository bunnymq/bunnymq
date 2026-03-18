package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"go.uber.org/zap"
)

func openTestIndex(t *testing.T, segMaxBytes int64, indexSampleBytes int) (*OffsetIndexSegment, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")
	idx, err := OpenOffsetIndex(path, 0, segMaxBytes, indexSampleBytes, zap.NewNop())
	if err != nil {
		t.Fatalf("OpenOffsetIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	return idx, path
}

func TestOffsetIndex_AppendLookup(t *testing.T) {
	idx, _ := openTestIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(0, 100); err != nil {
		t.Fatalf("Append 0: %v", err)
	}
	if err := idx.Append(10, 500); err != nil {
		t.Fatalf("Append 10: %v", err)
	}
	if err := idx.Append(20, 1000); err != nil {
		t.Fatalf("Append 20: %v", err)
	}

	cases := []struct {
		relOff int32
		want   int32
	}{
		{0, 100},
		{10, 500},
		{20, 1000},
	}
	for _, c := range cases {
		pos, found := idx.Lookup(c.relOff)
		if !found {
			t.Errorf("Lookup(%d): not found", c.relOff)
			continue
		}
		if pos != c.want {
			t.Errorf("Lookup(%d) = %d, want %d", c.relOff, pos, c.want)
		}
	}
}

func TestOffsetIndex_FloorLookup(t *testing.T) {
	idx, _ := openTestIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(0, 100); err != nil {
		t.Fatalf("Append 0: %v", err)
	}
	if err := idx.Append(10, 500); err != nil {
		t.Fatalf("Append 10: %v", err)
	}

	// Between entry 0 and entry 10 → should return entry at 0.
	pos, found := idx.Lookup(5)
	if !found {
		t.Fatal("Lookup(5): not found")
	}
	if pos != 100 {
		t.Errorf("Lookup(5) = %d, want 100", pos)
	}

	// Between entry 10 and beyond → should return entry at 10.
	pos, found = idx.Lookup(15)
	if !found {
		t.Fatal("Lookup(15): not found")
	}
	if pos != 500 {
		t.Errorf("Lookup(15) = %d, want 500", pos)
	}
}

func TestOffsetIndex_EmptyLookup(t *testing.T) {
	idx, _ := openTestIndex(t, 128*1024*1024, 4096)

	pos, found := idx.Lookup(0)
	if found {
		t.Errorf("Lookup on empty index: found=true, pos=%d", pos)
	}
}

func TestOffsetIndex_Seal(t *testing.T) {
	idx, path := openTestIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(0, 100); err != nil {
		t.Fatalf("Append 0: %v", err)
	}
	if err := idx.Append(10, 500); err != nil {
		t.Fatalf("Append 10: %v", err)
	}

	if err := idx.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 16 {
		t.Errorf("file size after Seal = %d, want 16", info.Size())
	}

	if err := idx.Append(20, 1000); err != ErrIndexFull {
		t.Errorf("Append after Seal: got %v, want ErrIndexFull", err)
	}
}

func TestOffsetIndex_PreAllocSize(t *testing.T) {
	const segMaxBytes = 128 * 1024 * 1024
	const indexSampleBytes = 4096

	_, path := openTestIndex(t, segMaxBytes, indexSampleBytes)

	maxEntries := int64((segMaxBytes + indexSampleBytes - 1) / indexSampleBytes)
	entryBytes := maxEntries * 8
	pageSize := int64(os.Getpagesize())
	wantSize := (entryBytes + pageSize - 1) / pageSize * pageSize

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != wantSize {
		t.Errorf("pre-alloc file size = %d, want %d", info.Size(), wantSize)
	}
}

func TestOffsetIndex_Rebuild(t *testing.T) {
	idx, _ := openTestIndex(t, 128*1024*1024, 4096)

	entries := []offsetEntry{
		{relativeOffset: 0, position: 100},
		{relativeOffset: 10, position: 500},
		{relativeOffset: 20, position: 1000},
	}
	idx.Rebuild(entries)

	cases := []struct {
		relOff int32
		want   int32
	}{
		{0, 100},
		{10, 500},
		{20, 1000},
	}
	for _, c := range cases {
		pos, found := idx.Lookup(c.relOff)
		if !found {
			t.Errorf("Lookup(%d) after Rebuild: not found", c.relOff)
			continue
		}
		if pos != c.want {
			t.Errorf("Lookup(%d) after Rebuild = %d, want %d", c.relOff, pos, c.want)
		}
	}
}

func TestOffsetIndex_ConcurrentAppendLookup(t *testing.T) {
	const N = 200
	idx, _ := openTestIndex(t, 128*1024*1024, 4096)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			if err := idx.Append(int32(i), int32(i*10)); err != nil {
				t.Errorf("Append(%d): %v", i, err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < N*50; i++ {
			idx.Lookup(int32(i % (N + 1)))
		}
	}()

	wg.Wait()
}
