package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func openTestTimeIndex(t *testing.T, segMaxBytes int64, indexSampleBytes int) (*TimeIndexSegment, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.timeindex")
	idx, err := OpenTimeIndex(path, 0, segMaxBytes, indexSampleBytes)
	if err != nil {
		t.Fatalf("OpenTimeIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	return idx, path
}

func TestTimeIndex_AppendLookup(t *testing.T) {
	idx, _ := openTestTimeIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(100, 0); err != nil {
		t.Fatalf("Append 100: %v", err)
	}
	if err := idx.Append(200, 10); err != nil {
		t.Fatalf("Append 200: %v", err)
	}
	if err := idx.Append(300, 20); err != nil {
		t.Fatalf("Append 300: %v", err)
	}

	relOff, found := idx.Lookup(200)
	if !found {
		t.Fatal("Lookup(200): not found")
	}
	if relOff != 10 {
		t.Errorf("Lookup(200) = %d, want 10", relOff)
	}
}

func TestTimeIndex_CeilingLookup(t *testing.T) {
	idx, _ := openTestTimeIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(100, 0); err != nil {
		t.Fatalf("Append 100: %v", err)
	}
	if err := idx.Append(200, 10); err != nil {
		t.Fatalf("Append 200: %v", err)
	}

	// 150 is between 100 and 200 — ceiling is the entry at 200.
	relOff, found := idx.Lookup(150)
	if !found {
		t.Fatal("Lookup(150): not found")
	}
	if relOff != 10 {
		t.Errorf("Lookup(150) = %d, want 10 (entry at timestamp 200)", relOff)
	}
}

func TestTimeIndex_NoneFound(t *testing.T) {
	idx, _ := openTestTimeIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(100, 0); err != nil {
		t.Fatalf("Append 100: %v", err)
	}
	if err := idx.Append(200, 10); err != nil {
		t.Fatalf("Append 200: %v", err)
	}
	if err := idx.Append(300, 20); err != nil {
		t.Fatalf("Append 300: %v", err)
	}

	// 400 is past all entries — no ceiling exists.
	_, found := idx.Lookup(400)
	if found {
		t.Error("Lookup(400): expected found=false, got found=true")
	}
}

func TestTimeIndex_AllOlder(t *testing.T) {
	idx, _ := openTestTimeIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(50, 0); err != nil {
		t.Fatalf("Append 50: %v", err)
	}
	if err := idx.Append(75, 5); err != nil {
		t.Fatalf("Append 75: %v", err)
	}

	// All entries are < 100.
	_, found := idx.Lookup(100)
	if found {
		t.Error("Lookup(100) with all entries older: expected found=false, got found=true")
	}
}

func TestTimeIndex_Seal(t *testing.T) {
	idx, path := openTestTimeIndex(t, 128*1024*1024, 4096)

	if err := idx.Append(100, 0); err != nil {
		t.Fatalf("Append 100: %v", err)
	}
	if err := idx.Append(200, 10); err != nil {
		t.Fatalf("Append 200: %v", err)
	}

	if err := idx.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 24 {
		t.Errorf("file size after Seal = %d, want 24", info.Size())
	}

	if err := idx.Append(300, 20); err != ErrIndexFull {
		t.Errorf("Append after Seal: got %v, want ErrIndexFull", err)
	}
}

func TestTimeIndex_Rebuild(t *testing.T) {
	idx, _ := openTestTimeIndex(t, 128*1024*1024, 4096)

	entries := []timeEntry{
		{timestampMs: 100, relativeOffset: 0},
		{timestampMs: 200, relativeOffset: 10},
		{timestampMs: 300, relativeOffset: 20},
	}
	idx.Rebuild(entries)

	cases := []struct {
		ts   int64
		want int32
	}{
		{100, 0},
		{200, 10},
		{300, 20},
	}
	for _, c := range cases {
		relOff, found := idx.Lookup(c.ts)
		if !found {
			t.Errorf("Lookup(%d) after Rebuild: not found", c.ts)
			continue
		}
		if relOff != c.want {
			t.Errorf("Lookup(%d) after Rebuild = %d, want %d", c.ts, relOff, c.want)
		}
	}
}
