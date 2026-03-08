package storage

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bunnymq/bunnymq/internal/config"
)

func storageTestConfig(segMaxBytes int64, indexSampleBytes int) *config.StorageConfig {
	return &config.StorageConfig{
		SegmentMaxBytes:  segMaxBytes,
		IndexSampleBytes: indexSampleBytes,
	}
}

func openTestStorage(t *testing.T, cfg *config.StorageConfig) *storageImpl {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStorage_AppendRead(t *testing.T) {
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	var appended [][]byte
	for i := range 5 {
		b := makeBatch(t, fmt.Sprintf("msg%d", i), int64(i*1000))
		if _, err := s.Append(b); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		appended = append(appended, b)
	}

	data, next, err := s.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if data == nil {
		t.Fatal("Read returned nil data")
	}

	// Reassemble expected bytes (appended slices have their offsets overwritten by Append).
	var want []byte
	for _, b := range appended {
		want = append(want, b...)
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("data mismatch: got %d bytes, want %d bytes", len(data), len(want))
	}
	if next != 5 {
		t.Fatalf("nextOffset = %d, want 5", next)
	}
}

func TestStorage_SegmentRoll(t *testing.T) {
	// 1-byte threshold forces a roll after every append.
	s := openTestStorage(t, storageTestConfig(1, 4096))

	for i := range 3 {
		b := makeBatch(t, fmt.Sprintf("v%d", i), int64(i))
		if _, err := s.Append(b); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	s.segMu.RLock()
	segCount := len(s.segments)
	s.segMu.RUnlock()

	if segCount < 2 {
		t.Fatalf("expected multiple segments after roll, got %d", segCount)
	}
}

func TestStorage_NewDataCh_Closes(t *testing.T) {
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	ch := s.NewDataCh()
	if _, err := s.Append(makeBatch(t, "x", 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	select {
	case <-ch:
		// expected: channel closed after Append
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed after Append")
	}
}

func TestStorage_NewDataCh_Replace(t *testing.T) {
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	ch1 := s.NewDataCh()
	if _, err := s.Append(makeBatch(t, "x", 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// ch1 must now be closed.
	select {
	case <-ch1:
	case <-time.After(2 * time.Second):
		t.Fatal("ch1 not closed")
	}

	// NewDataCh returns a fresh, open channel.
	ch2 := s.NewDataCh()
	if ch2 == ch1 {
		t.Fatal("NewDataCh returned same channel after Append")
	}
	select {
	case <-ch2:
		t.Fatal("new channel already closed before next Append")
	default:
	}
}

func TestStorage_EarliestLatestOffset(t *testing.T) {
	// Use tiny threshold so segment roll happens after each append.
	s := openTestStorage(t, storageTestConfig(1, 4096))

	if got := s.EarliestOffset(); got != 0 {
		t.Fatalf("EarliestOffset before append = %d, want 0", got)
	}
	if got := s.LatestOffset(); got != 0 {
		t.Fatalf("LatestOffset before append = %d, want 0", got)
	}

	for i := range 3 {
		if _, err := s.Append(makeBatch(t, "v", int64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// EarliestOffset stays at 0 (no retention deletion happened).
	if got := s.EarliestOffset(); got != 0 {
		t.Fatalf("EarliestOffset after roll = %d, want 0", got)
	}
	if got := s.LatestOffset(); got != 3 {
		t.Fatalf("LatestOffset = %d, want 3", got)
	}
}

func TestStorage_TruncateTo(t *testing.T) {
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	var offsets [3]int64
	for i := range 3 {
		off, err := s.Append(makeBatch(t, fmt.Sprintf("v%d", i), int64(i)))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		offsets[i] = off
	}
	// offsets[2] is the base_offset of batch 2.
	if err := s.TruncateTo(offsets[2]); err != nil {
		t.Fatalf("TruncateTo: %v", err)
	}

	if got := s.LatestOffset(); got != offsets[2] {
		t.Fatalf("LatestOffset = %d, want %d", got, offsets[2])
	}

	data, next, err := s.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read after truncate: %v", err)
	}
	if next != offsets[2] {
		t.Fatalf("nextOffset after truncate = %d, want %d", next, offsets[2])
	}
	_ = data
}

func TestStorage_OffsetOutOfRange(t *testing.T) {
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	if _, err := s.Append(makeBatch(t, "x", 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// EarliestOffset is 0; reading at -1 must return ErrOffsetOutOfRange.
	_, _, err := s.Read(-1, 1<<20)
	if err != ErrOffsetOutOfRange {
		t.Fatalf("Read(-1): got %v, want ErrOffsetOutOfRange", err)
	}
}

func TestStorage_ConcurrentReadAppend(t *testing.T) {
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			b := makeBatch(t, fmt.Sprintf("m%d", i), int64(i))
			if _, err := s.Append(b); err != nil {
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		off := int64(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			data, next, _ := s.Read(off, 1<<16)
			if data != nil {
				off = next
			}
			runtime.Gosched()
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestStorage_Close(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, storageTestConfig(128*1024*1024, 4096))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = s.Append(makeBatch(t, "x", 1))
	if err != ErrStorageClosed {
		t.Fatalf("Append after Close: got %v, want ErrStorageClosed", err)
	}
}
