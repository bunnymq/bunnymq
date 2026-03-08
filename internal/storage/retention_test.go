package storage

import (
	"context"
	"testing"
	"time"
)

// openStorageWithRoll creates a Storage that rolls into a new segment after
// each Append (SegmentMaxBytes=1 forces a roll as soon as any data is written).
func openStorageWithRoll(t *testing.T) *storageImpl {
	t.Helper()
	return openTestStorage(t, storageTestConfig(1, 4096))
}

// TestRetention_TimeExpired verifies that only the middle sealed segment is
// deleted when its successor's base_timestamp is older than the retention
// threshold and the surrounding segments are fresh.
//
// Layout after 3 appends with SegmentMaxBytes=1:
//
//	S0 (sealed, fresh ts) | S1 (sealed, fresh ts) | S2 (sealed, OLD ts) | S3 (active, empty)
//
// Time check per segment:
//
//	S0: next=S1 → fresh → keep
//	S1: next=S2 → OLD   → delete  ← only this one
//	S2: next=S3 → empty → skip   → keep
func TestRetention_TimeExpired(t *testing.T) {
	s := openStorageWithRoll(t)

	nowMs := time.Now().UnixMilli()
	veryOldMs := nowMs - 2*24*60*60*1000 // 2 days ago

	// Append 3 batches; each triggers a roll leaving 3 sealed + 1 empty active.
	for _, ts := range []int64{nowMs, nowMs, veryOldMs} {
		if _, err := s.Append(makeBatch(t, "v", ts)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	s.segMu.RLock()
	segsBefore := len(s.segments)
	s.segMu.RUnlock()
	if segsBefore < 3 {
		t.Fatalf("expected >= 3 segments, got %d", segsBefore)
	}

	retentionMs := int64(24 * 60 * 60 * 1000) // 1 day
	deleted, err := s.EnforceRetention(retentionMs, -1)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	s.segMu.RLock()
	segsAfter := len(s.segments)
	s.segMu.RUnlock()
	if segsAfter != segsBefore-1 {
		t.Fatalf("segment count = %d, want %d", segsAfter, segsBefore-1)
	}
}

// TestRetention_BytesOverCap verifies that oldest sealed segments are deleted
// when total log size exceeds retentionBytes, stopping as soon as the
// remaining total fits within the cap.
//
// Layout: 3 sealed segments each of size B, 1 empty active.
// retentionBytes = B  →  total=3B > B, delete S0 (2B>B), delete S1 (B≤B).
// Expected: 2 deleted.
func TestRetention_BytesOverCap(t *testing.T) {
	s := openStorageWithRoll(t)

	// Append the same batch 3 times so each sealed segment has equal LogSize.
	for range 3 {
		if _, err := s.Append(makeBatch(t, "x", 1000)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	s.segMu.RLock()
	segs := make([]*SegmentStorage, len(s.segments))
	copy(segs, s.segments)
	s.segMu.RUnlock()

	if len(segs) < 3 {
		t.Fatalf("expected >= 3 segments, got %d", len(segs))
	}

	// retentionBytes = size of one sealed segment; deletes exactly 2 oldest.
	retentionBytes := segs[0].LogSize()

	deleted, err := s.EnforceRetention(-1, retentionBytes)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
}

// TestRetention_ActiveNeverDeleted verifies that EnforceRetention is a no-op
// when only the active segment exists, even with the most aggressive settings.
func TestRetention_ActiveNeverDeleted(t *testing.T) {
	// Use large SegmentMaxBytes so no roll occurs.
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	if _, err := s.Append(makeBatch(t, "only", 1000)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	s.segMu.RLock()
	n := len(s.segments)
	s.segMu.RUnlock()
	if n != 1 {
		t.Fatalf("expected 1 segment, got %d", n)
	}

	// retentionMs=1ms and retentionBytes=0 would delete everything eligible.
	deleted, err := s.EnforceRetention(1, 0)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0 (active must never be deleted)", deleted)
	}
}

// TestRetention_NoneEligible verifies that segments are left untouched when
// all timestamps are fresh and total size is well under the bytes cap.
func TestRetention_NoneEligible(t *testing.T) {
	s := openStorageWithRoll(t)

	nowMs := time.Now().UnixMilli()
	for range 3 {
		if _, err := s.Append(makeBatch(t, "v", nowMs)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	s.segMu.RLock()
	segsBefore := len(s.segments)
	s.segMu.RUnlock()

	retentionMs := int64(7 * 24 * 60 * 60 * 1000) // 7 days — all batches are fresh
	retentionBytes := int64(1024 * 1024)            // 1 MiB — well above total

	deleted, err := s.EnforceRetention(retentionMs, retentionBytes)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}

	s.segMu.RLock()
	segsAfter := len(s.segments)
	s.segMu.RUnlock()
	if segsAfter != segsBefore {
		t.Fatalf("segment count changed from %d to %d, want no change", segsBefore, segsAfter)
	}
}

// TestRetention_UnionSemantics verifies that a segment is deleted when it
// exceeds the bytes cap even if its timestamps are fresh (union semantics:
// either criterion is sufficient, both are not required).
func TestRetention_UnionSemantics(t *testing.T) {
	s := openStorageWithRoll(t)

	nowMs := time.Now().UnixMilli()

	// 2 sealed segments + 1 empty active; both have fresh timestamps.
	for range 2 {
		if _, err := s.Append(makeBatch(t, "v", nowMs)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	s.segMu.RLock()
	segs := make([]*SegmentStorage, len(s.segments))
	copy(segs, s.segments)
	s.segMu.RUnlock()

	if len(segs) < 2 {
		t.Fatalf("expected >= 2 segments, got %d", len(segs))
	}

	// Time retention: 7 days — fresh batches are not expired.
	retentionMs := int64(7 * 24 * 60 * 60 * 1000)
	// Bytes retention: cap at 0 — oldest sealed must be deleted.
	retentionBytes := int64(0)

	deleted, err := s.EnforceRetention(retentionMs, retentionBytes)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if deleted == 0 {
		t.Fatal("deleted = 0, want > 0 (bytes criterion must trigger even when time does not)")
	}
}

// TestRetention_SetConfig_Applied verifies that values stored by
// SetRetentionConfig are loaded by the retention loop on the next tick.
func TestRetention_SetConfig_Applied(t *testing.T) {
	s := openStorageWithRoll(t)

	nowMs := time.Now().UnixMilli()
	for range 2 {
		if _, err := s.Append(makeBatch(t, "v", nowMs)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Default config lets everything through; set an aggressive one instead.
	s.SetRetentionConfig(-1, 0) // delete all sealed segments

	retMs := s.retentionMs.Load()
	retBytes := s.retentionBytes.Load()

	deleted, err := s.EnforceRetention(retMs, retBytes)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if deleted == 0 {
		t.Fatal("deleted = 0; SetRetentionConfig values were not applied")
	}
}

// TestRetention_LoopStopsOnContextCancel verifies that the background
// retention goroutine exits promptly when its context is cancelled.
func TestRetention_LoopStopsOnContextCancel(t *testing.T) {
	s := openTestStorage(t, storageTestConfig(128*1024*1024, 4096))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.startRetentionLoop(ctx, 10*time.Millisecond)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// expected: goroutine exited after context cancellation
	case <-time.After(2 * time.Second):
		t.Fatal("retention loop did not stop after context cancellation")
	}
}
