package client

import (
	"testing"
	"time"
)

func TestMetaCache_TTLExpiry(t *testing.T) {
	mc := NewMetaCache(5 * time.Second)

	meta := &TopicMeta{
		PartitionCount: 3,
		Leaders:        map[int32]string{0: "a:1", 1: "b:2", 2: "c:3"},
		FetchedAt:      time.Now().Add(-6 * time.Second), // already expired
	}
	mc.Put("topic1", meta)

	if got := mc.Get("topic1"); got != nil {
		t.Error("expected nil for expired entry, got non-nil")
	}
}

func TestMetaCache_SetLeader(t *testing.T) {
	mc := NewMetaCache(60 * time.Second)

	fetchedAt := time.Now()
	meta := &TopicMeta{
		PartitionCount: 3,
		Leaders:        map[int32]string{0: "a:1", 1: "b:2", 2: "c:3"},
		FetchedAt:      fetchedAt,
	}
	mc.Put("topic1", meta)

	mc.SetLeader("topic1", 1, "new-leader:9")

	got := mc.Get("topic1")
	if got == nil {
		t.Fatal("Get returned nil after SetLeader")
	}
	if got.Leaders[1] != "new-leader:9" {
		t.Errorf("partition 1 leader = %q, want %q", got.Leaders[1], "new-leader:9")
	}
	if got.Leaders[0] != "a:1" {
		t.Errorf("partition 0 leader = %q, want unchanged %q", got.Leaders[0], "a:1")
	}
	if got.Leaders[2] != "c:3" {
		t.Errorf("partition 2 leader = %q, want unchanged %q", got.Leaders[2], "c:3")
	}
	if !got.FetchedAt.Equal(fetchedAt) {
		t.Error("SetLeader must not update FetchedAt")
	}
}

func TestMetaCache_Invalidate(t *testing.T) {
	mc := NewMetaCache(60 * time.Second)

	mc.Put("topic1", &TopicMeta{
		PartitionCount: 2,
		Leaders:        map[int32]string{0: "a:1", 1: "b:2"},
		FetchedAt:      time.Now(),
	})

	mc.Invalidate("topic1")

	if got := mc.Get("topic1"); got != nil {
		t.Error("expected nil after Invalidate, got non-nil")
	}
}
