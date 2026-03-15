package client

import (
	"sync"
	"time"
)

// TopicMeta holds cached metadata for a single topic.
type TopicMeta struct {
	PartitionCount int32
	Leaders        map[int32]string // partitionID → Data API address
	FetchedAt      time.Time
}

// MetaCache caches per-topic metadata with a configurable TTL.
type MetaCache struct {
	mu    sync.RWMutex
	cache map[string]*TopicMeta
	ttl   time.Duration
}

// NewMetaCache creates a MetaCache with the given TTL.
func NewMetaCache(ttl time.Duration) *MetaCache {
	return &MetaCache{
		cache: make(map[string]*TopicMeta),
		ttl:   ttl,
	}
}

// Get returns cached metadata if present and not expired, or nil otherwise.
func (mc *MetaCache) Get(topic string) *TopicMeta {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	meta, ok := mc.cache[topic]
	if !ok || time.Since(meta.FetchedAt) > mc.ttl {
		return nil
	}
	return meta
}

// Put stores or replaces metadata for a topic.
func (mc *MetaCache) Put(topic string, meta *TopicMeta) {
	mc.mu.Lock()
	mc.cache[topic] = meta
	mc.mu.Unlock()
}

// SetLeader updates the leader address for a single partition without touching
// FetchedAt or other partitions.
func (mc *MetaCache) SetLeader(topic string, partitionID int32, addr string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	meta, ok := mc.cache[topic]
	if !ok {
		return
	}
	if meta.Leaders == nil {
		meta.Leaders = make(map[int32]string)
	}
	meta.Leaders[partitionID] = addr
}

// Invalidate removes the cached entry for a topic, forcing a refresh on next Get.
func (mc *MetaCache) Invalidate(topic string) {
	mc.mu.Lock()
	delete(mc.cache, topic)
	mc.mu.Unlock()
}
