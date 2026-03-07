package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// EnforceRetention deletes sealed segments that violate the retention policy.
// A segment is eligible if it satisfies the time constraint OR the bytes
// constraint. The active segment is never deleted.
func (s *storageImpl) EnforceRetention(retentionMs int64, retentionBytes int64) (int, error) {
	s.segMu.RLock()
	segs := make([]*SegmentStorage, len(s.segments))
	copy(segs, s.segments)
	s.segMu.RUnlock()

	if len(segs) <= 1 {
		return 0, nil
	}

	sealedSegs := segs[:len(segs)-1]
	toDelete := make([]bool, len(sealedSegs))

	// Bytes retention: mark oldest sealed segments until remaining total <=
	// retentionBytes. retentionBytes < 0 means unlimited.
	if retentionBytes >= 0 {
		var total int64
		for _, seg := range segs {
			total += seg.LogSize()
		}
		for i := 0; i < len(sealedSegs) && total > retentionBytes; i++ {
			toDelete[i] = true
			total -= sealedSegs[i].LogSize()
		}
	}

	// Time retention: segment S[i] is expired when its successor's first
	// batch timestamp is older than now - retentionMs.
	if retentionMs > 0 {
		nowMs := time.Now().UnixMilli()
		for i := range sealedSegs {
			nextSeg := segs[i+1]
			ts, err := s.firstBatchTimestamp(nextSeg)
			if err != nil {
				continue
			}
			if ts < nowMs-retentionMs {
				toDelete[i] = true
			}
		}
	}

	deleted := 0
	for i, del := range toDelete {
		if !del {
			continue
		}
		seg := sealedSegs[i]

		s.segMu.Lock()
		for j, s2 := range s.segments {
			if s2 == seg {
				s.segments = append(s.segments[:j], s.segments[j+1:]...)
				break
			}
		}
		s.segMu.Unlock()

		_ = seg.Close()
		base := segmentBasePath(s.dir, seg.BaseOffset())
		_ = os.Remove(base + ".log")
		_ = os.Remove(base + ".index")
		_ = os.Remove(base + ".timeindex")
		deleted++
	}

	return deleted, nil
}

// startRetentionLoop ticks at interval and calls EnforceRetention until ctx is
// cancelled.
func (s *storageImpl) startRetentionLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			retMs := s.retentionMs.Load()
			retBytes := s.retentionBytes.Load()
			_, _ = s.EnforceRetention(retMs, retBytes)
		}
	}
}

// firstBatchTimestamp reads base_timestamp (bytes [22,30]) from the first
// batch header in seg's log file.
func (s *storageImpl) firstBatchTimestamp(seg *SegmentStorage) (int64, error) {
	header, err := seg.log.ReadAt(0, 38)
	if err != nil || len(header) < 30 {
		return 0, fmt.Errorf("read batch header: %w", err)
	}
	return int64(binary.BigEndian.Uint64(header[22:30])), nil
}
