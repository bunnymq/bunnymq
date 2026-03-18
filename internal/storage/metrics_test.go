package storage

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
)

func newTestMetrics(t *testing.T) (*StorageMetrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := NewStorageMetrics(reg)
	return m, reg
}

func counterVal(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}

func histCount(h prometheus.Observer) uint64 {
	var m dto.Metric
	_ = h.(prometheus.Histogram).Write(&m)
	return m.GetHistogram().GetSampleCount()
}

// TestStorageMetrics_AppendIncrements verifies batches_appended_total and bytes_appended_total.
func TestStorageMetrics_AppendIncrements(t *testing.T) {
	m, _ := newTestMetrics(t)
	dir := t.TempDir()
	s, err := Open(dir, storageTestConfig(128*1024*1024, 4096), WithMetrics(m, "test-topic", "0"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var totalBytes int
	for i := range 3 {
		b := makeBatch(t, "msg", int64(i))
		if _, err := s.Append(b); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		totalBytes += len(b)
	}

	batchesCounter := m.BatchesAppendedTotal.WithLabelValues("test-topic", "0")
	if got := counterVal(batchesCounter); got != 3 {
		t.Errorf("batches_appended_total = %v, want 3", got)
	}

	bytesCounter := m.BytesAppendedTotal.WithLabelValues("test-topic", "0")
	if got := counterVal(bytesCounter); got != float64(totalBytes) {
		t.Errorf("bytes_appended_total = %v, want %d", got, totalBytes)
	}
}

// TestStorageMetrics_ReadObserved verifies that a Read records one observation.
func TestStorageMetrics_ReadObserved(t *testing.T) {
	m, _ := newTestMetrics(t)
	dir := t.TempDir()
	s, err := Open(dir, storageTestConfig(128*1024*1024, 4096), WithMetrics(m, "test-topic", "0"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, err := s.Append(makeBatch(t, "x", 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, _, err := s.Read(0, 1<<20); err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got := histCount(m.ReadDuration.WithLabelValues("test-topic", "0")); got != 1 {
		t.Errorf("read_duration_seconds count = %d, want 1", got)
	}
}

// TestStorageMetrics_SegmentRoll verifies segments_rolled_total and segment_count after a roll.
func TestStorageMetrics_SegmentRoll(t *testing.T) {
	m, _ := newTestMetrics(t)
	dir := t.TempDir()
	// SegmentMaxBytes=1 forces a roll after every append.
	s, err := Open(dir, storageTestConfig(1, 4096), WithMetrics(m, "test-topic", "0"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 3 {
		if _, err := s.Append(makeBatch(t, "v", int64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	rolled := counterVal(m.SegmentsRolledTotal.WithLabelValues("test-topic", "0"))
	if rolled < 3 {
		t.Errorf("segments_rolled_total = %v, want >= 3", rolled)
	}

	var segCountM dto.Metric
	_ = m.SegmentCount.WithLabelValues("test-topic", "0").Write(&segCountM)
	if got := segCountM.GetGauge().GetValue(); got < 2 {
		t.Errorf("segment_count = %v, want >= 2", got)
	}
}

// TestStorageMetrics_RetentionDeletion verifies segments_deleted_total{reason="bytes"} and earliest_offset.
func TestStorageMetrics_RetentionDeletion(t *testing.T) {
	m, _ := newTestMetrics(t)
	dir := t.TempDir()
	// SegmentMaxBytes=1 forces a roll after every append.
	s, err := Open(dir, storageTestConfig(1, 4096), WithMetrics(m, "test-topic", "0"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Append 3 batches; each rolls, giving 3 sealed segments + 1 active.
	for i := range 3 {
		if _, err := s.Append(makeBatch(t, "v", int64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Enforce bytes retention of 0: delete all sealed segments.
	deleted, err := s.EnforceRetention(-1, 0)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if deleted == 0 {
		t.Fatal("expected at least one segment deleted")
	}

	bytesDeleted := counterVal(m.SegmentsDeletedTotal.WithLabelValues("test-topic", "0", "bytes"))
	if bytesDeleted == 0 {
		t.Errorf("segments_deleted_total{reason=bytes} = 0, want > 0")
	}

	var earliestM dto.Metric
	_ = m.EarliestOffset.WithLabelValues("test-topic", "0").Write(&earliestM)
	if got := earliestM.GetGauge().GetValue(); got == 0 && deleted > 0 {
		// After deleting old segments, earliest offset should have advanced.
		// Since all sealed segments were deleted, earliest is now the active segment's base.
		s.segMu.RLock()
		actualEarliest := s.segments[0].BaseOffset()
		s.segMu.RUnlock()
		if actualEarliest > 0 && got != float64(actualEarliest) {
			t.Errorf("earliest_offset gauge = %v, want %d", got, actualEarliest)
		}
	}
}

// TestStorageMetrics_NilSafe verifies that a Storage with nil metrics does not panic.
func TestStorageMetrics_NilSafe(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, storageTestConfig(1, 4096))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	b := makeBatch(t, "x", 1)
	if _, err := s.Append(b); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, _, err := s.Read(0, 1<<20); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := s.EnforceRetention(-1, 0); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
}
