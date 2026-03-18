package storage

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	appendBuckets   = []float64{.00005, .0001, .00025, .0005, .001, .005, .01}
	recoveryBuckets = []float64{.01, .05, .1, .5, 1, 5, 10}
)

// StorageMetrics holds all Prometheus metrics for the storage package.
type StorageMetrics struct {
	BytesAppendedTotal   *prometheus.CounterVec
	BatchesAppendedTotal *prometheus.CounterVec
	SegmentsRolledTotal  *prometheus.CounterVec
	SegmentsDeletedTotal *prometheus.CounterVec
	ActiveSegmentBytes   *prometheus.GaugeVec
	SegmentCount         *prometheus.GaugeVec
	EarliestOffset       *prometheus.GaugeVec
	LatestOffset         *prometheus.GaugeVec
	AppendDuration       *prometheus.HistogramVec
	ReadDuration         *prometheus.HistogramVec
	RecoveryDuration     *prometheus.HistogramVec
	CRCErrorsTotal       *prometheus.CounterVec
}

// NewStorageMetrics creates and registers all 12 storage metrics with reg.
func NewStorageMetrics(reg prometheus.Registerer) *StorageMetrics {
	m := &StorageMetrics{
		BytesAppendedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bunnymq_storage_bytes_appended_total",
			Help: "Total bytes written to the log",
		}, []string{"topic", "partition_id"}),
		BatchesAppendedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bunnymq_storage_batches_appended_total",
			Help: "Total number of batches successfully appended",
		}, []string{"topic", "partition_id"}),
		SegmentsRolledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bunnymq_storage_segments_rolled_total",
			Help: "Number of times the active segment has been rolled",
		}, []string{"topic", "partition_id"}),
		SegmentsDeletedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bunnymq_storage_segments_deleted_total",
			Help: "Segments deleted by retention; reason is time or bytes",
		}, []string{"topic", "partition_id", "reason"}),
		ActiveSegmentBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_storage_active_segment_bytes",
			Help: "Current size of the active .log file in bytes",
		}, []string{"topic", "partition_id"}),
		SegmentCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_storage_segment_count",
			Help: "Total number of segments (sealed + active)",
		}, []string{"topic", "partition_id"}),
		EarliestOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_storage_earliest_offset",
			Help: "Earliest available offset after retention enforcement",
		}, []string{"topic", "partition_id"}),
		LatestOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_storage_latest_offset",
			Help: "Latest written offset",
		}, []string{"topic", "partition_id"}),
		AppendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_storage_append_duration_seconds",
			Help:    "Wall time for a single Append() call",
			Buckets: appendBuckets,
		}, []string{"topic", "partition_id"}),
		ReadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_storage_read_duration_seconds",
			Help:    "Wall time for a single Read() call",
			Buckets: appendBuckets,
		}, []string{"topic", "partition_id"}),
		RecoveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_storage_recovery_duration_seconds",
			Help:    "Wall time for Storage.Open() (crash recovery)",
			Buckets: recoveryBuckets,
		}, []string{"topic", "partition_id"}),
		CRCErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bunnymq_storage_crc_errors_total",
			Help: "Batches with invalid CRC-32C found during crash recovery",
		}, []string{"topic", "partition_id"}),
	}
	reg.MustRegister(
		m.BytesAppendedTotal,
		m.BatchesAppendedTotal,
		m.SegmentsRolledTotal,
		m.SegmentsDeletedTotal,
		m.ActiveSegmentBytes,
		m.SegmentCount,
		m.EarliestOffset,
		m.LatestOffset,
		m.AppendDuration,
		m.ReadDuration,
		m.RecoveryDuration,
		m.CRCErrorsTotal,
	)
	return m
}

// NoopStorageMetrics returns a StorageMetrics with nil fields. Storage methods
// nil-check each field before recording, so this is safe to pass in tests.
func NoopStorageMetrics() *StorageMetrics {
	return &StorageMetrics{}
}

// recordAppend updates the counters and histogram for a successful Append.
func (m *StorageMetrics) recordAppend(topic, partID string, batchLen int, dur time.Duration) {
	if m == nil {
		return
	}
	if m.BatchesAppendedTotal != nil {
		m.BatchesAppendedTotal.WithLabelValues(topic, partID).Inc()
	}
	if m.BytesAppendedTotal != nil {
		m.BytesAppendedTotal.WithLabelValues(topic, partID).Add(float64(batchLen))
	}
	if m.AppendDuration != nil {
		m.AppendDuration.WithLabelValues(topic, partID).Observe(dur.Seconds())
	}
}

// recordAppendGauges updates LatestOffset and ActiveSegmentBytes after each Append.
func (m *StorageMetrics) recordAppendGauges(topic, partID string, latestOff, activeBytes int64) {
	if m == nil {
		return
	}
	if m.LatestOffset != nil {
		m.LatestOffset.WithLabelValues(topic, partID).Set(float64(latestOff))
	}
	if m.ActiveSegmentBytes != nil {
		m.ActiveSegmentBytes.WithLabelValues(topic, partID).Set(float64(activeBytes))
	}
}

// recordRoll updates the roll counter and segment count gauge.
func (m *StorageMetrics) recordRoll(topic, partID string, segCount int) {
	if m == nil {
		return
	}
	if m.SegmentsRolledTotal != nil {
		m.SegmentsRolledTotal.WithLabelValues(topic, partID).Inc()
	}
	if m.SegmentCount != nil {
		m.SegmentCount.WithLabelValues(topic, partID).Set(float64(segCount))
	}
}

// recordRead observes the Read duration.
func (m *StorageMetrics) recordRead(topic, partID string, dur time.Duration) {
	if m == nil || m.ReadDuration == nil {
		return
	}
	m.ReadDuration.WithLabelValues(topic, partID).Observe(dur.Seconds())
}

// recordRetentionDeletion increments the segments deleted counter with the given reason.
func (m *StorageMetrics) recordRetentionDeletion(topic, partID, reason string) {
	if m == nil || m.SegmentsDeletedTotal == nil {
		return
	}
	m.SegmentsDeletedTotal.WithLabelValues(topic, partID, reason).Inc()
}

// recordEarliestOffset sets the earliest offset gauge.
func (m *StorageMetrics) recordEarliestOffset(topic, partID string, offset int64) {
	if m == nil || m.EarliestOffset == nil {
		return
	}
	m.EarliestOffset.WithLabelValues(topic, partID).Set(float64(offset))
}

// recordRecovery records recovery duration, segment count, earliest/latest offsets, and CRC errors.
func (m *StorageMetrics) recordRecovery(topic, partID string, dur time.Duration, segCount int, earliestOff, latestOff int64, crcErrors int) {
	if m == nil {
		return
	}
	if m.RecoveryDuration != nil {
		m.RecoveryDuration.WithLabelValues(topic, partID).Observe(dur.Seconds())
	}
	if m.SegmentCount != nil {
		m.SegmentCount.WithLabelValues(topic, partID).Set(float64(segCount))
	}
	if m.EarliestOffset != nil {
		m.EarliestOffset.WithLabelValues(topic, partID).Set(float64(earliestOff))
	}
	if m.LatestOffset != nil {
		m.LatestOffset.WithLabelValues(topic, partID).Set(float64(latestOff))
	}
	if m.CRCErrorsTotal != nil && crcErrors > 0 {
		m.CRCErrorsTotal.WithLabelValues(topic, partID).Add(float64(crcErrors))
	}
}
