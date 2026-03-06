# T-060: Metrics — Storage metrics implementation

**Milestone:** M5 — Integration and polish
**Effort:** S
**Status:** TODO

## Goal

Implement the `StorageMetrics` struct with all 12 storage metrics from `09-metrics-logging.md §2.4.1` and wire them into the `Storage` type so that every `Append`, `Read`, `Open`, and retention enforcement call updates the appropriate counters, gauges, and histograms.

## Context

Storage is the highest-frequency component in the system — every produce call ultimately ends in a storage append. Instrumenting it properly is essential for diagnosing throughput and latency regressions. The Prometheus client uses a non-global registry (per §2.2), so all metrics are registered against a `prometheus.Registerer` passed at construction time; this also allows tests to use isolated registries.

References:
- [09-metrics-logging.md §2.2 — Registry ownership](../../design/09-metrics-logging.md#22-registry-ownership)
- [09-metrics-logging.md §2.4.1 — Storage metrics catalog](../../design/09-metrics-logging.md#241-storage-metrics)

## Scope

- Create `internal/storage/metrics.go`:
  - `StorageMetrics` struct with typed fields for all 12 metrics from §2.4.1:
    - `BytesAppendedTotal *prometheus.CounterVec` — labels: `topic`, `partition_id`
    - `BatchesAppendedTotal *prometheus.CounterVec` — labels: `topic`, `partition_id`
    - `SegmentsRolledTotal *prometheus.CounterVec` — labels: `topic`, `partition_id`
    - `SegmentsDeletedTotal *prometheus.CounterVec` — labels: `topic`, `partition_id`, `reason`
    - `ActiveSegmentBytes *prometheus.GaugeVec` — labels: `topic`, `partition_id`
    - `SegmentCount *prometheus.GaugeVec` — labels: `topic`, `partition_id`
    - `EarliestOffset *prometheus.GaugeVec` — labels: `topic`, `partition_id`
    - `LatestOffset *prometheus.GaugeVec` — labels: `topic`, `partition_id`
    - `AppendDuration *prometheus.HistogramVec` — labels: `topic`, `partition_id`; buckets: 50µs, 100µs, 250µs, 500µs, 1ms, 5ms, 10ms
    - `ReadDuration *prometheus.HistogramVec` — labels: `topic`, `partition_id`; same buckets
    - `RecoveryDuration *prometheus.HistogramVec` — labels: `topic`, `partition_id`; buckets: 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s
    - `CRCErrorsTotal *prometheus.CounterVec` — labels: `topic`, `partition_id`
  - `NewStorageMetrics(reg prometheus.Registerer) *StorageMetrics` — creates and registers all metrics.
  - `NoopStorageMetrics() *StorageMetrics` — returns a struct with nil fields; `Storage` methods must nil-check before recording (allows tests without a registry).
- Modify `internal/storage/storage.go` (T-020's file):
  - Add `metrics *StorageMetrics` field to `Storage` struct.
  - `Append`: observe `AppendDuration`; increment `BatchesAppendedTotal` + `BytesAppendedTotal`; update `LatestOffset` + `ActiveSegmentBytes`.
  - `Read`: observe `ReadDuration`.
  - `Open` (recovery): observe `RecoveryDuration`; set `SegmentCount`, `EarliestOffset`, `LatestOffset`.
  - Segment roll: increment `SegmentsRolledTotal`; update `SegmentCount` + `ActiveSegmentBytes`.
  - Retention deletion: increment `SegmentsDeletedTotal{reason}`.
  - CRC error during recovery: increment `CRCErrorsTotal`.

## Out of scope

- Raft metrics — T-061.
- API/gRPC metrics — T-062.
- Metrics HTTP server — T-062.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] `NewStorageMetrics` registers all 12 metrics without panicking.
- [ ] After one `Append`, `bunnymq_storage_batches_appended_total` increments by 1 for the correct labels.
- [ ] After one `Read`, `bunnymq_storage_read_duration_seconds` has an observation.
- [ ] After segment roll, `bunnymq_storage_segments_rolled_total` increments.
- [ ] After retention deletion with `reason=bytes`, `bunnymq_storage_segments_deleted_total{reason="bytes"}` increments.
- [ ] `Storage` constructed without metrics (nil `*StorageMetrics`) does not panic.

## Tests required

- `TestStorageMetrics_AppendIncrements` — append 3 batches; verify `batches_appended_total` == 3 and `bytes_appended_total` == sum of batch sizes.
- `TestStorageMetrics_ReadObserved` — one read; verify `read_duration_seconds` has count == 1.
- `TestStorageMetrics_SegmentRoll` — trigger segment roll via size threshold; `segments_rolled_total` incremented; `segment_count` gauge updated.
- `TestStorageMetrics_RetentionDeletion` — enforce retention by bytes; `segments_deleted_total{reason="bytes"}` incremented; `earliest_offset` gauge updated.
- `TestStorageMetrics_NilSafe` — construct `Storage` with nil metrics; all operations succeed without panic.

## Dependencies

- T-020 (Storage implementation — struct and methods to instrument).
- T-021 (Retention enforcement — segment deletion to instrument).

## Notes

Use `prometheus.MustRegister` only in `NewStorageMetrics`; never call `MustRegister` in the hot path. The `NoopStorageMetrics` pattern avoids the need for a mock registry in unit tests that do not care about metrics — just pass `nil` or call `NoopStorageMetrics()` in the test setup. Nil-check pattern: `if m != nil && m.BatchesAppendedTotal != nil { m.BatchesAppendedTotal.WithLabelValues(topic, partID).Inc() }` — write a thin `m.recordAppend(topic, partID, bytes)` helper to centralise this.
