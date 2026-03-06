# T-061: Metrics — Raft and coordinator metrics implementation

**Milestone:** M5 — Integration and polish
**Effort:** S
**Status:** TODO

## Goal

Implement `RaftMetrics` with all 8 raft metrics from `09-metrics-logging.md §2.4.2` and wire them into the NodeHost wrapper, Metadata FSM, and Partition FSM so that leader changes, propose latency, snapshot timing, and FSM update duration are all tracked.

## Context

Raft health metrics are critical for diagnosing replication lag, leader instability, and snapshot storms. The `bunnymq_raft_is_leader` gauge is especially important for alerting — a prolonged absence of any leader (all shards at 0) indicates a quorum failure. Like storage metrics, all are registered against an explicit non-global registry.

References:
- [09-metrics-logging.md §2.2 — Registry ownership](../../design/09-metrics-logging.md#22-registry-ownership)
- [09-metrics-logging.md §2.4.2 — Raft metrics catalog](../../design/09-metrics-logging.md#242-raft-metrics)
- [03-raft-fsm.md — Partition FSM Update and Metadata FSM Update](../../design/03-raft-fsm.md)

## Scope

- Create `internal/cluster/metrics.go`:
  - `RaftMetrics` struct with typed fields for all 8 metrics from §2.4.2:
    - `CommittedIndex *prometheus.GaugeVec` — label: `shard_id`
    - `AppliedIndex *prometheus.GaugeVec` — label: `shard_id`
    - `IsLeader *prometheus.GaugeVec` — label: `shard_id`
    - `Term *prometheus.GaugeVec` — label: `shard_id`
    - `ProposeDuration *prometheus.HistogramVec` — labels: `shard_id`, `acks`; buckets: 500µs, 1ms, 2ms, 5ms, 10ms, 50ms, 100ms, 500ms
    - `SnapshotSaveDuration *prometheus.HistogramVec` — label: `shard_id`
    - `SnapshotRecoverDuration *prometheus.HistogramVec` — label: `shard_id`
    - `LeaderChangesTotal *prometheus.CounterVec` — label: `shard_id`
    - `FSMUpdateDuration *prometheus.HistogramVec` — label: `shard_id`; same buckets as propose
  - `NewRaftMetrics(reg prometheus.Registerer) *RaftMetrics`.
  - `NoopRaftMetrics() *RaftMetrics`.
- Modify the NodeHost wrapper / ClusterCoordinator (T-024/T-039):
  - In `SyncProposeMetadata` / `SyncProposePartition` wrappers: observe `ProposeDuration{acks="all"}`.
  - In `ProposePartition` (acks=0): observe `ProposeDuration{acks="zero"}`.
  - In `leaderSweepLoop` when leader changes detected: increment `LeaderChangesTotal`; update `IsLeader` gauge.
- Modify MetadataFSM (T-027):
  - At top of `Update()`: start timer; observe `FSMUpdateDuration` on return.
  - In `SaveSnapshot`: observe `SnapshotSaveDuration`.
  - In `RecoverFromSnapshot`: observe `SnapshotRecoverDuration`.
  - Update `CommittedIndex` and `AppliedIndex` gauges from the `Entry` passed to `Update`.
- Modify PartitionFSM (T-031):
  - Same `FSMUpdateDuration` observation in `Update()`.
  - Same snapshot timing in `PrepareSnapshot`/`RecoverFromSnapshot`.

## Out of scope

- API/gRPC metrics — T-062.
- Storage metrics — T-060.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] `ProposeDuration{acks="all"}` has an observation after `SyncProposeMetadata`.
- [ ] `LeaderChangesTotal` incremented when leader sweep detects a change.
- [ ] `IsLeader` gauge is 1 for the leader shard, 0 for followers.
- [ ] `FSMUpdateDuration` observed in both MetadataFSM and PartitionFSM `Update()`.
- [ ] `SnapshotSaveDuration` observed after MetadataFSM snapshot.
- [ ] Nil-safe: `NoopRaftMetrics()` allows components to run without a registry.

## Tests required

- `TestRaftMetrics_ProposeDuration_All` — call `SyncProposeMetadata` stub; `propose_duration_seconds{acks="all"}` count == 1.
- `TestRaftMetrics_ProposeDuration_Zero` — call `ProposePartition` stub; `propose_duration_seconds{acks="zero"}` count == 1.
- `TestRaftMetrics_LeaderChange` — sweep detects leader change; `leader_changes_total` incremented; `is_leader` gauge updated.
- `TestRaftMetrics_FSMUpdate_Timed` — `MetadataFSM.Update()` called; `fsm_update_duration_seconds` count == 1.
- `TestRaftMetrics_Snapshot_Timed` — `SaveSnapshot` called; `snapshot_save_duration_seconds` count == 1.
- `TestRaftMetrics_NilSafe` — nil metrics; all FSM operations complete without panic.

## Dependencies

- T-024 (NodeHost wrapper and propose wrappers to instrument).
- T-027 (MetadataFSM Update/Snapshot to instrument).
- T-031 (PartitionFSM Update/Snapshot to instrument).
- T-039 (ClusterCoordinator leaderSweepLoop to instrument).

## Notes

The `IsLeader` gauge is set in the `leaderSweepLoop` (T-040), not in the FSM — because the FSM does not know which node is the leader at apply time. The sweep calls `GetLeaderID(shardID)` and sets `IsLeader.WithLabelValues(shardID)` to 1 or 0. `CommittedIndex` and `AppliedIndex` can be updated from `Entry.Index` in the FSM `Update()` call — `CommittedIndex` == `AppliedIndex` from the FSM's perspective since dragonboat only delivers committed entries to the FSM.
