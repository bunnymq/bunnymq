# T-040: ClusterCoordinator — partition shard reconciliation and leader sweep

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement `reconcileOnce`, `reconcileLoop`, `startShard`, `stopShard`, `leaderSweepLoop`, and `sweepLeaders` inside `ClusterCoordinator`, and wire `StartPartitionReplica`/`StopPartitionReplica` calls to `DataCoordinator` from within `startShard`/`stopShard`.

## Context

The reconciliation goroutine compares which partition shards this node should run (from Metadata FSM) against those actually running (`runningShards`), starting and stopping shards as needed. The leader sweep goroutine detects Raft leader changes for locally running partition shards and commits `AssignPartitionLeader` commands to keep the Metadata FSM up to date. Both goroutines run in the background after `Bootstrap()`.

References:
- [04-cluster-coordinator.md §6 — Leader epoch tracking](../../design/04-cluster-coordinator.md#6-leader-epoch-tracking)
- [04-cluster-coordinator.md §7 — Partition shard lifecycle](../../design/04-cluster-coordinator.md#7-partition-shard-lifecycle--reconciliation-goroutine)
- [05-data-coordinator.md §3 — Shard registry](../../design/05-data-coordinator.md#3-shard-registry-and-partition-discovery)

## Scope

- Add to `internal/cluster/coordinator.go`:
  - `reconcileLoop(ctx context.Context)` — ticks every `ReconcileIntervalMs`; calls `reconcileOnce`.
  - `reconcileOnce(ctx context.Context)`:
    - `LookupMetadata(QueryListAllPartitions)` — get all `PartitionMeta`.
    - Build `expected map[uint64]shardInfo` for shards where this node is a replica.
    - Under `shardMu.Lock`: for each shard in `expected` not in `runningShards`, `go cc.startShard(shardID, info)`; for each shard in `runningShards` not in `expected`, `go cc.stopShard(shardID, info)`.
  - `startShard(shardID uint64, info shardInfo)`:
    - Determine `join` bool: `join = (cc.config.NodeID != slices.Min(replicaNodeIDs))`.
    - `cc.raftHost.StartPartitionShard(shardID, peers, join)`.
    - `cc.dataCoord.StartPartitionReplica(info.Topic, info.PartitionID, shardID)`.
    - Add to `runningShards` under `shardMu.Lock`.
  - `stopShard(shardID uint64, info shardInfo)`:
    - `cc.dataCoord.StopPartitionReplica(info.Topic, info.PartitionID, shardID)`.
    - `cc.raftHost.StopPartitionShard(shardID)`.
    - Remove from `runningShards`; `os.RemoveAll(partitionDir(...))`.
  - `leaderSweepLoop(ctx context.Context)` — ticks every `LeaderCheckIntervalMs`; calls `sweepLeaders`.
  - `sweepLeaders(ctx context.Context)`:
    - Snapshot `runningShards` under `shardMu.RLock`.
    - For each shard: `cc.raftHost.GetLeaderID(shardID)` → compare against `lastKnownLeader`; if changed, `SyncProposeMetadata(AssignPartitionLeaderCmd)`.
    - Update `lastKnownLeader` under `leaderMu.Lock`.
  - Add `dataCoord DataCoordinatorIface` field to `ClusterCoordinator` struct; inject via constructor.

## Out of scope

- ClusterCoordinator struct + topic lifecycle — T-039.
- DataCoordinator implementation — T-041, T-042.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] `reconcileOnce` starts shards not in `runningShards`; stops shards no longer expected.
- [ ] `startShard` calls `dataCoord.StartPartitionReplica` after raftHost.
- [ ] `stopShard` calls `dataCoord.StopPartitionReplica` before raftHost, then `os.RemoveAll`.
- [ ] `sweepLeaders` fires `AssignPartitionLeader` when leader term changes; skips when unchanged.
- [ ] `sweepLeaders` logs `warn` and continues when `SyncProposeMetadata` fails.

## Tests required

- `TestReconcileOnce_StartsExpectedShards` — mock metadata returns 2 shards for this node; `runningShards` empty; reconcile calls `startShard` for both.
- `TestReconcileOnce_StopsDeletedShards` — `runningShards` has shard not in metadata; reconcile calls `stopShard`.
- `TestReconcileOnce_IdempotentIfMatch` — `runningShards` matches metadata; no start/stop calls.
- `TestStartShard_RegistersWithDataCoord` — mock dataCoord; `startShard` calls `StartPartitionReplica`.
- `TestStopShard_UnregistersBeforeStop` — mock dataCoord; `StopPartitionReplica` called before `StopPartitionShard`.
- `TestSweepLeaders_FiresOnChange` — `GetLeaderID` returns new leader; `SyncProposeMetadata` called once.
- `TestSweepLeaders_SkipsIfUnchanged` — leader unchanged; no propose call.
- `TestSweepLeaders_ContinuesOnProposeError` — `SyncProposeMetadata` returns error; sweep logs and moves on.

## Dependencies

T-039 (ClusterCoordinator struct).
T-024 (raft.Host.StartPartitionShard, StopPartitionShard, GetLeaderID).
T-041 (DataCoordinator interface, for injection and testing).

## Notes

`GetLeaderID` has a VERIFY marker in `04-cluster-coordinator.md §6`: verify that dragonboat v4 exposes this method and its exact signature. Flag for implementer to check dragonboat v4 API docs. The `join=false` vs `join=true` heuristic (lowest NodeID in ReplicaNodeIDs starts without join) also carries a VERIFY marker (Open Question 3 in §13) — the implementer must verify this against dragonboat v4 `StartCluster` semantics before wiring it up. `partitionDir(dataDir, topic, partitionID)` produces the path `<dataDir>/partitions/<topic>/<partitionID>/`; define this helper in the package.
