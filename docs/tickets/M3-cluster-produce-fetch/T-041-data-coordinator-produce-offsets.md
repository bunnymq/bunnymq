# T-041: DataCoordinator — struct, shard registry, produce, and offset queries

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement the `DataCoordinator` struct, the shard registry (`StartPartitionReplica`/`StopPartitionReplica`), `leaderCheck`, `Produce` (both acks modes), and the three offset query methods (`GetEarliestOffset`, `GetLatestOffset`, `GetOffsetByTimestamp`).

## Context

`DataCoordinator` is the data-plane routing layer. It checks whether this node is the current partition leader (via a Metadata FSM lookup) before any Raft call, and returns `NotLeaderError` if not. `Produce(acks=all)` calls `SyncProposePartition`; `Produce(acks=0)` calls `ProposePartition`. Offset queries call `LookupPartition` with no Raft round-trip.

References:
- [05-data-coordinator.md §2 — Public interface](../../design/05-data-coordinator.md#2-public-interface)
- [05-data-coordinator.md §3 — Shard registry](../../design/05-data-coordinator.md#3-shard-registry-and-partition-discovery)
- [05-data-coordinator.md §4 — Routing logic / leaderCheck](../../design/05-data-coordinator.md#4-routing-logic)
- [05-data-coordinator.md §5 — Produce flow](../../design/05-data-coordinator.md#5-produce-flow)
- [05-data-coordinator.md §7 — Offset queries](../../design/05-data-coordinator.md#7-offset-queries)

## Scope

- Create `internal/data/coordinator.go`:
  - `DataCoordinatorConfig` struct: `NodeID uint64`, `NodeAddressCacheTTLMs int64`.
  - `DataCoordinator` struct: `config`, `raftHost *raft.Host`, `registryMu sync.RWMutex`, `shardRegistry map[partitionKey]shardEntry`, `nodeAddrCache` (simple TTL cache: `map[uint64]nodeAddrEntry` protected by a mutex), `logger *zap.Logger`.
  - `StartPartitionReplica(topic string, partitionID int32, shardID uint64)` — under `registryMu.Lock`, add to `shardRegistry`.
  - `StopPartitionReplica(topic string, partitionID int32, shardID uint64)` — under `registryMu.Lock`, delete from `shardRegistry`.
  - `leaderCheck(ctx, topic, partitionID) (shardID uint64, err error)`:
    - `LookupMetadata(QueryGetPartition)` → `nil` returns `ErrPartitionNotFound`.
    - `pm.LeaderNodeID != dc.config.NodeID` → `&NotLeaderError{LeaderNodeID, leaderAddress}`.
    - `shardRegistry` check → `ErrUnavailable` if absent.
  - `nodeAddress(ctx, nodeID) string` — cached lookup via `LookupMetadata(QueryListNodes)` with `NodeAddressCacheTTLMs`.
  - `Produce(ctx, topic, partitionID, batch, acks) (int64, error)`:
    - `leaderCheck` → `SyncProposePartition(CmdAppendBatch)` (acks=all) or `ProposePartition(CmdAppendBatch)` (acks=0).
    - Return `int64(result.Value)` on success (acks=all) or `-1` (acks=0).
  - `GetEarliestOffset`, `GetLatestOffset`, `GetOffsetByTimestamp` — `leaderCheck` → `LookupPartition(QueryEarliestOffset / QueryLatestOffset / QueryReadByTime)`.

## Out of scope

- `Fetch` + long-poll — T-042.
- DataCoordinator interface extraction — done here; used by T-040.

## Definition of done

- [ ] `go build ./internal/data/...` passes.
- [ ] `go test ./internal/data/...` passes.
- [ ] `Produce(acks=all)` returns assigned base offset from `sm.Result.Value`.
- [ ] `Produce(acks=0)` returns `-1`; `ProposePartition` called (not Sync).
- [ ] `leaderCheck` returns `ErrUnavailable` when shard not in registry.
- [ ] `leaderCheck` returns `NotLeaderError` with leader address when `pm.LeaderNodeID != nodeID`.
- [ ] `StartPartitionReplica` / `StopPartitionReplica` update registry; safe for concurrent calls.
- [ ] `GetLatestOffset` returns value from `LookupPartition(QueryLatestOffset)`.

## Tests required

- `TestDataCoordinator_ProduceAll_Success` — registry has shard; metadata says this node is leader; `SyncProposePartition` called; returns offset from result.
- `TestDataCoordinator_ProduceZero_AsyncFire` — acks=0; `ProposePartition` called (not Sync); returns -1.
- `TestDataCoordinator_LeaderCheck_NotLeader` — `pm.LeaderNodeID = 2`, this node = 1; returns `NotLeaderError`.
- `TestDataCoordinator_LeaderCheck_ShardNotRegistered` — registry empty; returns `ErrUnavailable`.
- `TestDataCoordinator_RegistryConcurrency` — N goroutines call `StartPartitionReplica`/`StopPartitionReplica` concurrently; no data race (run with `-race`).
- `TestDataCoordinator_GetLatestOffset` — LookupPartition stub returns 42; `GetLatestOffset` returns 42.
- `TestDataCoordinator_GetOffsetByTimestamp_NotFound` — LookupPartition returns `ErrTimestampNotFound`; method returns `ErrOffsetNotFound`.

## Dependencies

T-024 (raft.Host.SyncProposePartition, ProposePartition, LookupPartition, LookupMetadata).
T-025 (PartitionQuery types, PartitionCommand types).
T-028 (MetadataFSM Lookup: QueryGetPartition, QueryListNodes).
T-031 (PartitionFSM Lookup types and results).

## Notes

`nodeAddrCache` is a simple in-process map with per-entry expiry (compare `entry.fetchedAt + TTL > now`). A proper TTL cache library is not needed; a mutex-protected map with one cached timestamp per entry suffices. `DataCoordinatorIface` (extracted interface with `Produce`, `Fetch`, `GetEarliestOffset`, `GetLatestOffset`, `GetOffsetByTimestamp`, `StartPartitionReplica`, `StopPartitionReplica`) should be defined in this package to enable test doubles in T-037, T-038, and T-040.
