# T-039: ClusterCoordinator — bootstrap and topic lifecycle

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement the `ClusterCoordinator` struct, its `Bootstrap()` procedure (steps 1-4 from §8), and all six public topic/cluster lifecycle methods: `CreateTopic`, `DeleteTopic`, `ListTopics`, `DescribeTopic`, `AlterTopicPartitionCount`, `AlterTopicRetention`, `DescribeCluster`.

## Context

`ClusterCoordinator` is the sole writer to the Metadata FSM for all admin-plane operations. Topic lifecycle methods validate input, call `SyncProposeMetadata`/`LookupMetadata` via the `raft.Host`, and return typed results. `Bootstrap()` starts the metadata shard, waits for leader election, registers this node, and runs the initial reconciliation — all before the gRPC servers start accepting traffic.

References:
- [04-cluster-coordinator.md §2 — Public interface](../../design/04-cluster-coordinator.md#2-public-interface)
- [04-cluster-coordinator.md §3 — Method flows](../../design/04-cluster-coordinator.md#3-method-flows)
- [04-cluster-coordinator.md §4 — Partition assignment algorithm](../../design/04-cluster-coordinator.md#4-partition-assignment-algorithm)
- [04-cluster-coordinator.md §8 — Bootstrap](../../design/04-cluster-coordinator.md#8-bootstrap-behavior)

## Scope

- Create `internal/cluster/coordinator.go`:
  - `CoordinatorConfig` struct: `NodeID uint64`, `RaftAddress string`, `DataDir string`, `Peers map[uint64]string`, `BootstrapTimeoutMs int64`, `ReconcileIntervalMs int64`, `LeaderCheckIntervalMs int64`, `EagerReconcileOnCreate bool`.
  - `ClusterCoordinator` struct: `config CoordinatorConfig`, `raftHost *raft.Host`, `shardMu sync.RWMutex`, `runningShards map[uint64]shardInfo`, `leaderMu sync.Mutex`, `lastKnownLeader map[uint64]leaderRecord`, `logger *zap.Logger`.
  - `Bootstrap(ctx context.Context) error` — 6-step sequence from §8: start metadata shard (`StartCluster` with `join=false` for fresh cluster), poll `LookupMetadata(QueryListNodes)` until success (max `BootstrapTimeoutMs`), `SyncProposeMetadata(RegisterNodeCmd)`, call `reconcileOnce`, start background goroutines, close ready channel.
  - `CreateTopic` — validate name regex + partition count + RF ≤ cluster size; compute replica assignments using `assignReplicas` (FNV-1a §4); `SyncProposeMetadata(CreateTopicCmd)`; map FSM result; if `EagerReconcileOnCreate` call `reconcileOnce`.
  - `DeleteTopic` — `LookupMetadata(QueryGetTopic)` → `SyncProposeMetadata(DeleteTopicCmd)`.
  - `ListTopics` — `LookupMetadata(QueryListTopics)`.
  - `DescribeTopic` — `LookupMetadata(QueryGetTopic)` + `LookupMetadata(QueryGetPartitions)`.
  - `AlterTopicPartitionCount` — validate; compute new assignments; `SyncProposeMetadata(AlterTopicPartCountCmd)`.
  - `AlterTopicRetention` — `SyncProposeMetadata(AlterTopicRetentionCmd)`; fire `ProposePartition(shardID, RetentionConfigCmd)` for each partition shard (acks=0, goroutines).
  - `DescribeCluster` — `LookupMetadata(QueryListNodes)`.
  - Implement `assignReplicas(nodes []NodeInfo, topicName string, partitionID int32, rf int32) []uint64` using FNV-1a (exact algorithm from §4).

## Out of scope

- `reconcileOnce`, `reconcileLoop`, `leaderSweepLoop` — T-040.
- `startShard`, `stopShard` — T-040.
- gRPC handlers — T-036.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] `assignReplicas` with 3 nodes, 3 partitions, RF=1: each partition on a different node.
- [ ] `CreateTopic` with `replicationFactor > cluster size` returns `ErrInvalidArgument`.
- [ ] `CreateTopic` twice with same name: second call returns `ErrTopicAlreadyExists`.
- [ ] `Bootstrap` polls LookupMetadata until success; times out after `BootstrapTimeoutMs` with error.
- [ ] `AlterTopicRetention` fires goroutine-based `ProposePartition` for each shard.

## Tests required

- `TestAssignReplicas_Distribution` — 5 nodes, 5 partitions, RF=1: each node gets exactly 1 partition.
- `TestAssignReplicas_RF3` — 3 nodes, 1 partition, RF=3: all 3 nodes in the replica list.
- `TestAssignReplicas_Deterministic` — same inputs → same output (call twice).
- `TestCreateTopic_ValidatesName` — name with invalid chars → `ErrInvalidArgument`.
- `TestCreateTopic_ValidatesRF` — RF > node count → `ErrInvalidArgument`.
- `TestCreateTopic_AlreadyExists` — FSM stub returns AlreadyExists result → `ErrTopicAlreadyExists`.
- `TestDeleteTopic_NotFound` — LookupMetadata returns nil → `ErrTopicNotFound` without proposal.
- `TestBootstrap_Timeout` — LookupMetadata always returns error; `Bootstrap` returns error after timeout.
- `TestAlterTopicRetention_PropagatesShards` — 3 shards; `AlterTopicRetention` fires 3 async ProposePartition calls.

## Dependencies

T-024 (raft.Host with SyncProposeMetadata, LookupMetadata, ProposePartition).
T-025 (MetadataCommand types, sm.Result error encoding).
T-026 through T-028 (MetadataFSM commands and queries used here).

## Notes

`Bootstrap` must be called exactly once before the gRPC servers start. The `join=false` vs `join=true` semantics for `StartCluster` (VERIFY marker in §8) affects whether a fresh cluster vs. a restarting node needs different handling — flag this as "verify against dragonboat v4 docs during implementation." For the `AlterTopicRetention` goroutines, use `go cc.raftHost.ProposePartition(...)` with a fresh `context.Background()` (not the caller's ctx, which may be cancelled). Log `warn` on goroutine-scoped errors; do not propagate them.
