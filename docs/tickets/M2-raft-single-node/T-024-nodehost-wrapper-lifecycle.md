# T-024: NodeHost wrapper and lifecycle

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** M
**Status:** TODO

## Goal

Implement the `Host` struct in `internal/raft` that wraps dragonboat's `NodeHost` with typed helpers for metadata and partition shard operations, hiding all dragonboat types from the rest of the codebase.

## Context

Every module that interacts with consensus — ClusterCoordinator, DataCoordinator, both API servers — calls the helpers in `internal/raft`. No other module imports dragonboat directly. The wrapper serializes `MetadataCommand` to JSON and `PartitionCommand` to a prefix-byte format, and deserializes responses to typed values. The lifecycle (start, stop) is driven by the broker's main function.

References:
- [03-raft-fsm.md §1 — NodeHost wrapper](../../design/03-raft-fsm.md#1-nodehost-wrapper-internalraft)
- [03-raft-fsm.md §1.3 — Public API of the wrapper](../../design/03-raft-fsm.md#13-public-api-of-the-wrapper)

## Scope

- Define `Host` struct in `internal/raft/host.go`:
  - Fields: `nh *dragonboat.NodeHost`, `config *Config`.
- Define `Config` struct: `DataDir string`, `RaftAddress string`, `NodeID uint64`, `RaftRTTMs uint64`, `Peers map[uint64]string`.
- Implement `NewHost(config *Config) (*Host, error)`:
  - Constructs `dragonboat.NodeHostConfig` with `DeploymentID=1`, `WALDir`, `NodeHostDir`, `RTTMillisecond`, `RaftAddress`, `EnableMetrics=true`.
  - Calls `dragonboat.NewNodeHost(nhc)`.
- Implement `(*Host).StartMetadataShard(initialMembers map[uint64]string, join bool, factory sm.CreateStateMachineFunc) error`:
  - Constructs per-shard `config.Config` with `ClusterID=0`, `ElectionRTT=10`, `HeartbeatRTT=1`, `CheckQuorum=true`, `MaxInMemLogSize=32<<20`, `SnapshotEntries=1<<62`, `CompactionOverhead=1<<62`.
  - Calls `nh.StartCluster(initialMembers, join, factory, rc)`.
- Implement `(*Host).StartPartitionShard(shardID uint64, initialMembers map[uint64]string, join bool, factory sm.CreateOnDiskStateMachineFunc) error`:
  - Same `config.Config` as metadata shard but `ClusterID=shardID`.
- Implement `(*Host).StopPartitionShard(shardID uint64) error`: calls `nh.StopCluster(shardID)`.
- Implement typed metadata helpers:
  - `SyncProposeMetadata(ctx, cmd MetadataCommand) (sm.Result, error)`: JSON-encodes `cmd`, calls `nh.SyncPropose(ctx, session, data)`.
  - `ProposeMetadata(ctx, cmd MetadataCommand) error`: calls `nh.Propose(ctx, session, data, timeout)`.
  - `LookupMetadata(ctx, q MetadataQuery) (any, error)`: calls `nh.ReadLocalNode(ctx, 0, q)`.
- Implement typed partition helpers:
  - `SyncProposePartition(ctx, shardID uint64, cmd PartitionCommand) (sm.Result, error)`: serializes cmd (1-byte prefix + payload), calls `nh.SyncPropose`.
  - `ProposePartition(ctx, shardID uint64, cmd PartitionCommand) error`.
  - `LookupPartition(ctx, shardID uint64, q PartitionQuery) (any, error)`.
- Implement `(*Host).Close() error`: calls `nh.Close()`.
- Define `MetadataCommand`, `MetadataQuery`, `PartitionCommand`, `PartitionQuery` types (populated in T-025; stubs here are sufficient for the Host layer to compile).

## Out of scope

- Metadata FSM implementation — T-025 through T-029.
- Partition FSM implementation — T-030, T-031.
- Client session management (NoOP session vs regular session) — use `nh.GetNoOPSession(shardID)` for simplicity in v1.

## Definition of done

- [ ] `go build ./internal/raft/...` passes.
- [ ] `go test ./internal/raft/...` passes (tests may use in-process dragonboat with a single node).
- [ ] `SyncProposeMetadata` serializes `MetadataCommand` as JSON before calling dragonboat.
- [ ] `SyncProposePartition` serializes `PartitionCommand` as `[type_byte, payload...]`.
- [ ] `LookupMetadata` and `LookupPartition` call `ReadLocalNode` on the correct shard ID.
- [ ] `NodeHostConfig` fields match the values in `03-raft-fsm.md §1.1`.

## Tests required

- `TestHost_MetadataPropose_Serialize` — create a `MetadataCommand`; call `SyncProposeMetadata` against a fake `NodeHost` stub that captures the raw bytes; verify JSON encoding.
- `TestHost_PartitionPropose_Serialize` — create a `PartitionCommand{Type: CmdAppendBatch, Payload: []byte("data")}`; verify first byte = 0x01 and rest = `"data"`.
- `TestHost_StartStop` — start a single-node in-process dragonboat cluster (metadata shard only), verify `StartMetadataShard` and `Close` succeed without error.

## Dependencies

T-012 (go.mod with dragonboat dependency).
T-001, T-002, T-003, T-004, T-005, T-006 (VERIFY tickets for dragonboat API — resolve before implementing; if still open, implement conservatively per the design doc values and adjust if needed).

## Notes

Use `nh.GetNoOPSession(shardID)` for all proposals in v1. A real client session provides sequence-number deduplication, which is not needed when callers are the coordinator (which already handles retries at a higher level). The `SnapshotEntries: 1<<62` value should be verified by T-001; if it causes a dragonboat config validation error, use `math.MaxUint64 / 2` or the maximum confirmed value.
