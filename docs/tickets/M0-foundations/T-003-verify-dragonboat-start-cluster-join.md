# T-003: VERIFY dragonboat v4 — StartCluster join semantics

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** DONE

## Goal

Confirm the correct dragonboat v4 `StartCluster` call parameters for (a) bootstrapping a brand-new shard simultaneously across all replica nodes, and (b) a node joining an already-initialized shard.

## Context

`04-cluster-coordinator.md §7.4` includes a VERIFY comment about join semantics and proposes a heuristic: the node with the lowest `NodeID` in `ReplicaNodeIDs` calls `StartCluster` with `join=false` and `initialMembers=all replicas`; other nodes call with `join=true`. Open Question 3 of `04-CC.md` asks to confirm whether this heuristic is correct, or whether all nodes should simultaneously call `StartCluster` with `join=false` and the full `initialMembers` map.

References:
- [04-cluster-coordinator.md §7.4](../../design/04-cluster-coordinator.md#74-startshard)
- [04-cluster-coordinator.md OQ3](../../design/04-cluster-coordinator.md#13-open-questions)
- [03-raft-fsm.md §1.2](../../design/03-raft-fsm.md#12-lifecycle)

## Scope

- Read dragonboat v4 `NodeHost.StartCluster` / `StartOnDiskCluster` documentation and source.
- Confirm the correct parameter combination when all replica nodes start a brand-new shard simultaneously: do all use `join=false` with `initialMembers` = full replica map, or does one node use `join=false` and others use `join=true`?
- Confirm the correct parameters when an existing shard is already running in the cluster and a fresh node needs to join it (e.g., after `CreateTopic`, non-lowest-NodeID replicas start their shard instances).
- Confirm or replace the lowest-NodeID heuristic.
- Document the correct procedure for both scenarios.

## Out of scope

- Implementing `startShard` in ClusterCoordinator — M3 ticket.

## Definition of done

- [ ] New shard bootstrap: correct `StartCluster` parameters documented for all replica nodes.
- [ ] Existing shard join: correct `StartCluster` parameters documented.
- [ ] The lowest-NodeID heuristic confirmed or replaced with the correct dragonboat-recommended approach.
- [ ] Decision noted for use in T-012 repository skeleton and future CC implementation tickets.

## Tests required

N/A — research ticket.

## Dependencies

None.

## Notes

dragonboat v4 may expose `StartOnDiskCluster` for `IOnDiskStateMachine` types (partition shards) and `StartCluster` for `IStateMachine` types (metadata shard). Verify whether there is a separate method for on-disk FSMs. Also confirm: does dragonboat v4 require `join=true` nodes to know the `initialMembers` map, or can it be nil when joining an existing cluster?

---

## Resolution

**Investigated:** dragonboat v4 source (`nodehost.go`, `raftpb/raft.go`) — version `v4.0.0-20250723143628-076c7f6497dc`.

### Finding 1 — Method names changed: `StartCluster` → `StartReplica` / `StartOnDiskReplica`

In dragonboat v4 there is **no `StartCluster` or `StartOnDiskCluster`**. The design documents' proposed API calls must be updated:

| Design doc proposal | Correct dragonboat v4 method |
|---|---|
| `StartCluster(initialMembers, join, factory, rc)` | `StartReplica(initialMembers map[uint64]Target, join bool, create sm.CreateStateMachineFunc, cfg config.Config) error` |
| `StartOnDiskCluster(...)` | `StartOnDiskReplica(initialMembers map[uint64]Target, join bool, create sm.CreateOnDiskStateMachineFunc, cfg config.Config) error` |

`Target` is a type alias: `type Target = string`. By default it is the Raft address (`host:port`); when `DefaultNodeRegistryEnabled` (gossip mode) is used it is a UUID-format NodeHostID string. BunnyMQ uses the default mode, so `Target = RaftAddress`.

Also: `ClusterID`/`NodeID` were renamed to `ShardID`/`ReplicaID` in v4. The `config.Config` struct fields are `ShardID uint64` and `ReplicaID uint64`.

### Finding 2 — New shard bootstrap: ALL replicas call `join=false` with full `initialMembers`

The **lowest-NodeID heuristic proposed in §7.4 is wrong**. When bootstrapping a brand-new shard, every initial replica node calls `StartReplica` (or `StartOnDiskReplica`) with:

```go
join = false
initialMembers = map[uint64]Target{ /* all replica nodeIDs → raftAddresses */ }
```

This is confirmed by the dragonboat v4 docstring for `StartReplica`:

> When starting a brand new Raft shard, set join to false and specify all initial member node details in the initialMembers map.

There is no designated "bootstrapper" node. All N initial members call `StartReplica(join=false, initialMembers=fullMap)` simultaneously. Dragonboat runs a standard Raft leader election after all peers are up; there is no requirement for any single node to go first.

**The proposed heuristic** ("lowest NodeID calls `join=false`; all others call `join=true`") is incorrect and would cause the other replicas to return `ErrShardNotBootstrapped` (because `join=false` with an empty map on an un-bootstrapped node is an error, and `join=true` is for adding a new member to an *already-running* shard, not for initial bootstrap).

### Finding 3 — Joining an existing shard: `join=true`, `initialMembers` MUST be nil

When a node is added to an existing running shard (via `RequestAddReplica` membership change), the newly added node calls:

```go
join = true
initialMembers = nil // or empty map
```

Passing a non-empty `initialMembers` with `join=true` is a hard error — dragonboat returns `ErrInvalidShardSettings` immediately (enforced at line `nodehost.go:1552`):

```go
if join && len(initialMembers) > 0 {
    return nil, ErrInvalidShardSettings
}
```

**Relevance to BunnyMQ v1:** Cluster membership is static (REQUIREMENTS.md §3.2.3). No `RequestAddReplica` is ever issued, so the `join=true` path is **never needed in v1**. It is documented here for completeness only.

### Finding 4 — Restart (previously bootstrapped node): `join=false`, `initialMembers` can be nil or full map

When a previously bootstrapped replica restarts after a crash or stop:

```go
join = false
initialMembers = nil // safe: dragonboat reads saved bootstrap info from its logdb
// OR
initialMembers = fullMap // also works if addresses haven't changed
```

Dragonboat checks its internal logdb (`GetBootstrapInfo`) on every `StartReplica` call:
- If no bootstrap info exists → treats call as first-time bootstrap → requires non-empty `initialMembers`.
- If bootstrap info exists → validates incoming `initialMembers` against saved info (addresses must match if non-empty) → proceeds with restart.

Passing an empty map on restart is always safe. Passing the full peer map also works provided the node addresses are the same (which they always are in BunnyMQ's static cluster).

### Finding 5 — Recommended implementation for `startShard` in BunnyMQ

Because BunnyMQ v1 has static membership and the reconcile goroutine does not distinguish "first start" from "restart", the simplest and correct implementation is:

```go
func (cc *ClusterCoordinator) startShard(shardID uint64, info shardInfo) {
    // For a new shard (first time) AND for restart, dragonboat handles both
    // when join=false + full initialMembers: on first start it bootstraps;
    // on restart it validates the peer map against saved bootstrap info
    // (always passes since cluster membership is static).
    err := cc.raftHost.StartPartitionShard(info.ShardID, info.Peers, false /* join */)
    // ...
}
```

The `join` parameter in `StartPartitionShard` should always be `false` for partition shards. The `info.Peers` map always contains the full set of replica node IDs → Raft addresses. The lowest-NodeID heuristic and the `join` variable in §7.4 should be removed.

For the metadata shard bootstrap (§8 Step 1), the same rule applies: all broker nodes call `StartReplica(join=false, initialMembers=config.Peers)` simultaneously.

### DoD checklist

- [x] New shard bootstrap: correct `StartReplica` parameters documented — all replica nodes call `join=false, initialMembers=fullMap`. The lowest-NodeID heuristic is incorrect and must not be implemented.
- [x] Existing shard join: `join=true, initialMembers=nil` — only for `RequestAddReplica` additions; not needed in v1 static-membership cluster.
- [x] The lowest-NodeID heuristic is **replaced**: all nodes call `join=false` with full peer map for new shard bootstrap.
- [x] Decision noted for T-012 repository skeleton and future CC implementation tickets: use `StartReplica` / `StartOnDiskReplica` (not `StartCluster` / `StartOnDiskCluster`); always `join=false` with full peer map; `ShardID`/`ReplicaID` (not `ClusterID`/`NodeID`).
