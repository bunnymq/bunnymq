# T-002: VERIFY dragonboat v4 — GetLeaderID and leader query API

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** DONE

## Goal

Confirm the exact signature of dragonboat v4's leader-query method on `NodeHost` and determine whether the returned value can serve as `LeaderEpoch` in `AssignPartitionLeaderCmd`.

## Context

Two design documents reference a `GetLeaderID`-style call with differing proposed signatures:

- `04-cluster-coordinator.md §6` proposes `NodeHost.GetLeaderID(clusterID uint64) (leaderID uint64, term uint64, valid bool)`.
- `08-consumer-groups.md §9` proposes `nh.GetLeaderID(shardID) (leaderID uint64, valid bool, err error)`.

The ClusterCoordinator's leader sweep (`04-CC.md §6`) uses this call to detect leadership changes and propose `AssignPartitionLeaderCmd` with a `LeaderEpoch`. The GroupCoordinator's session timeout sweep (`08-consumer-groups.md §9`) uses it to guard against running the sweep on a non-leader node.

References:
- [04-cluster-coordinator.md §6](../../design/04-cluster-coordinator.md#6-leader-epoch-tracking)
- [08-consumer-groups.md §9](../../design/08-consumer-groups.md#9-session-timeout-enforcement)

## Scope

- Inspect dragonboat v4 `NodeHost` exported methods for leader queries.
- Confirm exact method name, parameter types, and return types.
- Determine whether a `term` or equivalent value is returned; if so, confirm it is monotonically increasing and suitable for use as `LeaderEpoch` in `AssignPartitionLeaderCmd`.
- Document the correct call pattern for: (a) ClusterCoordinator leader sweep, (b) GroupCoordinator sweep guard.

## Out of scope

- Implementing either sweep loop — covered in M3 ClusterCoordinator and M4 GroupCoordinator tickets.

## Definition of done

- [ ] Exact dragonboat v4 method signature for leader query documented (name, params, returns).
- [ ] Confirmed whether a `term` or epoch equivalent is returned.
- [ ] Decision documented: what value to use as `LeaderEpoch` in `AssignPartitionLeaderCmd`.
- [ ] Call patterns for CC leader sweep and GC sweep guard documented.

## Tests required

N/A — research ticket. No executable test required.

## Dependencies

None.

## Notes

Look in `github.com/lni/dragonboat/v4` for `NodeHost.GetLeaderID` or similar. Check whether the method is on `NodeHost` directly or on an `INodeHost` interface. The `valid bool` return likely indicates whether an election is in progress. If no `term` is returned, the `LeaderEpoch` can be derived from a monotonically increasing counter maintained in the coordinator rather than from dragonboat.

---

## Resolution

**Investigated:** dragonboat v4 source (`nodehost.go`, `node.go`, `internal/raft/raft.go`) — version `v4.0.0-20250723143628-076c7f6497dc`.

### Finding 1 — Exact method signature (4 returns, not 3)

```go
// nodehost.go:681
func (nh *NodeHost) GetLeaderID(shardID uint64) (uint64, uint64, bool, error)
// returns: leaderID uint64, term uint64, valid bool, err error
```

The method is on `*NodeHost` directly. There is **no `INodeHost` interface** in dragonboat v4 that includes `GetLeaderID` — only `INodeHostRegistry` exists (for gossip registry queries). Both design docs had the signature partially wrong:

- `04-cluster-coordinator.md §6` proposed `(leaderID, term, valid)` — correct fields, but missing `err`.
- `08-consumer-groups.md §9` proposed `(leaderID, valid, err)` — missing `term`.

The actual signature is `(leaderID uint64, term uint64, valid bool, err error)`.

### Finding 2 — `valid bool` semantics

`valid` is set to `leaderID != raft.NoLeader` where `raft.NoLeader = 0`:

```go
// node.go:573
func (n *node) getLeaderID() (uint64, uint64, bool) {
    lv := n.leaderInfo.Load()
    if lv == nil {
        return 0, 0, false
    }
    leaderInfo := lv.(*leaderInfo)
    return leaderInfo.leaderID, leaderInfo.term, leaderInfo.leaderID != raft.NoLeader
}
```

`valid = false` when:
- The node has not yet received any leader information (startup / first election not yet seen locally).
- The shard is in an election and the new leader has not been announced.

### Finding 3 — `term` is Raft election term; suitable as `LeaderEpoch`

The `term` field comes from `leaderInfo.term`, which is the standard Raft election term. By the Raft protocol invariant, **a new leader can only be elected in a strictly higher term than any previous leader**. The term is therefore monotonically increasing across leader changes for a given shard.

**Decision:** Use `term` directly as `LeaderEpoch` in `AssignPartitionLeaderCmd`. No application-level counter is needed.

### Finding 4 — `err` semantics

`err` is non-nil only in two cases:
- `ErrClosed` — the `NodeHost` has been shut down.
- `ErrShardNotFound` — the shard ID is not registered with this `NodeHost`.

Both are programming errors or shutdown conditions, not transient election states. Callers should treat non-nil `err` as fatal (log + return).

### Finding 5 — IsLeader check for GC sweep guard

`GetLeaderID` returns the **leader's `replicaID`**, not a boolean for "am I the leader?". The GC coordinator must compare `leaderID` against its own `replicaID` for the metadata shard:

```go
leaderID, _, valid, err := nh.GetLeaderID(metadataShardID)
if err != nil || !valid {
    return // not safe to run sweep
}
if leaderID != myReplicaID {
    return // we are a follower
}
// run sweep
```

`myReplicaID` is the `ReplicaID` value passed to `NodeHost.StartReplica` / `StartOnDiskCluster` at node startup. The coordinator must store this at construction time. `NodeHost.ID()` returns a UUID string (the NodeHost's persistent identity), which is **not** the Raft `replicaID` — do not confuse them.

### Call patterns for implementation tickets

**CC leader sweep (`04-cluster-coordinator.md §6`):**

```go
leaderID, term, valid, err := nh.GetLeaderID(shardID)
if err != nil {
    return fmt.Errorf("GetLeaderID shard %d: %w", shardID, err)
}
if !valid {
    continue // election in progress, skip this shard this sweep
}
// propose AssignPartitionLeaderCmd with LeaderID=leaderID, LeaderEpoch=term
```

**GC sweep guard (`08-consumer-groups.md §9`):**

```go
leaderID, _, valid, err := nh.GetLeaderID(metadataShardID)
if err != nil || !valid || leaderID != gc.replicaID {
    return // not the leader, skip sweep
}
```

### DoD checklist

- [x] Exact dragonboat v4 method signature documented — `func (nh *NodeHost) GetLeaderID(shardID uint64) (uint64, uint64, bool, error)` → `(leaderID, term, valid, err)`.
- [x] Confirmed `term` is returned — it is the Raft election term (second return value).
- [x] Decision documented — use `term` directly as `LeaderEpoch` in `AssignPartitionLeaderCmd`; it is monotonically increasing by Raft invariant.
- [x] Call patterns for CC leader sweep and GC sweep guard documented above.
