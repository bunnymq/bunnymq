# T-004: VERIFY dragonboat v4 — IEventListener / leader-change callback

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** DONE

## Goal

Confirm whether dragonboat v4 exposes a callback mechanism for leader-change events, determine its interface, and recommend whether to replace the ClusterCoordinator's periodic leader sweep with a callback-driven approach.

## Context

`04-cluster-coordinator.md §6` documents the leader-epoch tracking mechanism as a periodic sweep every `leader_check_interval_ms` (default 3 s). An alternative is mentioned: dragonboat v4 may expose a `NodeHostConfig.RaftEventListener` field (candidate interface: `raft.IEventListener` with a `LeaderUpdated(LeaderInfo)` method). If available, the callback approach reduces epoch staleness from up to 3 s to near-zero and eliminates unnecessary `SyncProposeMetadata` calls when leadership has not changed.

References:
- [04-cluster-coordinator.md §6](../../design/04-cluster-coordinator.md#6-leader-epoch-tracking)
- [04-cluster-coordinator.md OQ1](../../design/04-cluster-coordinator.md#13-open-questions)

## Scope

- Inspect dragonboat v4 `NodeHostConfig` struct for event listener fields.
- Find and document the interface name, method signature(s), and callback parameters for leader-change events.
- Assess whether the callback fires reliably on every leader change (including stepdowns and elections during network partitions).
- Recommend: (a) callback as primary + sweep as 30 s safety net, or (b) sweep-only at 3 s interval.

## Out of scope

- Implementing the event listener or the sweep — M3 ClusterCoordinator tickets.

## Findings

### Interface confirmed: yes

dragonboat v4 exposes `raftio.IRaftEventListener` (package `github.com/lni/dragonboat/v4/raftio`):

```go
type IRaftEventListener interface {
    LeaderUpdated(info LeaderInfo)
}

type LeaderInfo struct {
    ShardID   uint64
    ReplicaID uint64
    Term      uint64
    LeaderID  uint64  // raftio.NoLeader (0) when there is no leader / during election
}
```

Registration is via `config.NodeHostConfig.RaftEventListener` (field type `raftio.IRaftEventListener`), set at `NodeHost` construction time before any shard is started.

### Goroutine semantics

NodeHost uses a **single dedicated goroutine** to invoke all `IRaftEventListener` methods sequentially (`config/config.go` comment: "NodeHost uses a single dedicated goroutine to invoke all RaftEventListener methods one by one"). CPU-intensive or IO operations must be offloaded to user-managed goroutines. Calling `SyncProposeMetadata` directly inside `LeaderUpdated` would block this goroutine and stall all subsequent leader-change callbacks for every shard — it must not be done.

### Callback reliability

`LeaderUpdated` fires on every leader change including stepdowns (where `LeaderID` is set to `raftio.NoLeader = 0`). It fires on every node that has the listener registered, not just the leader. Callbacks for distinct shards may interleave but are serialized through the single goroutine.

### Recommendation: callback as primary + 30 s sweep as safety net

Use `IRaftEventListener.LeaderUpdated` as the primary mechanism:
- `LeaderUpdated` enqueues a `LeaderInfo` value on a buffered channel (capacity ≥ number of partition shards).
- A dedicated goroutine drains the channel and calls `SyncProposeMetadata` with `AssignPartitionLeaderCmd`. Entries where `LeaderID == raftio.NoLeader` (election in progress) are silently dropped — the sweep will catch the resolved leader.
- Retain the periodic sweep (`leaderSweepLoop`) as a safety net at **30 s** (not 3 s) to recover from any dropped or missed callbacks.

This eliminates the up-to-3-second staleness window of the sweep-only approach and reduces unnecessary Raft proposals when leadership is stable.

## Definition of done

- [x] dragonboat v4 event listener availability confirmed (yes/no).
- [x] If available: interface name, method signature, and `LeaderInfo` fields documented.
- [x] Recommendation documented: callback + long-interval sweep vs sweep-only.

## Tests required

N/A — research ticket.

## Dependencies

None.

## Notes

Search dragonboat v4 source for `IEventListener`, `RaftEventListener`, `LeaderUpdated`, or `NodeHostConfig` event-related fields. The callback (if available) would be registered at `NodeHost` construction time, before any shard is started, and would fire on the goroutine managing that shard — check whether it is safe to call `SyncProposeMetadata` from inside the callback (likely not; should enqueue on a channel for a dedicated goroutine).
