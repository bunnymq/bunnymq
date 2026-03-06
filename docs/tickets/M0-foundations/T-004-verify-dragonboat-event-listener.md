# T-004: VERIFY dragonboat v4 — IEventListener / leader-change callback

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

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

## Definition of done

- [ ] dragonboat v4 event listener availability confirmed (yes/no).
- [ ] If available: interface name, method signature, and `LeaderInfo` fields documented.
- [ ] Recommendation documented: callback + long-interval sweep vs sweep-only.

## Tests required

N/A — research ticket.

## Dependencies

None.

## Notes

Search dragonboat v4 source for `IEventListener`, `RaftEventListener`, `LeaderUpdated`, or `NodeHostConfig` event-related fields. The callback (if available) would be registered at `NodeHost` construction time, before any shard is started, and would fire on the goroutine managing that shard — check whether it is safe to call `SyncProposeMetadata` from inside the callback (likely not; should enqueue on a channel for a dedicated goroutine).
