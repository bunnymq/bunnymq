# T-008: DECIDE — Retention enforcement: local independent vs Raft-commanded

**Milestone:** M0 — Foundations
**Effort:** XS
**Status:** DONE

## Goal

Decide whether each partition replica enforces retention independently via a Storage-internal background goroutine, or the partition shard leader issues `DeleteSegmentsBefore` Raft commands so all replicas delete at the same log index, and document the decision.

## Context

`02-storage.md §7` and `05-data-coordinator.md §8` both document the current design: each replica runs its own background retention goroutine that ticks every `retention_check_interval_ms` (default 5 min) and calls `storage.EnforceRetention()`. This means replicas may delete different segments within a `retention_check_interval_ms` window of each other — eventual consistency in segment visibility. OQ3 of `05-data-coordinator.md` raises the alternative: the leader computes which segments to delete and issues a `DeleteSegmentsBefore(earliestValidOffset int64)` command via Raft, ensuring all replicas delete identically at the same log index. This requires adding a new `0x03` command type to `03-raft-fsm.md` and a `DeleteSegmentsBefore(offset int64) error` method to `internal/storage.Storage`.

References:
- [02-storage.md §7](../../design/02-storage.md#7-retention-enforcement)
- [05-data-coordinator.md §8](../../design/05-data-coordinator.md#8-retention-enforcement)
- [05-data-coordinator.md OQ3](../../design/05-data-coordinator.md#12-open-questions)

## Scope

- Evaluate the practical impact of eventual-consistency retention on consumer behavior (`OffsetOutOfRange` from one replica before others).
- Evaluate the implementation complexity of Raft-commanded retention (new FSM command type, new Storage method, leader-only retention loop, follower application).
- Make the decision.
- If Raft-commanded is chosen: list the design additions required (new PartitionCommand type `0x03`, Storage interface addition, DataCoordinator retention loop ownership) and flag affected M2/M3 tickets.

## Out of scope

- Implementing retention enforcement — M2 Storage tickets (local) or M3 DataCoordinator tickets (Raft-commanded).

## Definition of done

- [ ] Decision documented: local independent enforcement (recommended) or Raft-commanded.
- [ ] If Raft-commanded: new command type `0x03` and `Storage.DeleteSegmentsBefore(int64)` documented as required additions, and affected tickets flagged.
- [ ] Decision referenced in future M2 storage retention ticket (T-028 area).

## Tests required

N/A — decision ticket.

## Dependencies

None.

## Notes

The local-independent approach is strongly recommended for v1. All replicas apply identical Raft entries and therefore write identical bytes. Since retention is based on time and size thresholds applied to sealed segments, replicas delete the same segments within one `retention_check_interval_ms` window. A consumer hitting `OffsetOutOfRange` on one replica will receive the same error from all replicas within 5 minutes. This is acceptable for a course-project demo. The Raft-commanded approach adds ~150 lines of implementation and a new FSM command for no meaningful consumer-visible benefit at this scale.

---

## Findings

_References: `02-storage.md §7`, `05-data-coordinator.md §8`, `05-data-coordinator.md OQ3`._

---

### Practical impact of eventual-consistency retention

Under local-independent enforcement each replica's background goroutine runs on its own 5-minute tick. Two replicas may therefore reach the deletion decision up to one full `retention_check_interval_ms` (300 000 ms) apart. However, because all replicas apply identical Raft entries they hold **byte-for-byte identical segment files**. Retention eligibility — whether a sealed segment's next-segment timestamp is older than `retentionMs`, or whether accumulated segment bytes exceed `retentionBytes` — evaluates to the same boolean on every replica given the same wall-clock window.

**Consumer-visible impact.** The only consumer-visible symptom is a transient period in which a segment is deleted on replica A but not yet on replica B. During this window a consumer reading from replica A receives `OffsetOutOfRange`; if it reconnects to replica B it succeeds. Within the next tick (≤ 5 min) replica B performs the same deletion and both replicas are consistent. For a course-project demo this transient divergence is acceptable: the consumer's recovery path (`GetEarliestOffset` → reset position) already handles `OffsetOutOfRange` regardless of cause.

No `OffsetOutOfRange` result on a non-leader follower is possible in BunnyMQ v1 because only the shard leader serves reads. All produce/fetch traffic is pinned to the leader via `leaderCheck`. Follower-read inconsistency is therefore a non-issue: the consumer always talks to a single, stable leader until a leader change occurs, at which point the 5-minute window resets.

---

### Implementation complexity of Raft-commanded retention

The Raft-commanded alternative requires:

| Addition | Where | Scope |
|---|---|---|
| New `PartitionCommand` type `0x03 DeleteSegmentsBefore` with field `EarliestValidOffset int64` | `03-raft-fsm.md §4.2` | Immutable design doc amendment |
| `Storage.DeleteSegmentsBefore(offset int64) error` method | `02-storage.md §2` (`Storage` interface) | Immutable design doc amendment + new implementation |
| Leader-only retention loop in `DataCoordinator` | `05-data-coordinator.md §8` | DataCoordinator implementation |
| FSM `Update()` handling for `0x03` | `03-raft-fsm.md §4.4` | PartitionFSM implementation |

Estimated additions: ~150 lines of production code, ~80 lines of tests, plus amendments to two immutable design documents. The `PartitionFSM.Update()` determinism constraint already satisfied (the leader computes the cutoff offset from wall-clock time before proposing; the FSM applies the precomputed offset deterministically). However, the leader-only retention loop introduces a new goroutine in `DataCoordinator` with its own lifecycle (start on `StartPartitionReplica`, stop on `StopPartitionReplica`), leadership-change handling, and a new dragonboat `SyncPropose` on every 5-minute tick — all for no consumer-visible improvement in v1.

---

### Decision

**Use local independent enforcement — the current design in `02-storage.md §7` and `05-data-coordinator.md §8`.**

Rationale:

1. All replicas hold identical bytes (Raft guarantees identical Apply sequences), so retention decisions are identical given the same threshold values and arrive within one `retention_check_interval_ms` (5 min) of each other.
2. Only the shard leader serves reads in v1, so the cross-replica transient divergence window is never visible to a consumer in practice.
3. The Raft-commanded alternative requires amending two immutable design documents, adding a new FSM command type, a new `Storage` method, and a leader-only goroutine in `DataCoordinator` — ~230 lines of code with no consumer-visible benefit at course-project scale.
4. The `OffsetOutOfRange` recovery path (call `GetEarliestOffset` and reset) is required regardless; consumers must handle it. The source of the out-of-range condition (local retention vs. Raft command) is irrelevant to the recovery procedure.

No design document amendments are required. The Raft-commanded approach is deferred indefinitely; it is not planned for any M2/M3 ticket.

---

### Definition of done checklist

- [x] Decision documented: local independent enforcement chosen.
- [x] If Raft-commanded: new command type `0x03` and `Storage.DeleteSegmentsBefore(int64)` documented as required additions, and affected tickets flagged. (N/A — Raft-commanded rejected; additions enumerated above for reference only, no tickets flagged.)
- [x] Decision referenced in future M2 storage retention ticket (T-028 area): local independent enforcement is the correct implementation target; no design doc amendments needed before T-028.
