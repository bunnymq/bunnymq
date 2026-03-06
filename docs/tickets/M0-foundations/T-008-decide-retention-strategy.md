# T-008: DECIDE — Retention enforcement: local independent vs Raft-commanded

**Milestone:** M0 — Foundations
**Effort:** XS
**Status:** TODO

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
