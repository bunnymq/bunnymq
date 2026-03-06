# T-007: DECIDE — Partition FSM fsync strategy: immediate in Update() vs deferred to Sync()

**Milestone:** M0 — Foundations
**Effort:** XS
**Status:** TODO

## Goal

Decide whether `PartitionFSM.Update()` fsyncs the log and atomically renames the sidecar after every call, or defers the fsync to the dragonboat `Sync()` hook, and document the decision with its crash-safety implications.

## Context

`03-raft-fsm.md §4.4` and OQ3 describe the current design: every `Update()` call invokes `persistApplied(index)` which: (1) calls `storage.Sync()` (fsync the active log file), (2) writes the sidecar atomically. This ensures that on any crash, the sidecar and log are consistent. The alternative (deferred): move the fsync and sidecar write to the `Sync()` hook that dragonboat calls periodically, batching the durability cost across many `Update()` calls. The trade-off is write amplification vs crash-recovery complexity.

References:
- [03-raft-fsm.md §4.4](../../design/03-raft-fsm.md#44-update)
- [03-raft-fsm.md §4.3](../../design/03-raft-fsm.md#43-open--partition-recovery) — sidecar reconciliation during Open()
- [03-raft-fsm.md OQ3](../../design/03-raft-fsm.md#6-open-questions)

## Scope

- Determine how frequently dragonboat v4 calls `IOnDiskStateMachine.Sync()` relative to `Update()` (per-batch? per-tick? configurable?).
- Evaluate the crash-safety impact of deferring: if a crash occurs between `Update()` and `Sync()`, how many Raft entries must be re-applied on restart? Is this bounded?
- Evaluate write amplification cost of immediate fsync: at 200 batches/s, that is 200 fsync calls per second per partition, which is significant on spinning disk but negligible on NVMe SSD.
- Make the decision and document it clearly.

## Out of scope

- Implementing `Update()` or `Sync()` — M2 PartitionFSM ticket.

## Definition of done

- [ ] dragonboat v4 `Sync()` call frequency documented.
- [ ] Decision documented: immediate fsync in `Update()` (recommended) or deferred to `Sync()`.
- [ ] If deferred: maximum data re-apply window quantified and accepted.
- [ ] Decision to be referenced in the M2 PartitionFSM Update ticket.

## Tests required

N/A — decision ticket; no executable test.

## Dependencies

T-001 (general dragonboat API understanding useful context).

## Notes

The current design in `03-raft-fsm.md` favors immediate fsync in `Update()`. The recovery path in `Open()` is designed around the sidecar being always consistent with the log at the cost of per-batch fsync. For a course-project demo with NVMe storage, immediate fsync is the safer, simpler choice. The deferred approach only pays off when targeting spinning-disk deployments or very high per-partition batch rates. Recommend keeping immediate fsync for v1 unless dragonboat calls `Sync()` after every committed batch anyway (which would make the trade-off moot).
