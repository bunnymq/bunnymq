# T-007: DECIDE — Partition FSM fsync strategy: immediate in Update() vs deferred to Sync()

**Milestone:** M0 — Foundations
**Effort:** XS
**Status:** DONE

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

---

## Findings

_Verified against `github.com/lni/dragonboat/v4@v4.0.0-20250723143628-076c7f6497dc`._

_Source files examined: `internal/settings/soft.go`, `node.go`, `snapshotstate.go`, `internal/rsm/statemachine.go`, `statemachine/disk.go`._

---

### dragonboat v4 `Sync()` call frequency

dragonboat calls `IOnDiskStateMachine.Sync()` from **one callsite** in `node.go`:

```go
// node.go
func (n *node) runSyncTask() {
    if !n.sm.OnDiskStateMachine() {
        return
    }
    if !n.syncTask.timeToRun(n.millisecondSinceStart()) {
        return
    }
    if !n.sm.TaskChanBusy() {
        n.pushTask(rsm.Task{PeriodicSync: true}, true)
    }
}
```

`runSyncTask()` is called once per `processRaftUpdate()` loop iteration, but `timeToRun` gates it by `SyncTaskInterval`. The default value from `internal/settings/soft.go`:

```go
SyncTaskInterval: 180000,  // 180,000 ms = 3 minutes
```

**`Sync()` fires at most once every 3 minutes.** It is **not** called per-batch, per-entry, or per-tick.

One additional callsite exists: `concurrentSave()` in `internal/rsm/statemachine.go` calls `s.sync()` before saving a snapshot for concurrent state machines. Because BunnyMQ's partition FSM uses Strategy A (no-op snapshots with `SnapshotEntries = 1<<62`), snapshot saves are effectively never triggered, so this path is irrelevant.

---

### Crash-safety impact of deferring fsync to `Sync()`

If `persistApplied` (fsync + sidecar write) is moved to `Sync()` instead of `Update()`:

- A crash at any point between two `Sync()` calls leaves the sidecar pointing to the state as of the last `Sync()`.
- On restart, `Open()` reads the stale sidecar and returns a `lastAppliedIndex` up to **3 minutes** behind the true state.
- dragonboat re-applies all Raft entries from `lastAppliedIndex + 1` through the current commit index.
- At 200 batches/s per partition, this can be up to **36,000 batches** to re-apply per partition.
- Re-applying is correct (dragonboat still has the entries in its WAL or can fetch them from the leader), but recovery time is proportional to the backlog.

The constraint from the `IOnDiskStateMachine.Update()` contract:

> it is strictly forbidden to have the data associated with the applied entry 3 available in the state machine while the one with index value 2 got lost during reboot.

With deferred sync, the Storage file may contain bytes from committed entries that the sidecar does not yet record. The `Open()` reconciliation already handles this via `TruncateTo`, but with deferred sync, the truncation window grows from "last `Update()` batch" to "up to 3 minutes of entries."

---

### Write amplification cost of immediate fsync

At 200 `Update()` calls/s per partition with immediate fsync:

- **NVMe SSD** (`fdatasync` latency ~0.05 ms): 200 × 0.05 ms = 10 ms/s of fsync overhead per partition — negligible.
- **Spinning disk** (`fdatasync` latency ~5 ms): 200 × 5 ms = 1,000 ms/s — would saturate the disk at just one active partition.

BunnyMQ targets NVMe storage for course-project use. Immediate fsync is acceptable.

Note: dragonboat's `BatchedEntryApply` is `true` by default (`internal/settings/soft.go`), meaning it may batch multiple committed entries into a single `Update()` call. The effective fsync rate is therefore ≤ the raw entry rate, reducing write amplification further.

---

### Decision

**Use immediate fsync in `Update()` (the current design in `03-raft-fsm.md §4.4`).**

Rationale:

1. dragonboat's `Sync()` fires at a 3-minute interval — far too infrequent to bound the re-apply window to an acceptable level for a message broker.
2. The deferred approach would require re-applying up to 36,000 batches per partition after a crash, making recovery slow.
3. The `Open()` recovery path (sidecar reconciliation via `TruncateTo`) is designed around the sidecar being consistent with `Update()` completion, not the `Sync()` cadence. Deferring would make the truncation window much larger and recovery harder to reason about.
4. Immediate fsync has negligible cost on NVMe (course-project target).
5. The deferred approach only pays off on spinning-disk deployments — not in scope for v1.

`Sync()` in the PartitionFSM remains a no-op (as written in `§4.6`):

```go
func (fsm *PartitionFSM) Sync() error {
    return nil // durability handled in Update() via persistApplied
}
```

---

### Definition of done checklist

- [x] dragonboat v4 `Sync()` call frequency documented.
- [x] Decision documented: immediate fsync in `Update()` (recommended).
- [x] If deferred: maximum data re-apply window quantified and accepted. (N/A — deferred rejected; window of 3 min / 36k batches documented as unacceptable.)
- [x] Decision to be referenced in the M2 PartitionFSM Update ticket.
