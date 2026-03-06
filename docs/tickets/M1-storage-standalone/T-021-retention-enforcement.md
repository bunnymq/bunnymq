# T-021: Retention enforcement

**Milestone:** M1 — Storage standalone
**Effort:** S
**Status:** TODO

## Goal

Implement the background retention goroutine and `EnforceRetention` in `internal/storage`, deleting sealed segments that violate the time or bytes retention policy.

## Context

Retention is the mechanism that prevents unbounded disk growth. The design uses union semantics: a segment is eligible for deletion if it is expired by time **or** exceeds the cumulative bytes cap. Only sealed segments are eligible; the active segment is always kept. `SetRetentionConfig` allows per-topic overrides to be delivered by the Partition FSM after topic creation.

References:
- [02-storage.md §7 — Retention enforcement](../../design/02-storage.md#7-retention-enforcement)
- [02-storage.md §5.5 — Segment deletion](../../design/02-storage.md#55-segment-deletion)

## Scope

- Implement `(*storageImpl).EnforceRetention(retentionMs int64, retentionBytes int64) (deletedSegments int, err error)`:
  - Snapshot segment slice under `segMu.RLock()`; release before any deletions.
  - Exclude the active (last) segment from all eligibility checks.
  - **Bytes retention:** accumulate `LogSize()` from oldest to newest sealed segment. Mark segments for deletion until remaining total ≤ `retentionBytes`. If total ≤ `retentionBytes`, mark none.
  - **Time retention:** for each sealed segment `S[i]`, check `S[i+1].firstBatchBaseTimestamp() < now - retentionMs`. If true, mark `S[i]` for deletion. `firstBatchBaseTimestamp` reads the first batch header from the `.log` file (bytes [22,30] = `base_timestamp`).
  - Delete all marked segments: for each, acquire `segMu.Lock()`, splice from `segments`, release, then call `os.Remove` on the three files (outside the lock).
  - Returns count of deleted segments.
- Implement `(*storageImpl).startRetentionLoop(ctx context.Context, interval time.Duration)`:
  - Ticks every `interval` (default `config.RetentionCheckIntervalMs`).
  - Reads `retentionMs` and `retentionBytes` from atomics.
  - Calls `EnforceRetention`.
  - Logs any returned error at `warn` level; does not stop the loop on error.
  - Exits when `ctx.Done()` is closed.
- Implement `(*storageImpl).SetRetentionConfig(retentionMs, retentionBytes int64)`:
  - Stores values in `retentionMs` and `retentionBytes` atomics.

## Out of scope

- Retention command via Raft (Partition FSM calls `SetRetentionConfig` on the Storage; that wiring is M2).
- Time-based segment roll — explicitly out of scope per `02-storage.md §5.3`.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] `EnforceRetention` does not delete the active segment under any configuration.
- [ ] Time-expired segments deleted correctly; unexpired segments preserved.
- [ ] Bytes-over-cap segments deleted oldest-first until under cap.
- [ ] Segments deleted by both time and bytes are not double-deleted.
- [ ] `SetRetentionConfig` values take effect on the next `EnforceRetention` call.

## Tests required

- `TestRetention_TimeExpired` — 3 sealed segments; middle one expired by time; only middle deleted.
- `TestRetention_BytesOverCap` — 3 sealed segments totalling 300 bytes; cap = 100 bytes; oldest 2 deleted.
- `TestRetention_ActiveNeverDeleted` — single active segment; EnforceRetention is a no-op.
- `TestRetention_NoneEligible` — all segments fresh and under cap; 0 deleted.
- `TestRetention_UnionSemantics` — segment expired by bytes but not time; still deleted.
- `TestRetention_SetConfig_Applied` — call SetRetentionConfig; next EnforceRetention uses new values.
- `TestRetention_LoopStopsOnContextCancel` — start retention loop goroutine, cancel context, verify goroutine exits.

## Dependencies

T-019 (SegmentStorage.LogSize, segment file structure).
T-020 (Storage struct; retention loop is started from Storage.Open).

## Notes

The time retention check uses the *next* segment's `base_timestamp` (first batch header bytes [22,30]): this ensures a segment is not deleted until all of its batches are definitively older than the threshold — consistent with Kafka semantics per `02-storage.md §7`. Union semantics (OR, not AND) means a segment is deleted if *either* criterion is met. The file removal (`os.Remove` × 3) happens outside `segMu` to avoid blocking reads during the slower syscall.
