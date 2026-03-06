# T-022: Crash recovery on startup (Storage.Open)

**Milestone:** M1 — Storage standalone
**Effort:** M
**Status:** TODO

## Goal

Implement the 5-step crash recovery algorithm in `Storage.Open` that enumerates segments on disk, validates the active segment via CRC-32C scan, truncates any partial tail write, and rebuilds the active segment's indexes — producing a consistent `Storage` state from any crash scenario.

## Context

The storage layer tolerates crashes at any point during an `Append` or segment seal because the Raft log replay re-applies entries after restart. Recovery must return the correct `nextOffset` so the Partition FSM can reconcile against the sidecar (`applied.idx`) to determine whether any replay is needed. Three crash scenarios are handled: clean shutdown (no truncation), crash during append (CRC check fails at the tail), crash during seal (partially-sealed files detected).

References:
- [02-storage.md §6 — Crash recovery](../../design/02-storage.md#6-crash-recovery)
- [02-storage.md §9 — Failure modes](../../design/02-storage.md#9-failure-modes)

## Scope

- Implement `recoverStorage(dir string, config *StorageConfig) ([]*SegmentStorage, int64, error)` called from `Storage.Open`:

  **Step 1 — Enumerate segments:**
  - `filepath.Glob` for `*.log` files; parse the 20-digit filename as `base_offset`.
  - Sort ascending. If none found, create the first segment with `base_offset = 0`; return immediately.

  **Step 2 — Open sealed segments:**
  - For each `.log` except the last: call `OpenSegmentStorage(dir, baseOffset, config, readonly=true)`.
  - Validate index file sizes are multiples of entry size (8 for `.index`, 12 for `.timeindex`); if not, truncate to the nearest valid multiple (handles crash during `ftruncate`).

  **Step 3 — Recover the active segment:**
  - Scan the last `.log` from byte 0, reading 38-byte headers; verify `batch_length ≥ 38` and `position + batch_length ≤ fileSize`; compute CRC-32C over `records[]` and compare to header field.
  - On first failure: truncate the `.log` file to the current `position`; stop scan.
  - Track `nextOffset = baseOffset + sum(recordCount)` across valid batches.

  **Step 4 — Rebuild active segment indexes:**
  - Re-scan bytes `[0, validPosition)` of the active `.log`.
  - Apply the same `index_sample_bytes` sampling logic as `SegmentStorage.Append`.
  - Write entries into freshly mmap'd `.index` and `.timeindex` files (delete existing files if present, create new pre-allocated ones).

  **Step 5 — Open active segment for writing:**
  - Re-open the active `.log` with `O_WRONLY|O_APPEND`.
  - Set `logSize = validPosition`.

- Return `([]*SegmentStorage, nextOffset, nil)`.

## Out of scope

- sidecar (`applied.idx`) reconciliation — that is owned by the Partition FSM (M2 tickets).
- Sealed segment deletion — T-021.
- Normal append path — T-020.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] Clean shutdown recovery: `nextOffset` equals the offset after the last appended batch.
- [ ] Partial tail write: last batch header truncated (< 38 bytes in file) → `nextOffset` excludes it.
- [ ] CRC mismatch: batch with corrupted `records[]` → file truncated at that batch start → `nextOffset` excludes it.
- [ ] After recovery, `Append` continues from `nextOffset` without gap.
- [ ] Sealed segments opened read-only; index size validated.
- [ ] Rebuilt active indexes match the state that would have been produced by a clean append sequence.

## Tests required

- `TestRecovery_CleanShutdown` — write 5 batches, close cleanly, reopen; `LatestOffset` = expected value; all 5 batches readable.
- `TestRecovery_PartialHeader` — write 4 batches, then manually append 10 random bytes; reopen; `LatestOffset` = offset after batch 4.
- `TestRecovery_CRCMismatch` — write 3 batches cleanly, then write a batch with a flipped byte in `records[]`; reopen; only first 3 batches present.
- `TestRecovery_EmptyDir` — open on empty directory; `LatestOffset = 0`; single empty active segment created.
- `TestRecovery_MultipleSegments` — two sealed segments + one active (partially crashed); reopen; all sealed data intact; active truncated correctly.
- `TestRecovery_IndexSizeValidation` — manually truncate `.index` file to odd byte count; reopen; index treated as empty and rebuilt.
- `TestRecovery_AppendAfterRecovery` — after partial-write recovery, append one more batch; it lands at `nextOffset` with correct `base_offset`.

## Dependencies

T-015 (CRC-32C and batch header parsing).
T-016 (LogSegment).
T-017 (OffsetIndexSegment rebuild).
T-018 (TimeIndexSegment rebuild).
T-019 (SegmentStorage open).

## Notes

The CRC-32C is computed over `records[]` = bytes `[38, batch_length)`. The scan loop reads only the 38-byte header to get `batch_length`, then reads the records payload for CRC verification — do not load the full batch before checking `batch_length` is sane (guards against corrupt `batch_length` causing an enormous allocation). The fallback for crashed-during-seal: if the last `.log` file's name suggests it should be a sealed segment (because a newer `.log` file also exists), the same scan applies — the sealed candidate is treated like an active segment for recovery purposes, then sealed after validation.
