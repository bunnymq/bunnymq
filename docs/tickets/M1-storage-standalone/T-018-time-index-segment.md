# T-018: TimeIndexSegment with mmap

**Milestone:** M1 — Storage standalone
**Effort:** S
**Status:** TODO

## Goal

Implement `TimeIndexSegment` in `internal/storage` that wraps a mmap'd `.timeindex` file with 12-byte entries (timestamp_ms int64 + relative_offset int32), mirroring the lifecycle of `OffsetIndexSegment` but supporting binary search by timestamp.

## Context

`TimeIndexSegment` enables `ReadByTime`: given a timestamp, find the first batch whose `max_timestamp >= timestampMs`. The time index is sparse with the same `index_sample_bytes` sampling rate as the offset index. The implementation is structurally identical to `OffsetIndexSegment` — same mmap lifecycle, same seal procedure — differing only in entry size (12 vs 8 bytes), key type (int64 vs int32), and binary-search semantics (ceiling vs floor).

References:
- [02-storage.md §1 — TimeIndexSegment](../../design/02-storage.md#1-component-hierarchy)
- [02-storage.md §3.3 — Index entry layouts](../../design/02-storage.md#33-index-entry-layouts)
- [02-storage.md §4 — mmap mechanics](../../design/02-storage.md#4-mmap-mechanics)

## Scope

- Define `TimeIndexSegment` struct in `internal/storage/time_index.go`:
  - Fields: `file *os.File`, `data []byte`, `entryCount atomic.Int64`, `baseOffset int64`, `maxEntries int64`.
- Implement `OpenTimeIndex(path string, baseOffset int64, segMaxBytes int64, indexSampleBytes int) (*TimeIndexSegment, error)`:
  - Same pre-allocation logic as `OffsetIndexSegment`, but entry size = 12.
  - Pre-allocation: `ceil(segMaxBytes / indexSampleBytes) × 12` rounded up to page size.
- Implement `(*TimeIndexSegment).Append(timestampMs int64, relativeOffset int32)`:
  - Writes 12 bytes at slot `entryCount`: big-endian int64 then int32.
  - Increments `entryCount` atomically after write.
- Implement `(*TimeIndexSegment).Lookup(timestampMs int64) (relativeOffset int32, found bool)`:
  - Binary search for the **ceiling** — the smallest entry whose `timestamp_ms >= timestampMs`.
  - Returns `(relativeOffset, true)` if found, `(0, false)` if all entries are older than `timestampMs`.
  - This gives the conservative starting point for the log scan in `ReadByTime`.
- Implement `(*TimeIndexSegment).Seal() error`: identical procedure to `OffsetIndexSegment.Seal` with entry size 12.
- Implement `(*TimeIndexSegment).Rebuild(entries []timeEntry)` for crash recovery (T-022).
- Implement `(*TimeIndexSegment).Close() error`.

## Out of scope

- `OffsetIndexSegment` — T-017.
- Integration with SegmentStorage — T-019.
- Crash recovery rebuild invocation — T-022.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] `Lookup` returns ceiling: first entry ≥ query timestamp.
- [ ] `Lookup` returns `found=false` when all entries are older than the query timestamp.
- [ ] Seal shrinks file to `entryCount × 12`.
- [ ] Pre-allocation uses entry size 12.

## Tests required

- `TestTimeIndex_AppendLookup` — append 3 entries at timestamps 100, 200, 300; lookup 200 returns offset for the 200 entry.
- `TestTimeIndex_CeilingLookup` — lookup 150 (between 100 and 200) returns the entry at 200 (ceiling).
- `TestTimeIndex_NoneFound` — lookup 400 (past all entries) returns `found=false`.
- `TestTimeIndex_AllOlder` — all entries have timestamps < query; returns `found=false`.
- `TestTimeIndex_Seal` — append 2 entries, seal, file size = 24 bytes.
- `TestTimeIndex_Rebuild` — write 3 entries via Rebuild; Lookup works correctly.

## Dependencies

T-012 (package stub).
T-017 (same mmap and fallocate logic; may share helpers).

## Notes

The ceiling semantics (vs floor in the offset index) are intentional: `ReadByTime` must return the batch that *contains* records with `timestamp >= timestampMs`, so we want the first eligible batch, not the last one before the threshold. Shared helpers for mmap, fallocate, and pre-allocation can be extracted from T-017 into an internal helper file if the implementer prefers to avoid duplication; this is up to the implementer's judgement.
