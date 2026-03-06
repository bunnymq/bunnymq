# T-019: SegmentStorage — roll and seal

**Milestone:** M1 — Storage standalone
**Effort:** M
**Status:** TODO

## Goal

Implement `SegmentStorage` in `internal/storage` that composes `LogSegment`, `OffsetIndexSegment`, and `TimeIndexSegment` into a single unit, handling batch append with index sampling, batched reads up to `maxBytes`, and the 6-step seal procedure.

## Context

`SegmentStorage` is the composing layer between the three per-file types (T-016, T-017, T-018) and the top-level `Storage`. It owns the sampling decision (whether to write an index entry on this append) and the seal procedure (ftruncate → msync → remap indexes → fsync log). The roll decision (when to seal and start a new segment) is made by `Storage.Append` and delegated to `SegmentStorage.Seal()`.

References:
- [02-storage.md §1 — SegmentStorage](../../design/02-storage.md#1-component-hierarchy)
- [02-storage.md §4 — Segment seal procedure](../../design/02-storage.md#4-mmap-mechanics)
- [02-storage.md §5.2-5.3 — Segment growth and roll criteria](../../design/02-storage.md#52-segment-growth)

## Scope

- Define `SegmentStorage` struct in `internal/storage/segment_storage.go`:
  - Fields: `log *LogSegment`, `offsetIdx *OffsetIndexSegment`, `timeIdx *TimeIndexSegment`, `baseOffset int64`, `sealed bool`, `bytesSinceLastIndex int64`, `config *StorageConfig`.
- Implement `NewSegmentStorage(dir string, baseOffset int64, config *StorageConfig) (*SegmentStorage, error)`:
  - Creates new `.log`, `.index`, `.timeindex` files for the given base offset.
- Implement `OpenSegmentStorage(dir string, baseOffset int64, config *StorageConfig, readonly bool) (*SegmentStorage, error)`:
  - Opens existing files; `readonly=true` for sealed segments.
- Implement `(*SegmentStorage).Append(batch []byte) (startPos int64, err error)`:
  - Calls `log.Append(batch)`, returns the start position.
  - Extracts `base_offset`, `max_timestamp` from the batch header (bytes [0,8] and [30,38]).
  - Conditionally writes index entries: if `bytesSinceLastIndex >= config.IndexSampleBytes`:
    - `offsetIdx.Append(relativeOffset, int32(startPos))`
    - `timeIdx.Append(maxTimestamp, relativeOffset)`
    - Resets `bytesSinceLastIndex = 0`.
  - Otherwise increments `bytesSinceLastIndex += len(batch)`.
- Implement `(*SegmentStorage).Read(offset int64, maxBytes int) ([]byte, int64, error)`:
  - Looks up `relativeOffset = int32(offset - baseOffset)` in `offsetIdx` to find start position.
  - If not found (empty index or offset before first entry), starts scan from position 0.
  - Scans the `.log` from the returned position, collecting complete batches whose `[base_offset, base_offset+record_count)` range covers `offset`, then continuing until `maxBytes` reached.
  - Returns concatenated batch bytes, next offset, nil.
  - Returns `(nil, offset, nil)` if offset is past `logSize`.
- Implement `(*SegmentStorage).ReadByTime(timestampMs int64, maxBytes int) ([]byte, int64, error)`:
  - Looks up `timestampMs` in `timeIdx` (ceiling) to get a `relativeOffset`.
  - Converts to absolute offset and falls back to `Read(absoluteOffset, maxBytes)`.
- Implement `(*SegmentStorage).Seal() error`:
  - 6-step procedure: ftruncate `.index` → msync `.index` → remap `.index` read-only → ftruncate `.timeindex` → msync `.timeindex` → remap `.timeindex` read-only → fsync `.log`.
  - Sets `sealed = true`.
- Implement `(*SegmentStorage).LogSize() int64`: returns `log.logSize`.
- Implement `(*SegmentStorage).BaseOffset() int64`.
- Implement `(*SegmentStorage).Close() error`.

## Out of scope

- Top-level `Storage` segment management — T-020.
- Crash-recovery scan — T-022.
- Retention-driven deletion — T-021.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] `Append` + `Read` round-trip: batch written at offset X can be read back starting at offset X.
- [ ] Index sampling: index entry written on the Nth batch crossing the `index_sample_bytes` threshold; not on every batch.
- [ ] `Seal` produces a file with the correct truncated size; `log.logSize` is unchanged after seal.
- [ ] `ReadByTime` returns batches starting at the first batch with `max_timestamp >= timestampMs`.
- [ ] `Read` returns `(nil, offset, nil)` when offset is past logSize.

## Tests required

- `TestSegmentStorage_AppendRead` — write 3 batches; read from base offset returns all 3.
- `TestSegmentStorage_ReadFromMiddle` — read starting at offset of batch 2 returns only batches 2 and 3.
- `TestSegmentStorage_MaxBytesLimit` — read with `maxBytes = len(batch1)` returns only batch 1.
- `TestSegmentStorage_IndexSampling` — 10 batches appended; index only contains entries at the sampling threshold boundary.
- `TestSegmentStorage_Seal` — seal; index files truncated to actual size; log size unchanged.
- `TestSegmentStorage_ReadByTime` — batches with known timestamps; `ReadByTime` returns starting from correct batch.
- `TestSegmentStorage_ReadEmptyReturnsNil` — read at `nextOffset` returns `(nil, offset, nil)`.

## Dependencies

T-016 (LogSegment).
T-017 (OffsetIndexSegment).
T-018 (TimeIndexSegment).
T-015 (batch encoding; tests use `EncodeBatch` to create test fixtures).

## Notes

The 6-step seal order is precisely specified in `02-storage.md §4`: offset index first, then time index, then log. Do not reorder. The `startPos` returned by `LogSegment.Append` (bytes before this write) is used as the `position` field in the offset index. The `Read` scan loop: after finding the floor index entry, scan the log from that position forward, skipping batches whose range doesn't yet reach `offset`, collecting batches until `maxBytes` is exceeded or end of log is reached.
