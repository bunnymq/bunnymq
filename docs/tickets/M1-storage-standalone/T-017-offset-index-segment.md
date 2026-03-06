# T-017: OffsetIndexSegment with mmap

**Milestone:** M1 — Storage standalone
**Effort:** M
**Status:** TODO

## Goal

Implement `OffsetIndexSegment` in `internal/storage` that wraps a mmap'd `.index` file with 8-byte entries (relative_offset int32 + position int32), supporting append in active state, binary-search lookup by offset, and seal-to-read-only transition.

## Context

The offset index provides O(log n) positioning into the `.log` file for a given consumer offset. It is sparse: one entry is written per `index_sample_bytes` (default 4 KiB) of log data. Binary search returns the floor entry, giving a conservative start position; the caller then scans forward in the `.log` to the exact batch. The mmap approach (write in active, read-only after seal) is chosen for low-syscall index entry writes on the hot append path.

References:
- [02-storage.md §1 — OffsetIndexSegment](../../design/02-storage.md#1-component-hierarchy)
- [02-storage.md §3.3 — Index entry layouts](../../design/02-storage.md#33-index-entry-layouts)
- [02-storage.md §4 — mmap mechanics](../../design/02-storage.md#4-mmap-mechanics)

## Scope

- Define `OffsetIndexSegment` struct in `internal/storage/offset_index.go`:
  - Fields: `file *os.File`, `data []byte` (mmap region), `entryCount atomic.Int64`, `baseOffset int64`, `maxEntries int64`.
- Implement `OpenOffsetIndex(path string, baseOffset int64, segMaxBytes int64, indexSampleBytes int) (*OffsetIndexSegment, error)`:
  - Creates or opens the `.index` file.
  - Pre-allocates via `fallocate` (Linux) or write-zeroes fallback (other OS); logs `warn` if fallback is used.
  - Allocation size: `ceil(segMaxBytes / indexSampleBytes) × 8` rounded up to OS page size.
  - Maps `PROT_READ|PROT_WRITE|MAP_SHARED`.
  - Initializes `entryCount` from file size / 8 (non-zero if reopening after crash).
- Implement `(*OffsetIndexSegment).Append(relativeOffset int32, position int32)`:
  - Writes 8 bytes at slot `entryCount` in the mmap region (big-endian int32 + int32).
  - Increments `entryCount` atomically after write.
  - Returns `ErrIndexFull` if `entryCount >= maxEntries`.
- Implement `(*OffsetIndexSegment).Lookup(relativeOffset int32) (position int32, found bool)`:
  - Binary search over `[0, entryCount)` entries for the largest entry ≤ `relativeOffset`.
  - Returns `(position, true)` for the floor match, or `(0, false)` if no entries exist.
  - Reads `entryCount` atomically before searching.
- Implement `(*OffsetIndexSegment).Seal() error`:
  - `ftruncate` to `entryCount × 8`.
  - `msync` (MS_SYNC).
  - `munmap`.
  - Re-mmap `PROT_READ|MAP_SHARED`.
- Implement `(*OffsetIndexSegment).Rebuild(entries []offsetEntry)`:
  - Resets `entryCount` to 0 and writes all provided entries; used by crash recovery (T-022).
- Implement `(*OffsetIndexSegment).Close() error`.

## Out of scope

- Time-based index — T-018.
- Integration with SegmentStorage roll — T-019.
- Index rebuild during crash recovery — T-022.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] `Lookup` returns floor match for exact and in-between offsets.
- [ ] `Append` + `Lookup` round-trip is correct for 1, 2, and N entries.
- [ ] `Seal` shrinks file to `entryCount × 8`; subsequent writes return `ErrIndexFull`.
- [ ] File pre-allocated to max capacity at creation.
- [ ] `entryCount` atomic ordering: entry bytes visible before counter increment (verified by concurrent test).

## Tests required

- `TestOffsetIndex_AppendLookup` — append 3 entries; lookup at exact offsets returns correct positions.
- `TestOffsetIndex_FloorLookup` — lookup at offset between two entries returns the lower entry's position.
- `TestOffsetIndex_EmptyLookup` — lookup on empty index returns `found=false`.
- `TestOffsetIndex_Seal` — append 2 entries, seal, file size = 16 bytes; further Append returns `ErrIndexFull`.
- `TestOffsetIndex_PreAllocSize` — newly created index file size equals computed allocation size.
- `TestOffsetIndex_Rebuild` — write 3 entries via `Rebuild`; `Lookup` returns correct positions.
- `TestOffsetIndex_ConcurrentAppendLookup` — goroutine appending entries while another reads via Lookup; no data race (run with `-race`).

## Dependencies

T-012 (package stub).

## Notes

On Linux, use `golang.org/x/sys/unix.Fallocate` with `FALLOC_FL_KEEP_SIZE`. On Darwin / other platforms, fall back to writing a zero-filled slice. Big-endian encoding: `binary.BigEndian.PutUint32`. The atomic ordering guarantee: write the 8 bytes into `data[slot*8 : slot*8+8]` first, then call `entryCount.Add(1)`. On Go's memory model this is sufficient because readers load `entryCount` before indexing into `data`, and x86/arm64 store ordering ensures the data write is visible before the counter write from the reader's perspective once the atomic load returns the incremented value.
