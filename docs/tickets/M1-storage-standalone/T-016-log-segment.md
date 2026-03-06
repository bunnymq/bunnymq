# T-016: LogSegment — append and read

**Milestone:** M1 — Storage standalone
**Effort:** M
**Status:** TODO

## Goal

Implement `LogSegment` in `internal/storage` that wraps an `*os.File` and provides append, sequential read, and position-bounded scan operations over a single `.log` file, handling both active (writable) and sealed (read-only) states.

## Context

`LogSegment` is the lowest-level file wrapper in the storage hierarchy. It has no index awareness: it appends raw batch bytes, returns raw batch bytes on reads, and tracks `logSize` to tell `SegmentStorage` when to trigger a roll. A sealed segment is re-opened read-only after the roll procedure; this state transition is exercised by T-019. Crash-recovery truncation is exercised by T-022.

References:
- [02-storage.md §1 — Component hierarchy](../../design/02-storage.md#1-component-hierarchy)
- [02-storage.md §5.1-5.2 — Segment creation and growth](../../design/02-storage.md#51-segment-creation)

## Scope

- Define `LogSegment` struct in `internal/storage/log_segment.go`:
  - Fields: `file *os.File`, `logSize int64`, `baseOffset int64`, `readonly bool`.
- Implement `OpenLogSegment(path string, baseOffset int64, create bool) (*LogSegment, error)`:
  - `create=true`: `O_WRONLY|O_APPEND|O_CREATE`, initial `logSize` from `os.Stat`.
  - `create=false`: `O_RDONLY`, `logSize` from `os.Stat`.
- Implement `(*LogSegment).Append(batch []byte) (int64, error)`:
  - Writes all bytes atomically (single `Write` call using `O_APPEND` semantics).
  - Increments `logSize` by `len(batch)`.
  - Returns starting byte position within the file (i.e., `logSize` before append).
  - Must be called only on an active (non-readonly) segment; returns `ErrSegmentReadOnly` otherwise.
- Implement `(*LogSegment).ReadAt(pos int64, length int) ([]byte, error)`:
  - Reads exactly `length` bytes starting at `pos` using `pread` (via `file.ReadAt`).
  - Returns `io.ErrUnexpectedEOF` if fewer bytes available.
- Implement `(*LogSegment).ScanFrom(startPos int64, callback func(batchBytes []byte, pos int64) bool) error`:
  - Reads batches sequentially from `startPos` to end of valid data.
  - Extracts `batch_length` from bytes [8,12] to advance `pos` correctly.
  - Calls `callback` with each raw batch slice and its start position.
  - Returns early if `callback` returns false.
- Implement `(*LogSegment).Truncate(pos int64) error`: calls `file.Truncate(pos)`, updates `logSize = pos`.
- Implement `(*LogSegment).Sync() error`: calls `file.Sync()` (fsync).
- Implement `(*LogSegment).Close() error`.

## Out of scope

- Index updates — T-017, T-018.
- Segment roll coordination — T-019.
- Crash-recovery CRC scan — T-022.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] `Append` returns correct start position matching file byte count.
- [ ] `ReadAt` on a written batch returns exact bytes written.
- [ ] `ScanFrom` iterates all batches in correct order.
- [ ] `Truncate` leaves file at the given size; `logSize` updated correctly.
- [ ] `ErrSegmentReadOnly` returned on write to sealed segment.

## Tests required

- `TestLogSegment_AppendRead` — write two batches; `ReadAt` at each position returns the exact bytes appended.
- `TestLogSegment_AppendReturnsPosition` — first append returns 0; second returns `len(batch1)`.
- `TestLogSegment_ScanFrom` — three batches appended; `ScanFrom(0)` visits all three in order.
- `TestLogSegment_ScanFrom_PartialStart` — `ScanFrom` from the start of the second batch sees only batches 2 and 3.
- `TestLogSegment_Truncate` — append two batches, truncate to `len(batch1)`, scan yields only batch 1.
- `TestLogSegment_ReadOnlyRejectsAppend` — open read-only, call Append, expect `ErrSegmentReadOnly`.
- `TestLogSegment_Sync` — call Sync on active segment; no error.

## Dependencies

T-012 (package stub exists).
T-015 (batch byte format; tests use raw encoded batches from `EncodeBatch`).

## Notes

Use `file.ReadAt` (pread) for reads — never `file.Seek`+`file.Read`, which is not safe when Append is concurrent. `O_APPEND` ensures kernel-level atomicity for writes up to `PIPE_BUF` or a single write syscall; since batches are at most 4 MiB and we use a single `Write` call, this is safe. Do not call `fsync` inside `Append` — that is the role of `Storage.Sync()` called by the Partition FSM before writing the sidecar.
