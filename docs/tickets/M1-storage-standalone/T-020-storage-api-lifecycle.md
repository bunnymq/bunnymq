# T-020: Storage public API and lifecycle

**Milestone:** M1 — Storage standalone
**Effort:** M
**Status:** TODO

## Goal

Implement the top-level `Storage` struct in `internal/storage` that implements the full `Storage` interface from `02-storage.md §2`, manages the ordered segment list, and enforces the concurrency model with `segMu`, `chanMu`, and `retMu`.

## Context

`Storage` is the component that the Partition FSM calls directly. It owns `nextOffset`, coordinates segment roll when the active segment exceeds `segment_max_bytes`, broadcasts the `newDataCh` channel on each append, and provides the `Sync` and `TruncateTo` operations used by the Partition FSM during crash recovery. The implementation integrates the pieces built in T-015 through T-019 into a complete, interface-conforming component.

References:
- [02-storage.md §2 — Public interface](../../design/02-storage.md#2-public-interface)
- [02-storage.md §5.3 — Roll criteria](../../design/02-storage.md#53-roll-criteria)
- [02-storage.md §8 — Concurrency model](../../design/02-storage.md#8-concurrency-model)

## Scope

- Implement `Storage` struct in `internal/storage/storage.go`:
  - Fields: `segments []*SegmentStorage`, `nextOffset int64`, `segMu sync.RWMutex`, `newDataCh chan struct{}`, `chanMu sync.Mutex`, `retentionMs atomic.Int64`, `retentionBytes atomic.Int64`, `config *StorageConfig`, `retCancel context.CancelFunc`.
- Implement `Open(dir string, config *StorageConfig) (*storageImpl, error)`:
  - Calls crash recovery logic (T-022) to enumerate and open segments.
  - Starts the background retention goroutine (T-021).
  - Returns a fully ready `Storage` instance.
- Implement `(*storageImpl).Append(batch []byte) (int64, error)`:
  - Acquires `segMu.Lock()`.
  - Overwrites `batch[0:8]` with `nextOffset` (big-endian int64).
  - Delegates to `active().Append(batch)`.
  - Updates `nextOffset += recordCount`.
  - Checks roll criterion; if `active().LogSize() >= config.SegmentMaxBytes`, calls `roll()`.
  - Releases `segMu`.
  - Closes and replaces `newDataCh` under `chanMu`.
- Implement `(*storageImpl).Read(offset int64, maxBytes int) ([]byte, int64, error)`:
  - Acquires `segMu.RLock()`, snapshots the segment slice, releases lock.
  - Binary-searches segments by `BaseOffset()` for the segment that may contain `offset`.
  - Delegates to `SegmentStorage.Read`; returns `ErrOffsetOutOfRange` if `offset < EarliestOffset()`.
- Implement `(*storageImpl).ReadByTime(timestampMs int64, maxBytes int) ([]byte, int64, error)`:
  - Walks segments oldest-first, calling `SegmentStorage.ReadByTime` on each until data is found.
  - Returns `ErrTimestampNotFound` if no segment has data at that timestamp.
- Implement `(*storageImpl).EarliestOffset() int64`: returns `segments[0].BaseOffset()` under RLock.
- Implement `(*storageImpl).LatestOffset() int64`: returns `nextOffset` under RLock.
- Implement `(*storageImpl).NewDataCh() <-chan struct{}`: returns current channel under `chanMu`.
- Implement `(*storageImpl).SetRetentionConfig(retentionMs, retentionBytes int64)`: updates atomics.
- Implement `(*storageImpl).Sync() error`: calls `active().log.Sync()` under RLock.
- Implement `(*storageImpl).TruncateTo(offset int64) error`:
  - Acquires `segMu.Lock()`.
  - Finds the segment containing `offset`; truncates segments newer than that segment, truncates the target segment's log at the byte position corresponding to `offset`.
  - Rebuilds the active segment's indexes from scratch.
  - Resets `nextOffset = offset`.
- Implement `(*storageImpl).Close() error`:
  - Cancels retention goroutine.
  - Seals or closes each segment.
  - Returns first error.
- Implement private `roll() error`:
  - Seals the active segment.
  - Creates a new `SegmentStorage` with `baseOffset = nextOffset`.
  - Appends to `segments`.

## Out of scope

- Background retention goroutine — T-021.
- Crash recovery logic (called from `Open`) — T-022.
- Partition FSM integration — M2 tickets.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes.
- [ ] `Append` + `Read` round-trip across multiple segments.
- [ ] Segment roll occurs at the correct `segment_max_bytes` threshold.
- [ ] `NewDataCh` channel is replaced after each `Append`; old channel is closed.
- [ ] `TruncateTo` reduces `LatestOffset()` to the given value.
- [ ] `ErrOffsetOutOfRange` returned for offsets below `EarliestOffset()`.
- [ ] No data race under `-race` flag for concurrent Append + Read.

## Tests required

- `TestStorage_AppendRead` — write 5 batches; read back all 5 starting at offset 0.
- `TestStorage_SegmentRoll` — configure tiny `segment_max_bytes`; verify a new segment is created after threshold.
- `TestStorage_NewDataCh_Closes` — snapshot channel before Append, verify it is closed after Append.
- `TestStorage_NewDataCh_Replace` — second snapshot after Append returns a new open channel.
- `TestStorage_EarliestLatestOffset` — correct before and after segment roll.
- `TestStorage_TruncateTo` — append 3 batches; TruncateTo at offset of batch 2; LatestOffset = offset of batch 2; read returns only batches 0 and 1.
- `TestStorage_OffsetOutOfRange` — read at offset < EarliestOffset returns ErrOffsetOutOfRange.
- `TestStorage_ConcurrentReadAppend` — goroutine appending while goroutine reading; no race (run with `-race`).
- `TestStorage_Close` — close on open storage returns no error; subsequent Append returns error.

## Dependencies

T-015 (EncodeBatch for test fixtures).
T-019 (SegmentStorage).
T-021 (retention goroutine started from Open; T-020 and T-021 can be implemented together).
T-022 (crash recovery called from Open).

## Notes

The `newDataCh` replacement on Append: under `chanMu`, close the old channel and create a new one. Readers who snapshot the old channel via `NewDataCh()` will be woken by the close; they then call `NewDataCh()` again to get the new channel for the next wait. The `TruncateTo` method is only called by the Partition FSM during `Open()` to undo unapplied writes — it is never called during normal operation, so it does not need to be lock-free or fast.
