# T-031: Partition FSM — Lookup and Strategy A snapshots

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** S
**Status:** TODO

## Goal

Implement `PartitionFSM.Lookup()` for the four query types and the three no-op `IOnDiskStateMachine` snapshot methods (`PrepareSnapshot`, `SaveSnapshot`, `RecoverFromSnapshot`) and the `Sync` no-op.

## Context

`Lookup` is the read path for the Data Coordinator's fetch flow — it delegates to Storage without any Raft round-trip. Strategy A snapshots are intentional no-ops for v1: the Raft log is never compacted, and recovery always replays from index 0. The `Sync()` hook is also a no-op because `persistApplied` inside `Update()` already handles durability.

References:
- [03-raft-fsm.md §4.5 — Lookup](../../design/03-raft-fsm.md#45-lookup)
- [03-raft-fsm.md §4.6 — Strategy A snapshots](../../design/03-raft-fsm.md#46-snapshot-strategy--strategy-a-v1)

## Scope

- Implement `(*PartitionFSM).Lookup(query any) (any, error)`:
  - Type-asserts `query` to `PartitionQuery`.
  - Dispatches on `q.Type`:
    - `QueryRead`: returns `storage.Read(q.Offset, q.MaxBytes)` (3-value tuple).
    - `QueryReadByTime`: returns `storage.ReadByTime(q.TimestampMs, q.MaxBytes)`.
    - `QueryEarliestOffset`: returns `storage.EarliestOffset()`.
    - `QueryLatestOffset`: returns `storage.LatestOffset()`.
  - Returns `error` from storage unchanged.
- Define helper `type PartitionLookupResult struct { Batches []byte; NextOffset int64 }` for `QueryRead` and `QueryReadByTime` return (wraps 3-tuple into single any value).
- Implement Strategy A snapshot methods:
  - `PrepareSnapshot() (any, error)`: returns `nil, nil`.
  - `SaveSnapshot(ctx any, w io.Writer, done <-chan struct{}) error`: writes marker `"strategy-a-noop"` to `w`; returns nil.
  - `RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error`: reads and discards; returns nil.
- Implement `(*PartitionFSM).Sync() error`: returns nil.
- Implement `(*PartitionFSM).Close() error`: calls `storage.Close()`.

## Out of scope

- Strategy B snapshots — explicitly not implemented in v1.
- PartitionFSM.Open and Update — T-030.

## Definition of done

- [ ] `go build ./internal/partition/...` passes.
- [ ] `go test ./internal/partition/...` passes.
- [ ] `Lookup(QueryRead)` returns the batch bytes and next offset from Storage.
- [ ] `Lookup(QueryEarliestOffset)` returns `storage.EarliestOffset()`.
- [ ] `SaveSnapshot` writes the no-op marker; `RecoverFromSnapshot` reads and discards it; no state change.
- [ ] `Close` calls `storage.Close()`.

## Tests required

- `TestPartitionFSM_Lookup_Read` — append a batch via Update; Lookup QueryRead at offset 0; returns the batch bytes.
- `TestPartitionFSM_Lookup_EarliestLatest` — after 3 appends; QueryEarliestOffset = 0; QueryLatestOffset = 3.
- `TestPartitionFSM_Lookup_ReadNoData` — QueryRead at offset == LatestOffset; returns (nil, offset, nil).
- `TestPartitionFSM_SnapshotNoOp` — call PrepareSnapshot, SaveSnapshot, RecoverFromSnapshot; no error; state unchanged.
- `TestPartitionFSM_Sync_NoOp` — Sync returns nil.
- `TestPartitionFSM_Close` — Close; subsequent Lookup returns error (storage closed).

## Dependencies

T-030 (PartitionFSM struct, storage field).
T-025 (PartitionQuery, PartitionQueryType).
T-020 (Storage.Read, ReadByTime, EarliestOffset, LatestOffset, Close).

## Notes

`Lookup` on `IOnDiskStateMachine` **may** be called concurrently with `Update` by dragonboat. Storage's concurrency model (§8 of 02-storage.md) handles this: reads use `segMu.RLock()` and are safe concurrent with the single Append goroutine. No additional mutex is needed in `Lookup`. The `PartitionLookupResult` wrapper type is needed because `Lookup` returns `any` — the 3-value tuple `([]byte, int64, error)` cannot be returned directly as a single interface value.
