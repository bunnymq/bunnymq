# T-042: DataCoordinator — Fetch with long-poll

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement `DataCoordinator.Fetch` including the `fetchWithLongPoll` loop that parks the goroutine on `newDataCh` until records arrive, the deadline fires, or the client disconnects.

## Context

The long-poll fetch is the key latency optimization for consumers: instead of the client sending repeated empty-result requests, the server blocks until a new batch is committed. The design requires taking the `newDataCh` snapshot **before** the read, then parking on it — this eliminates the race between a new batch arriving between the read and the channel snapshot.

References:
- [05-data-coordinator.md §6 — Fetch flow](../../design/05-data-coordinator.md#6-fetch-flow)
- [05-data-coordinator.md §6.2 — Long-poll](../../design/05-data-coordinator.md#62-long-poll-fetch-maxwaitms--0-no-data-available)
- [03-raft-fsm.md §4.5 — QueryGetNewDataCh](../../design/03-raft-fsm.md#45-lookup)

## Scope

- Add to `internal/data/coordinator.go`:
  - `Fetch(ctx, topic, partitionID, offset, maxBytes, maxWaitMs) ([]byte, int64, error)`:
    - `leaderCheck` to get `shardID`.
    - If `maxWaitMs == 0`: single `LookupPartition(QueryRead)` → return.
    - If `maxWaitMs > 0`: call `fetchWithLongPoll`.
  - `fetchWithLongPoll(ctx, topic, partitionID, shardID, offset, maxBytes, maxWaitMs)`:
    - Loop: compute `remaining`; if ≤ 0 return `(nil, 0, nil)`.
    - `LookupPartition(QueryGetNewDataCh)` → snapshot `ch`.
    - Re-verify leadership (leader may have changed during a previous wait iteration).
    - `LookupPartition(QueryRead)` → if records non-empty, return.
    - `select { case <-ch: continue; case <-time.After(remaining): return nil; case <-ctx.Done(): return ctx.Err() }`.
  - `QueryGetNewDataCh` is a new `PartitionQueryType` value (added to the enum from T-025); the `PartitionFSM.Lookup` case for it calls `storage.NewDataCh()` and returns the channel as `interface{}`.

## Out of scope

- Produce and offset queries — T-041.
- Adding `QueryGetNewDataCh` to `PartitionFSM.Lookup` — this ticket must add that case to the FSM; it is a small targeted addition to the code created in T-031.

## Definition of done

- [ ] `go build ./internal/data/...` and `go build ./internal/partition/...` pass.
- [ ] `go test ./internal/data/...` passes.
- [ ] `Fetch` with `maxWaitMs=0` and data available: returns data immediately.
- [ ] `Fetch` with `maxWaitMs=0` and no data: returns `(nil, 0, nil)` without blocking.
- [ ] `Fetch` long-poll: goroutine parks on `newDataCh`; wakes when batch appended.
- [ ] `Fetch` long-poll deadline: returns `(nil, 0, nil)` after `maxWaitMs` with no data.
- [ ] `Fetch` long-poll: ctx cancelled mid-wait → returns `ctx.Err()` promptly (no goroutine leak).
- [ ] Long-poll re-checks leadership on each iteration; returns `NotLeaderError` if leadership changes.

## Tests required

- `TestFetch_ImmediateData` — `LookupPartition(QueryRead)` stub returns batch; `maxWaitMs=0`; returns batch.
- `TestFetch_NoDataNoWait` — no data; `maxWaitMs=0`; returns `(nil, 0, nil)`.
- `TestFetch_LongPollWakesOnData` — `LookupPartition(QueryRead)` returns empty first; goroutine blocks on `ch`; close `ch`; next iteration returns data.
- `TestFetch_LongPollTimeout` — no data arrives; deadline fires; returns `(nil, 0, nil)`.
- `TestFetch_LongPollCtxCancel` — cancel ctx while waiting; returns error; no leaked goroutine (verified by checking goroutine count or using `goleak`).
- `TestFetch_LongPollLeaderChange` — after first wait iteration, metadata returns new leader; returns `NotLeaderError`.
- `TestFetch_NewDataCh_RaceElimination` — concurrent: append batch while `LookupPartition(QueryRead)` is in-flight; verify no deadlock or missed wakeup (stress test with `-race`).

## Dependencies

T-041 (DataCoordinator struct + leaderCheck).
T-031 (PartitionFSM Lookup; needs `QueryGetNewDataCh` case added here).
T-025 (PartitionQueryType enum; add `QueryGetNewDataCh` value).
T-020 (Storage.NewDataCh returns `<-chan struct{}`).

## Notes

`QueryGetNewDataCh` was identified in `05-data-coordinator.md §12 Open Question 2` as requiring a new PartitionQueryType value. This ticket resolves it by adding the enum value and the FSM handler in `internal/partition/fsm.go` (the code written in T-031). The addition is a targeted one-line dispatch case in the `switch q.Type` block of `Lookup()`. The race elimination property of the long-poll loop (take `ch` before read) is the key design invariant to preserve; the stress test (`TestFetch_NewDataCh_RaceElimination`) is the regression guard for it.
