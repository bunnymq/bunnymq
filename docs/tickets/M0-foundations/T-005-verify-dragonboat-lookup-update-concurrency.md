# T-005: VERIFY dragonboat v4 — IOnDiskStateMachine Lookup/Update concurrency and channel return

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** DONE

## Goal

Confirm two concurrency properties of dragonboat v4's `IOnDiskStateMachine`: (a) `Lookup()` may be called concurrently with `Update()` on the same FSM instance, and (b) `Lookup()` may safely return a Go channel (`<-chan struct{}`) as `interface{}`.

## Context

`05-data-coordinator.md §6.2` and OQ2 require that the Partition FSM's `Lookup()` returns `storage.NewDataCh()` (a `<-chan struct{}`) for the long-poll fetch path. This requires:
1. dragonboat allows `Lookup()` and `Update()` to run concurrently on the same `IOnDiskStateMachine` instance.
2. dragonboat does not restrict the Go type that `Lookup()` returns via `interface{}`.

`05-data-coordinator.md §9` marks point (1) as "VERIFY pending dragonboat v4 API documentation review." Point (2) is a Go type-system question but must be confirmed against any dragonboat-specific restrictions. The new `QueryGetNewDataCh` PartitionQueryType is gated on this verification.

References:
- [05-data-coordinator.md §6.2](../../design/05-data-coordinator.md#62-long-poll-fetch-maxwaitms--0-no-data-available)
- [05-data-coordinator.md §9](../../design/05-data-coordinator.md#9-concurrency-model)
- [03-raft-fsm.md §4.5](../../design/03-raft-fsm.md#45-lookup)

## Scope

- Read dragonboat v4 `IOnDiskStateMachine` interface documentation for `Lookup()` threading guarantees.
- Confirm: does dragonboat guarantee that `Lookup()` and `Update()` are NEVER called concurrently on the same instance (like `IStateMachine`), or ALWAYS potentially concurrent (requiring the FSM to handle it)?
- Confirm: is there any restriction on what Go type `Lookup()` may return? (Channels, pointers, structs all permitted?)
- If `Lookup()` and `Update()` are NOT concurrent: document the impact on Storage's concurrency model (the design in `02-storage.md §8` assumes concurrent access).
- If channel return is NOT safe: propose an alternative (e.g., return a struct wrapping the channel).
- Document the confirmed threading model and any caveats.

## Out of scope

- Implementing Lookup/Update methods — M2 PartitionFSM tickets.

## Findings

### 1. Lookup/Update concurrency: **confirmed concurrent** for `IOnDiskStateMachine`

dragonboat v4 source (`statemachine/disk.go`) documents this explicitly in the `IOnDiskStateMachine` type comment:

> "An IOnDiskStateMachine type allows its Update method to be concurrently invoked when there are ongoing calls to the Lookup or the SaveSnapshot method."

The `Update()` method comment adds:

> "Concurrent calls to the Lookup method and the SaveSnapshot method are not blocked when the state machine is being updated by the Update method."

The `Lookup()` method comment confirms the symmetric direction:

> "Concurrent calls to the Update and RecoverFromSnapshot method are not blocked when calls to the Lookup method are being processed."

**Internal mechanism** (`internal/rsm/adapter.go`): `OnDiskStateMachine.Concurrent()` returns `true`. In `internal/rsm/statemachine.go`, when `s.Concurrent()` is true the `Lookup` path calls `concurrentLookup` → `s.sm.ConcurrentLookup(query)` **without holding the internal `sync.RWMutex`**. For regular `IStateMachine`, the write lock guards `Update` and the read lock guards `Lookup` — so multiple Lookups are concurrent with each other but never with Update. `IOnDiskStateMachine` has no such mutex guard on its Lookup/Update pair.

**Mutual exclusion that does exist**: `Update`, `Sync`, `PrepareSnapshot`, `RecoverFromSnapshot`, and `Close` are mutually exclusive with each other (guarded by an internal lock), but **Lookup** is excluded from this set and runs freely alongside `Update`.

### 2. Channel return from Lookup: **safe, no restrictions**

`Lookup(interface{}) (interface{}, error)` — Go's `interface{}` accepts any type. The dragonboat internal adapter (`OnDiskStateMachine.Lookup` in `internal/rsm/adapter.go`) passes the result directly through:

```go
func (s *OnDiskStateMachine) Lookup(query interface{}) (interface{}, error) {
    s.ensureOpened()
    return s.sm.Lookup(query)
}
```

No type assertion, no serialization, no restriction is applied to the returned value. Returning a `<-chan struct{}` via `interface{}` is fully safe from dragonboat's perspective.

### 3. Impact on Storage concurrency model

The design assumption in `02-storage.md §8` (concurrent Lookup/Update access) is **correct**. Storage's existing `segMu` + `chanMu` locks are sufficient:

- `Lookup` for `QueryGetNewDataCh` acquires `chanMu` to copy the current `newDataCh` reference, then returns it. Releasing `chanMu` before return is fine because the channel value itself is immutable after capture.
- `Update` (via `Append`) acquires `segMu` then `chanMu`, appends data, creates a new channel, closes the old one, and releases both locks. No ordering conflict with Lookup.
- Because Lookup and Update run concurrently, the FSM must NOT assume it holds any exclusive lock during Lookup — which the Storage design correctly handles.

### 4. `QueryGetNewDataCh` PartitionQueryType: **safe to add**

The long-poll fetch pattern documented in `05-data-coordinator.md §6.2` is confirmed as safe:

1. DataCoordinator calls `nodeHost.ReadIndex` + `nodeHost.ReadLocalNode` (which invokes `Lookup`).
2. `Lookup(QueryGetNewDataCh{})` returns `storage.NewDataCh()` — a snapshot of the current `<-chan struct{}`.
3. DataCoordinator waits on the returned channel (or the `maxWaitMs` timer, whichever fires first).
4. Concurrently, another goroutine calls `Update` which runs `Append`, closes the old channel, and creates a new one. The closed channel wakes the waiting DataCoordinator.

No race condition exists because the channel value is captured atomically under `chanMu`, and closing a channel is safe to call once by exactly one goroutine (the `Append` path holds `chanMu` when it closes the old channel and replaces it).

## Definition of done

- [x] Lookup/Update concurrency behavior confirmed (concurrent or serialized) with dragonboat v4 source reference.
- [x] Channel-via-interface{} return safety confirmed or alternative approach documented.
- [x] If the design assumption (concurrent Lookup/Update) is wrong: impact on Storage concurrency model assessed and documented.
- [x] `QueryGetNewDataCh` PartitionQueryType confirmed as safe to add (pending this result).

## Tests required

N/A — research ticket. A minimal `_test.go` that instantiates a stub FSM and calls Lookup/Update from two goroutines may be written as part of investigation but is not required.

## Dependencies

None.

## Notes

For `IStateMachine` (used by MetadataFSM), dragonboat guarantees `Lookup()` and `Update()` are never concurrent. For `IOnDiskStateMachine`, the behavior may differ. Check dragonboat v4 source in `statemachine/` package. The key concern: if `Update()` and `Lookup()` can run concurrently, Storage's `segMu` + `chanMu` locks already handle it correctly. If they cannot run concurrently, the `newDataCh` pattern still works because `Lookup()` returns the channel snapshot under `chanMu`, and the channel itself is closed asynchronously by `Append()` after the lock is released.
