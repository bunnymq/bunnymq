# T-005: VERIFY dragonboat v4 — IOnDiskStateMachine Lookup/Update concurrency and channel return

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

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

## Definition of done

- [ ] Lookup/Update concurrency behavior confirmed (concurrent or serialized) with dragonboat v4 source reference.
- [ ] Channel-via-interface{} return safety confirmed or alternative approach documented.
- [ ] If the design assumption (concurrent Lookup/Update) is wrong: impact on Storage concurrency model assessed and documented.
- [ ] `QueryGetNewDataCh` PartitionQueryType confirmed as safe to add (pending this result).

## Tests required

N/A — research ticket. A minimal `_test.go` that instantiates a stub FSM and calls Lookup/Update from two goroutines may be written as part of investigation but is not required.

## Dependencies

None.

## Notes

For `IStateMachine` (used by MetadataFSM), dragonboat guarantees `Lookup()` and `Update()` are never concurrent. For `IOnDiskStateMachine`, the behavior may differ. Check dragonboat v4 source in `statemachine/` package. The key concern: if `Update()` and `Lookup()` can run concurrently, Storage's `segMu` + `chanMu` locks already handle it correctly. If they cannot run concurrently, the `newDataCh` pattern still works because `Lookup()` returns the channel snapshot under `chanMu`, and the channel itself is closed asynchronously by `Append()` after the lock is released.
