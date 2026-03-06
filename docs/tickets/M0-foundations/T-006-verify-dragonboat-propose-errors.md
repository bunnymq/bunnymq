# T-006: VERIFY dragonboat v4 — Propose and SyncPropose error types

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

## Goal

Identify all error types returned by dragonboat v4's `NodeHost.Propose` (async) and `NodeHost.SyncPropose` (sync) on failure so Data Coordinator can map them to correct, client-visible gRPC status codes.

## Context

`05-data-coordinator.md OQ5` asks to verify `ProposePartition` error semantics. `DataCoordinator.sendToPartition` in the produce path must map dragonboat errors to `Unavailable` (retriable) or `Unknown` (non-retriable). The wrong mapping causes either client retry storms (on non-retriable errors) or silent data-loss risk (on non-retriable errors treated as retriable). `SyncProposeMetadata` errors are also relevant for ClusterCoordinator, GroupCoordinator, and other callers.

References:
- [05-data-coordinator.md OQ5](../../design/05-data-coordinator.md#12-open-questions)
- [05-data-coordinator.md §5](../../design/05-data-coordinator.md#5-produce-flow)
- [06-api-protocol.md §7](../../design/06-api-protocol.md#7-error-code-to-grpc-status-mapping)

## Scope

- Identify error types returned by `NodeHost.Propose` (async) in dragonboat v4: pipeline full, NodeHost closing, shard not found, not-leader, etc.
- Identify error types returned by `NodeHost.SyncPropose` (sync): timeout, context cancellation, quorum unavailable, not-leader, etc.
- Classify each error as retriable (`Unavailable`) or non-retriable (`Internal`/`Unknown`), aligning with `06-api-protocol.md §7`.
- Document the mapping table for the `mapRaftError()` helper function that M3 implementation tickets will implement.

## Out of scope

- Implementing `mapRaftError()` — M3 DataCoordinator ticket.
- Mapping errors from Metadata FSM commands — covered by same error types, no additional scope.

## Definition of done

- [ ] Error types from `NodeHost.Propose` documented.
- [ ] Error types from `NodeHost.SyncPropose` documented.
- [ ] Retriable vs non-retriable classification documented for each error type.
- [ ] `mapRaftError(err) → BunnyErrorCode` mapping table documented.

## Tests required

N/A — research ticket.

## Dependencies

None.

## Notes

Look at `github.com/lni/dragonboat/v4` exported error variables (e.g., `ErrClusterNotFound`, `ErrClusterClosed`, `ErrBadKey`, `ErrTimeout`, `ErrSystemBusy`, etc. — names may differ in v4). Check the `RequestState` and `RequestResult` types for async `Propose`. For `SyncPropose`, the `context.DeadlineExceeded` from a context timeout is distinct from dragonboat's own timeout. Both must be handled.
