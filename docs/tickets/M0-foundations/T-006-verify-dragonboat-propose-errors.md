# T-006: VERIFY dragonboat v4 — Propose and SyncPropose error types

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** DONE

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

---

## Findings

_Verified against `github.com/lni/dragonboat/v4@v4.0.0-20250723143628-076c7f6497dc`._

_Source files examined: `nodehost.go`, `request.go`, `node.go`._

---

### `NodeHost.Propose` (async) — errors returned before channel

`Propose(session, cmd, timeout)` returns `(*RequestState, error)`. The caller then waits on `rs.AppliedC()` for the async result. Two distinct error surfaces:

**Phase 1 — synchronous errors (returned from `Propose` itself):**

| Error | Source | Cause |
|---|---|---|
| `ErrClosed` | `nodehost.go` | `NodeHost.Close()` has been called |
| `ErrShardNotFound` | `nodehost.go` | Shard with `session.ShardID` is not registered on this node |
| `ErrShardNotReady` | `node.go` (`node.propose`) | Shard exists but `!n.initialized()` — not yet bootstrapped |
| `ErrInvalidOperation` | `node.go` | Witness node; never occurs in BunnyMQ (all replicas are voters) |
| `ErrInvalidSession` | `node.go` | Session's `ShardID` does not match the node's shard (indicates a call-site bug — BunnyMQ uses `NoOPSession` so this is a programming error) |
| `ErrPayloadTooBig` | `node.go` / `request.go` | Payload exceeds `rsm.GetMaxBlockSize(EntryCompressionType)` |
| `ErrTimeoutTooSmall` | `request.go` (`proposalShard.propose`) | `timeout == 0` ticks |
| `ErrShardClosed` | `request.go` (`proposalShard.propose`) | Entry queue is stopped (shard is shutting down — `entryQueue.add` returns `stopped=true`) |
| `ErrSystemBusy` | `request.go` (`proposalShard.propose`) | Entry queue is full (pipeline at `MaxInMemLogSize` — `entryQueue.add` returns `added=false`) |

**Phase 2 — async result codes from `rs.AppliedC()` channel:**

`getRequestState` translates `RequestResult` codes to errors:

| `RequestResult` method | Error returned | Meaning |
|---|---|---|
| `Completed()` | `nil` | Entry applied; `GetResult()` holds FSM return value |
| `Rejected()` | `ErrRejected` | Session evicted or invalid; with `NoOPSession` this should not occur |
| `Timeout()` | `ErrTimeout` | Dragonboat's internal tick-based timeout fired before commit |
| `Terminated()` | `ErrShardClosed` | Shard shut down while proposal was pending |
| `Dropped()` | `ErrShardNotReady` | Entry dropped because no leader is known yet |
| `Aborted()` | `ErrAborted` | User-defined abort from FSM; should not occur in BunnyMQ's deterministic FSMs |

**Context cancellation (fires instead of channel):**

| `ctx.Err()` | Error returned |
|---|---|
| `context.Canceled` | `ErrCanceled` |
| `context.DeadlineExceeded` | `ErrTimeout` |

> **Key finding:** `context.DeadlineExceeded` and dragonboat's internal tick-based timeout both surface as the same `ErrTimeout` sentinel. There is **no way to distinguish them** after `getRequestState` returns — both are `errors.Is(err, ErrTimeout)`.

---

### `NodeHost.SyncPropose` (sync) — error surfaces

`SyncPropose` is implemented as:
1. Call `getTimeoutFromContext(ctx)` — can return `ErrDeadlineNotSet` or `ErrInvalidDeadline`.
2. Call `Propose(session, cmd, timeout)` — same Phase 1 errors above.
3. Call `getRequestState(ctx, rs)` — same Phase 2 + context errors above.

Additional errors unique to `SyncPropose`:

| Error | Cause |
|---|---|
| `ErrDeadlineNotSet` | Context has no deadline — always set a timeout before calling `SyncPropose` |
| `ErrInvalidDeadline` | Deadline has already passed when `SyncPropose` is called |

---

### `IsTempError` helper

dragonboat v4 exports `IsTempError(err error) bool` (`request.go`):

```go
func IsTempError(err error) bool {
    return errors.Is(err, ErrSystemBusy) ||
        errors.Is(err, ErrShardClosed) ||
        errors.Is(err, ErrShardNotInitialized) ||
        errors.Is(err, ErrShardNotReady) ||
        errors.Is(err, ErrTimeout) ||
        errors.Is(err, ErrClosed) ||
        errors.Is(err, ErrAborted)
}
```

This is dragonboat's own classification of retriable errors. BunnyMQ's `mapRaftError` should align with it except where the design doc overrides (e.g., `ErrTimeout` → `TIMEOUT`/`DEADLINE_EXCEEDED` rather than `UNAVAILABLE`).

---

### `mapRaftError` mapping table

For the M3 `mapRaftError(err error) BunnyErrorCode` helper (`internal/raft` or `internal/coordinator/data`):

| Error | `BunnyErrorCode` | gRPC Status | Retriable | Notes |
|---|---|---|---|---|
| `ErrClosed` | `UNAVAILABLE` | `UNAVAILABLE` | yes | NodeHost shutting down |
| `ErrShardNotFound` | `UNAVAILABLE` | `UNAVAILABLE` | yes | Shard not yet registered |
| `ErrShardClosed` | `UNAVAILABLE` | `UNAVAILABLE` | yes | Shard stopping/restarting |
| `ErrShardNotReady` | `UNAVAILABLE` | `UNAVAILABLE` | yes | No leader or shard not bootstrapped |
| `ErrSystemBusy` | `UNAVAILABLE` | `UNAVAILABLE` | yes | Pipeline full (back-off and retry) |
| `ErrShardNotInitialized` | `UNAVAILABLE` | `UNAVAILABLE` | yes | Shard initializing |
| `ErrTimeout` | `TIMEOUT` | `DEADLINE_EXCEEDED` | yes | Safe to retry; proposal was never applied |
| `ErrCanceled` | — | — | — | Client canceled context; propagate `ctx.Err()` directly to gRPC layer |
| `ErrDeadlineNotSet` | `UNKNOWN` | `INTERNAL` | no | Programming error: context without deadline passed to SyncPropose |
| `ErrInvalidDeadline` | `UNKNOWN` | `INTERNAL` | no | Programming error: expired deadline passed to SyncPropose |
| `ErrPayloadTooBig` | `BATCH_TOO_LARGE` | `INVALID_ARGUMENT` | no | Should be caught by the API layer before Propose is called |
| `ErrRejected` | `UNKNOWN` | `INTERNAL` | no | Should not occur with `NoOPSession` |
| `ErrAborted` | `UNKNOWN` | `INTERNAL` | no | Unexpected FSM abort |
| `ErrTimeoutTooSmall` | `UNKNOWN` | `INTERNAL` | no | Programming error |
| `ErrInvalidOperation` | `UNKNOWN` | `INTERNAL` | no | Witness node; impossible in BunnyMQ |
| `ErrInvalidSession` | `UNKNOWN` | `INTERNAL` | no | Call-site bug |
| any other `error` | `UNKNOWN` | `INTERNAL` | no | Unknown dragonboat error |

**Implementation note for `mapRaftError`:**

```go
func mapRaftError(err error) error {
    switch {
    case err == nil:
        return nil
    case errors.Is(err, dragonboat.ErrCanceled):
        // caller canceled; let the gRPC framework handle ctx.Err()
        return status.FromContextError(ctx.Err()).Err()
    case errors.Is(err, dragonboat.ErrTimeout):
        return status.Error(codes.DeadlineExceeded, err.Error())
    case errors.Is(err, dragonboat.ErrSystemBusy),
        errors.Is(err, dragonboat.ErrShardNotFound),
        errors.Is(err, dragonboat.ErrShardClosed),
        errors.Is(err, dragonboat.ErrShardNotReady),
        errors.Is(err, dragonboat.ErrShardNotInitialized),
        errors.Is(err, dragonboat.ErrClosed):
        return status.Error(codes.Unavailable, err.Error())
    case errors.Is(err, dragonboat.ErrPayloadTooBig):
        return status.Error(codes.InvalidArgument, err.Error())
    default:
        return status.Error(codes.Internal, err.Error())
    }
}
```

> **Note on `ErrTimeout` retry safety:** When `SyncPropose` returns `ErrTimeout`, the Raft entry was **not** committed (either it never reached a quorum or it timed out before the local node applied it). Retrying is safe — there is no risk of duplicate application. When async `Propose` returns `ErrTimeout` (from the channel), the same guarantee holds.

---

### Definition of done checklist

- [x] Error types from `NodeHost.Propose` documented.
- [x] Error types from `NodeHost.SyncPropose` documented.
- [x] Retriable vs non-retriable classification documented for each error type.
- [x] `mapRaftError(err) → BunnyErrorCode` mapping table documented.
