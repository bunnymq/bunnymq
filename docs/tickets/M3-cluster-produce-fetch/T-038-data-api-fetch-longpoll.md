# T-038: Data API — Fetch with long-poll

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement the `Fetch` RPC of `DataServiceServer`, including the long-poll path that blocks until records arrive (or `max_wait_ms` elapses), by delegating to `DataCoordinator.Fetch`.

## Context

`Fetch` is a unary RPC with server-side long-polling: when no records are available and `max_wait_ms > 0`, the server parks the gRPC goroutine until `newDataCh` fires, the deadline fires, or the client disconnects. The polling logic lives in `DataCoordinator` (T-042); the handler here is thin: validate, call coordinator, map errors to gRPC status.

References:
- [06-api-protocol.md §5.2 — Fetch](../../design/06-api-protocol.md#52-fetch)
- [05-data-coordinator.md §6 — Fetch flow](../../design/05-data-coordinator.md#6-fetch-flow)
- [06-api-protocol.md §7 — Error mapping](../../design/06-api-protocol.md#7-error-code-to-grpc-status-mapping)

## Scope

- Add `Fetch(ctx, *pb.FetchRequest) (*pb.FetchResponse, error)` to `DataServer`:
  - Validate: `req.Offset >= 0`; `req.MaxBytes > 0` (default to 1 MiB if 0).
  - Call `dc.Fetch(ctx, topic, partitionID, offset, maxBytes, maxWaitMs)`.
  - On `NotLeaderError` → `codes.FailedPrecondition` + `NOT_LEADER` + `NotLeaderDetail`.
  - On `ErrOffsetOutOfRange` → `codes.OutOfRange` + `OFFSET_OUT_OF_RANGE`.
  - On `(nil, 0, nil)` (timeout, no records) → return `FetchResponse{Records: nil, NextOffset: req.Offset}`.
  - On success with records → `FetchResponse{Records: records, NextOffset: nextOffset}`.
  - On `ctx.Err()` (client disconnect) → return appropriate gRPC status (cancelled or deadline exceeded).

## Out of scope

- DataCoordinator.Fetch implementation (including the long-poll select loop) — T-042.
- Produce/GetOffsets — T-037.

## Definition of done

- [ ] `go build ./internal/api/data/...` passes.
- [ ] `go test ./internal/api/data/...` passes.
- [ ] `Fetch` with records available: response has correct `Records` bytes and `NextOffset`.
- [ ] `Fetch` with no records + `max_wait_ms=0`: coordinator called with `maxWaitMs=0`; response `Records=nil`, `NextOffset = req.Offset`.
- [ ] `Fetch` long-poll (mock coordinator blocks then returns): response arrives after records appear.
- [ ] `Fetch` `OFFSET_OUT_OF_RANGE`: gRPC `OUT_OF_RANGE`.
- [ ] Client disconnect (cancelled ctx): RPC returns error without goroutine leak.

## Tests required

- `TestDataServer_Fetch_ImmediateData` — coordinator stub returns records immediately; response non-empty.
- `TestDataServer_Fetch_EmptyNoWait` — coordinator stub returns `(nil, offset, nil)` immediately; response `Records=nil`, `NextOffset=req.Offset`.
- `TestDataServer_Fetch_LongPollReturnsData` — coordinator stub blocks 10ms then returns records; handler waits and returns.
- `TestDataServer_Fetch_OffsetOutOfRange` — coordinator returns `ErrOffsetOutOfRange`; gRPC `OUT_OF_RANGE`.
- `TestDataServer_Fetch_NotLeader` — coordinator returns `NotLeaderError`; gRPC `FAILED_PRECONDITION` + details.
- `TestDataServer_Fetch_CtxCancelled` — cancel ctx before coordinator responds; handler returns cancelled error.

## Dependencies

T-034 (proto stubs).
T-035 (gRPC infra).
T-037 (DataServer struct already created; Fetch adds to it).
T-042 (DataCoordinator.Fetch interface for test mocks).

## Notes

`ctx.Done()` fires on both `context.Canceled` (client disconnect) and `context.DeadlineExceeded` (per-call timeout). The handler does not need to differentiate: in both cases, returning `ctx.Err()` is correct — gRPC translates it to the appropriate status code. The long-poll logic (select on newDataCh) is entirely in DataCoordinator; the handler just passes `ctx` through and benefits from its cancellation.
