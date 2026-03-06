# T-037: Data API — Produce and offset queries

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement the `Produce` and `GetOffsets` RPCs of `DataServiceServer` in `internal/api/data`, including server-side batch validation (CRC-32C, size limits) and gRPC error mapping.

## Context

`Produce` is the hot path: it validates the `batch_data` bytes, calls `DataCoordinator.Produce`, and returns the assigned offset. `GetOffsets` covers three query types (EARLIEST, LATEST, BY_TIMESTAMP). Both RPCs must return `NotLeaderDetail` in `Status.details` when the local node is not the partition leader, so the client can retry against the correct node.

References:
- [06-api-protocol.md §5.1 — Produce](../../design/06-api-protocol.md#51-produce)
- [06-api-protocol.md §5.3 — Offset queries](../../design/06-api-protocol.md#53-offset-queries)
- [06-api-protocol.md §7 — Error mapping](../../design/06-api-protocol.md#7-error-code-to-grpc-status-mapping)
- [06-api-protocol.md §10 — Wire batch validation](../../design/06-api-protocol.md#10-wire-batch-format)
- [05-data-coordinator.md §5 — Produce flow](../../design/05-data-coordinator.md#5-produce-flow)
- [05-data-coordinator.md §7 — Offset queries](../../design/05-data-coordinator.md#7-offset-queries)

## Scope

- Create `internal/api/data/server.go`: `DataServer` struct embedding a `DataCoordinatorIface`.
- Implement `Produce(ctx, *pb.ProduceRequest) (*pb.ProduceResponse, error)`:
  - Validate `batch_data`: `len < 38` → `INVALID_MESSAGE_FORMAT`; `batch_length` field inconsistency → `INVALID_MESSAGE_FORMAT`; `len > 4 MiB` → `BATCH_TOO_LARGE`; CRC-32C mismatch (over bytes `[38:batch_length)`) → `INVALID_MESSAGE_FORMAT`.
  - Call `dc.Produce(ctx, topic, partitionID, batchData, AcksMode(req.Acks))`.
  - On `NotLeaderError`: return `codes.FailedPrecondition` + `BunnyErrorDetail{NOT_LEADER}` + `NotLeaderDetail{leaderNodeID, leaderAddress}` via `status.WithDetails`.
  - On success: return `ProduceResponse{PartitionId: partID, Offset: assignedOffset}`.
- Implement `GetOffsets(ctx, *pb.GetOffsetsRequest) (*pb.GetOffsetsResponse, error)`:
  - Dispatch on `req.QueryType`: `EARLIEST` → `dc.GetEarliestOffset`; `LATEST` → `dc.GetLatestOffset`; `BY_TIMESTAMP` → `dc.GetOffsetByTimestamp(ctx, topic, partID, req.TimestampMs)`.
  - Map `ErrOffsetNotFound` → `codes.NotFound + BunnyErrorCode_OFFSET_NOT_FOUND`.
- Create `internal/api/data/validate.go`: `validateBatch(data []byte) error` — the four validation steps above.
- Implement consumer-group RPCs (`JoinGroup`, `Heartbeat`, `LeaveGroup`, `CommitOffset`, `FetchCommittedOffsets`) as `return nil, status.Error(codes.Unimplemented, "not implemented in M3")` stubs.

## Out of scope

- Fetch RPC — T-038.
- DataCoordinator implementation — T-041.
- Consumer group RPC implementations — M4.

## Definition of done

- [ ] `go build ./internal/api/data/...` passes.
- [ ] `go test ./internal/api/data/...` passes.
- [ ] `Produce`: valid batch → coordinator called; response has correct `offset`.
- [ ] `Produce`: CRC mismatch → `codes.InvalidArgument` + `INVALID_MESSAGE_FORMAT`.
- [ ] `Produce`: `len(batch_data) > 4 MiB` → `codes.InvalidArgument` + `BATCH_TOO_LARGE`.
- [ ] `Produce`: `NotLeaderError` → `codes.FailedPrecondition`; `NotLeaderDetail` in Status.details.
- [ ] `GetOffsets EARLIEST/LATEST` → coordinator returns correct offset.
- [ ] `GetOffsets BY_TIMESTAMP` with no matching batch → `codes.NotFound`.

## Tests required

- `TestDataServer_Produce_ValidBatch` — crafted valid batch; coordinator stub returns offset 42; response `Offset=42`.
- `TestDataServer_Produce_CRCMismatch` — corrupt last byte of CRC field; returns `INVALID_MESSAGE_FORMAT`.
- `TestDataServer_Produce_TooLarge` — `len > 4 MiB`; returns `BATCH_TOO_LARGE`.
- `TestDataServer_Produce_NotLeader` — coordinator returns `NotLeaderError{nodeID:2, addr:"broker2:9092"}`; status details contain `NotLeaderDetail`.
- `TestDataServer_GetOffsets_Earliest` — coordinator returns 0; response `Offset=0`.
- `TestDataServer_GetOffsets_ByTimestamp_NotFound` — coordinator returns `ErrOffsetNotFound`; status `NOT_FOUND`.
- `TestValidateBatch_ShortHeader` — `len < 38` → error.
- `TestValidateBatch_BadBatchLength` — `batch_length > len(data)` → error.

## Dependencies

T-034 (proto stubs).
T-035 (gRPC infra).
T-041 (DataCoordinator interface for tests).

## Notes

CRC-32C polynomial is Castagnoli (`0x1EDC6F41`); use `hash/crc32.MakeTable(crc32.Castagnoli)`. The `batch_length` field is at bytes `[8:12]` (int32 big-endian); compute it from the batch header before using it to bound the CRC slice. The four validation checks are designed to reject malformed batches before calling into dragonboat, preventing any storage corruption from reaching the FSM. Tests should use a real `EncodeBatch` from T-015 to produce valid test input — do not hand-craft raw bytes except for error-case tests.
