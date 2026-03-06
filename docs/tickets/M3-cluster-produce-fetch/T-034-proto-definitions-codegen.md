# T-034: Proto definitions and codegen pipeline

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** S
**Status:** TODO

## Goal

Author all four `.proto` source files (`common.proto`, `errors.proto`, `management.proto`, `data.proto`), wire the protoc codegen step into the Makefile, and commit the generated Go stubs to `pkg/proto/v1/`.

## Context

BunnyMQ's wire protocol is gRPC + Protobuf over two services: `ManagementService` (port 9091) and `DataService` (port 9092). All message types, enum values, and RPC signatures are specified in `06-api-protocol.md`. Generated stubs must be committed so that downstream packages (`internal/api/management`, `internal/api/data`, `pkg/client`) can import them without running protoc at build time.

References:
- [06-api-protocol.md §2 — Proto file layout](../../design/06-api-protocol.md#2-proto-package-and-file-layout)
- [06-api-protocol.md §3 — Shared types](../../design/06-api-protocol.md#3-shared-message-types)
- [06-api-protocol.md §4 — ManagementService](../../design/06-api-protocol.md#4-managementservice)
- [06-api-protocol.md §5 — DataService](../../design/06-api-protocol.md#5-dataservice)

## Scope

- Create `api/common.proto`: `TopicInfo`, `PartitionInfo`, `PartitionInfoWithOffsets`, `NodeInfo`, `TopicPartition`, `PartitionOffset`.
- Create `api/errors.proto`: `BunnyErrorCode` enum (all 17 values), `BunnyErrorDetail`, `NotLeaderDetail`.
- Create `api/management.proto`: `ManagementService` with 8 RPCs; all request/response message types from §4.
- Create `api/data.proto`: `DataService` with 8 RPCs (Produce, Fetch, GetOffsets, JoinGroup, Heartbeat, LeaveGroup, CommitOffset, FetchCommittedOffsets); all request/response and enum types from §5.
  - Include consumer-group RPCs in the proto now even though their server implementations land in M4 — proto is the interface contract.
- Add `make proto` target to Makefile:
  ```makefile
  proto:
      protoc --go_out=. --go_opt=paths=source_relative \
             --go-grpc_out=. --go-grpc_opt=paths=source_relative \
             api/*.proto
  ```
- Commit generated `pkg/proto/v1/*.pb.go` and `pkg/proto/v1/*_grpc.pb.go`.
- Add `google.golang.org/grpc`, `google.golang.org/protobuf`, `google.golang.org/genproto` to `go.mod`.

## Out of scope

- Server implementations — T-035 through T-038.
- Client usage of generated types — T-043 through T-046.

## Definition of done

- [ ] `make proto` succeeds without errors.
- [ ] `go build ./pkg/proto/v1/...` succeeds.
- [ ] All 8 `ManagementService` RPCs are present in the generated stub.
- [ ] All 8 `DataService` RPCs are present in the generated stub.
- [ ] `BunnyErrorCode` enum contains all 17 values from §3.2.
- [ ] `pkg/proto/v1/` is committed; no `.proto` source files in `pkg/`.

## Tests required

- N/A — generated code is not tested directly. Compile-time check: `go build ./...` succeeds after generation.

## Dependencies

T-001 (repository skeleton with `go.mod`).

## Notes

The `go_package` option must be set to `github.com/bunnymq/bunnymq/pkg/proto/v1;bunnymqv1` to match the module path from `01-modules.md`. Consumer-group RPC message types (`JoinGroupRequest`, etc.) are included in `data.proto` now: the proto is a contract, not an implementation, and having the full service definition from the start prevents a painful proto-breaking change in M4. The server-side handler for those RPCs will simply return `codes.Unimplemented` in M3 and be filled in during M4.
