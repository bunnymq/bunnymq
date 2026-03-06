# T-013: Proto codegen pipeline

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

## Goal

Create the four `.proto` source files in `api/` matching `06-api-protocol.md`, configure `buf` (or `protoc`) for reproducible codegen, and commit generated Go stubs to `pkg/proto/v1/` so that `make proto` regenerates them idempotently.

## Context

`01-modules.md §1` specifies that source `.proto` files live in `api/` and generated Go code is committed to `pkg/proto/v1/`. `06-api-protocol.md §2–5` defines the complete content of all four proto files: `common.proto`, `errors.proto`, `management.proto`, `data.proto`. The generated stubs are the foundation for all gRPC server and client code in M3.

References:
- [01-modules.md §1](../../design/01-modules.md#1-module-tree) — proto file placement convention
- [06-api-protocol.md §2](../../design/06-api-protocol.md#2-proto-package-and-file-layout) — proto package and file layout
- [06-api-protocol.md §3](../../design/06-api-protocol.md#3-shared-message-types-commonproto-errorsproto) — common and error types
- [06-api-protocol.md §4](../../design/06-api-protocol.md#4-managementservice-managementproto) — ManagementService
- [06-api-protocol.md §5](../../design/06-api-protocol.md#5-dataservice-dataproto) — DataService

## Scope

- Write `api/common.proto`: `TopicInfo`, `PartitionInfo`, `PartitionInfoWithOffsets`, `NodeInfo`, `TopicPartition`, `PartitionOffset` messages.
- Write `api/errors.proto`: `BunnyErrorCode` enum, `BunnyErrorDetail` message, `NotLeaderDetail` message.
- Write `api/management.proto`: `ManagementService` with all 8 RPCs and their request/response messages from `06-api-protocol.md §4`.
- Write `api/data.proto`: `DataService` with all 8 RPCs and their request/response messages from `06-api-protocol.md §5`, including `AcksMode`, `OffsetQueryType`, `HeartbeatStatus` enums.
- Configure `buf.yaml` (module: `buf.build/bunnymq/bunnymq`) and `buf.gen.yaml` with Go + gRPC-Go plugins, outputting to `pkg/proto/v1/`.
- Run `buf generate` (or `protoc` if buf is unavailable); commit generated `*.pb.go` and `*_grpc.pb.go` files.
- Add `google.golang.org/grpc` and `google.golang.org/protobuf` to `go.mod` (already in T-012 skeleton; verify versions are consistent with buf-generated code).
- Add `google.golang.org/genproto/googleapis/rpc/status` if needed for `BunnyErrorDetail` packing/unpacking via `status.WithDetails`.

## Out of scope

- gRPC server/client implementations — M3 tickets.
- buf remote lint rules or breaking-change detection (optional enhancement, not required for M0).

## Definition of done

- [ ] `api/common.proto`, `api/errors.proto`, `api/management.proto`, `api/data.proto` created with all types from `06-api-protocol.md §3–5`.
- [ ] `make proto` (delegating to `buf generate`) regenerates `pkg/proto/v1/*.pb.go` and `*_grpc.pb.go` idempotently.
- [ ] `go build ./pkg/proto/...` passes after codegen.
- [ ] `ManagementServiceClient` and `DataServiceClient` stubs are importable from `pkg/proto/v1`.
- [ ] Generated files committed to the repository.
- [ ] `buf.yaml` and `buf.gen.yaml` committed.
- [ ] `google.golang.org/grpc` version in `go.mod` is compatible with generated stubs (grpc-go v1.6x+).

## Tests required

`TestProtoCompiles` — satisfied by `go build ./pkg/proto/...` in CI.

`TestManagementServiceDefinition` and `TestDataServiceDefinition` — verify at compile time that `ManagementServiceServer` and `DataServiceServer` interfaces exist and have the expected methods. These can be trivial compile-time type assertions:

```go
// pkg/proto/v1/proto_test.go
var _ ManagementServiceServer = (*UnimplementedManagementServiceServer)(nil)
var _ DataServiceServer        = (*UnimplementedDataServiceServer)(nil)
```

## Dependencies

T-012 (go.mod must exist before proto codegen can output to the module).

## Notes

`go_package` option must be `github.com/bunnymq/bunnymq/pkg/proto/v1;bunnymqv1` as specified in `06-api-protocol.md §2`. If using buf, pin the plugin versions in `buf.gen.yaml` to avoid version drift. The `BunnyErrorDetail` and `NotLeaderDetail` messages are embedded in `google.rpc.Status.details` — this requires importing `google/rpc/status.proto`; add the corresponding `googleapis` dependency to `buf.yaml` deps. Verify that `status.WithDetails()` and `status.Details()` work correctly with custom message types at build time (resolves `06-api-protocol.md OQ6`).
