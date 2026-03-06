# T-036: Management API gRPC server

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement `ManagementServiceServer` in `internal/api/management` — all 8 RPCs — translating gRPC request/response types to and from `ClusterCoordinator` method calls and mapping coordinator errors to gRPC status codes with `BunnyErrorDetail`.

## Context

`ManagementService` is the admin-plane API: topic lifecycle and cluster description. It delegates entirely to `ClusterCoordinator` (T-039). The handler's only responsibilities are: decode proto → call coordinator → encode result → map errors to gRPC status. Error mapping follows the table in `06-api-protocol.md §7`.

References:
- [06-api-protocol.md §4 — ManagementService messages](../../design/06-api-protocol.md#4-managementservice)
- [06-api-protocol.md §7 — Error mapping table](../../design/06-api-protocol.md#7-error-code-to-grpc-status-mapping)
- [04-cluster-coordinator.md §2 — Public interface](../../design/04-cluster-coordinator.md#2-public-interface)

## Scope

- Create `internal/api/management/server.go`:
  - `ManagementServer` struct embedding `cluster.ClusterCoordinator`.
  - Implement `pb.ManagementServiceServer` interface (8 methods):
    - `CreateTopic` → `cc.CreateTopic`; returns `CreateTopicResponse{Topic: protoTopicInfo(...)}`.
    - `DeleteTopic` → `cc.DeleteTopic`.
    - `ListTopics` → `cc.ListTopics`.
    - `DescribeTopic` → `cc.DescribeTopic`; returns `DescribeTopicResponse{Topic, Partitions}`.
    - `AlterTopicPartitions` → `cc.AlterTopicPartitionCount`.
    - `AlterTopicRetention` → `cc.AlterTopicRetention`.
    - `DescribeCluster` → `cc.DescribeCluster`.
    - `ListPartitions` → `cc.DescribeTopic` (reads partition info + offset queries via DataCoordinator for offsets).
  - Helper `mapCoordError(err error) error` converts coordinator sentinel errors to gRPC status with `BunnyErrorDetail` embedded via `status.WithDetails`.
- Create `internal/api/management/errors.go`: sentinel error types (`ErrTopicNotFound`, `ErrTopicAlreadyExists`, `ErrInvalidArgument`, `ErrUnavailable`) returned by coordinator and mapped here.

## Out of scope

- DataService handlers — T-037, T-038.
- ClusterCoordinator implementation — T-039, T-040.
- ListPartitions earliest/latest offset population — requires DataCoordinator (T-041); for M3 return 0/0 as stubs.

## Definition of done

- [ ] `go build ./internal/api/management/...` passes.
- [ ] `go test ./internal/api/management/...` passes.
- [ ] `CreateTopic` maps `ErrTopicAlreadyExists` → `codes.AlreadyExists` + `BunnyErrorCode_TOPIC_ALREADY_EXISTS`.
- [ ] `DeleteTopic` maps `ErrTopicNotFound` → `codes.NotFound` + `BunnyErrorCode_TOPIC_NOT_FOUND`.
- [ ] `AlterTopicPartitions` maps `ErrInvalidArgument` → `codes.InvalidArgument`.
- [ ] `DescribeCluster` returns populated `ClusterDescription` from coordinator.

## Tests required

- `TestManagementServer_CreateTopic_Success` — mock coordinator returns `TopicInfo`; response has correct proto fields.
- `TestManagementServer_CreateTopic_AlreadyExists` — coordinator returns `ErrTopicAlreadyExists`; gRPC status `ALREADY_EXISTS`.
- `TestManagementServer_DeleteTopic_NotFound` — coordinator returns `ErrTopicNotFound`; gRPC status `NOT_FOUND`.
- `TestManagementServer_DescribeTopic_Success` — coordinator returns `TopicDescription` with 3 partitions; proto has 3 `PartitionInfo` entries.
- `TestManagementServer_AlterTopicPartitions_InvalidArg` — coordinator returns `ErrInvalidArgument`; gRPC status `INVALID_ARGUMENT`.
- `TestManagementServer_DescribeCluster` — coordinator returns 3 nodes; proto has 3 `NodeInfo` entries.

## Dependencies

T-034 (proto stubs).
T-035 (gRPC server + interceptor infra).
T-039 (ClusterCoordinator interface, for mock/stub in tests).

## Notes

Tests use a mock or stub `ClusterCoordinator` (interface or simple struct). The coordinator interface should be extracted as `ClusterCoordinatorIface` in its own file so that tests in `internal/api/management` can provide a test double without importing the full coordinator. `ListPartitions` calls `DescribeTopic` for partition topology; it also needs `EarliestOffset`/`LatestOffset` for each partition. For M3, stub these as 0 and implement the real offset queries when DataCoordinator (T-041) is complete — the two tickets are in the same milestone and can be integrated in either order.
