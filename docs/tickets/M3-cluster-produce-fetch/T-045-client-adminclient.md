# T-045: Client library — AdminClient

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** S
**Status:** TODO

## Goal

Implement `pkg/client.AdminClient` — a thin wrapper over `ManagementServiceClient` that exposes all 8 `ManagementService` RPCs as typed Go methods with common retry policy applied.

## Context

`AdminClient` is the simplest of the three client types: no internal state beyond the gRPC connection pool, no caching, no leader routing. It always targets the `ManagementService` port (`:9091`). Clients use it for topic management and cluster inspection.

References:
- [07-client-library.md §8 — AdminClient](../../design/07-client-library.md#8-adminclient)
- [06-api-protocol.md §4 — ManagementService](../../design/06-api-protocol.md#4-managementservice)
- [07-client-library.md §2 — Retry policy](../../design/07-client-library.md#2-common-configuration)

## Scope

- Create `pkg/client/admin.go`:
  - `AdminClient` struct: `config Config`, `pool *iclient.ConnPool`, `addr string`.
  - `NewAdminClient(config Config) (*AdminClient, error)` — connects to first reachable bootstrap server.
  - 8 methods, each: dial `pool.Get(addr)` → `pb.NewManagementServiceClient(conn)` → RPC with `config.RequestTimeout` deadline → convert response to typed result → apply retry for `UNAVAILABLE` / `TIMEOUT`.
    - `CreateTopic(ctx, req CreateTopicRequest) (TopicInfo, error)`.
    - `DeleteTopic(ctx, name string) error`.
    - `ListTopics(ctx) ([]TopicInfo, error)`.
    - `DescribeTopic(ctx, name string) (TopicDescription, error)`.
    - `AlterTopicPartitions(ctx, name string, newCount int32) error`.
    - `AlterTopicRetention(ctx, name string, retentionMs, retentionBytes int64) error`.
    - `DescribeCluster(ctx) (ClusterDescription, error)`.
    - `ListPartitions(ctx, topic string) ([]PartitionInfoWithOffsets, error)`.
  - Domain types `TopicInfo`, `TopicDescription`, `PartitionInfo`, `ClusterDescription`, `NodeDescriptor` in `pkg/client/types.go` (or `record.go`): mirror the coordinator types from T-039 but in the public `pkg/client` package.
  - `Close() error` — `pool.Close()`.

## Out of scope

- Producer — T-044.
- Consumer — T-046.
- Server-side implementations of these RPCs — T-036.

## Definition of done

- [ ] `go build ./pkg/client/...` passes.
- [ ] `go test ./pkg/client/...` passes.
- [ ] `CreateTopic` returns `TopicAlreadyExists` error (not a raw gRPC status) when server returns `ALREADY_EXISTS`.
- [ ] `DeleteTopic` returns `TopicNotFound` error when server returns `NOT_FOUND`.
- [ ] `DescribeCluster` returns populated `ClusterDescription` with typed `NodeDescriptor` entries.
- [ ] `UNAVAILABLE` response: client retries up to `MaxRetries` with backoff.

## Tests required

- `TestAdminClient_CreateTopic_Success` — management server stub returns success; `TopicInfo` populated.
- `TestAdminClient_CreateTopic_AlreadyExists` — server returns `ALREADY_EXISTS`; typed error returned.
- `TestAdminClient_DeleteTopic_NotFound` — server returns `NOT_FOUND`; typed error returned.
- `TestAdminClient_DescribeCluster` — server returns 3 nodes; `ClusterDescription.Nodes` has 3 entries.
- `TestAdminClient_Unavailable_Retry` — server returns `UNAVAILABLE` twice then success; client retried; final call succeeds.

## Dependencies

T-034 (proto stubs).
T-043 (ConnPool, retry helpers).

## Notes

`AdminClient` uses the `ManagementService` port (default `:9091`), not the `DataService` port. In `NewAdminClient`, the `BootstrapServers` in `config` should be treated as ManagementService addresses. This differs from `Producer` and `Consumer`, which connect to `DataService` addresses. The public `pkg/client` domain types (`TopicInfo`, etc.) duplicate fields from the coordinator's types (`internal/cluster.TopicInfo`) — this is intentional to avoid exposing internal packages through the public API surface. A `fromProtoTopicInfo(proto *pb.TopicInfo) TopicInfo` conversion helper should be defined in the package.
