# API Protocol — Detailed Design

BunnyMQ exposes two gRPC services: `ManagementService` for admin and cluster operations, and `DataService` for produce, fetch, and consumer-group operations. Each service binds to its own configurable port to allow independent network-policy control. Both services share the same authentication mechanism (token in gRPC metadata) and the same server-side interceptor chain. All Protobuf message types live in the `bunnymq.v1` package; generated Go code is committed to `pkg/proto/v1/`. The on-wire batch format for produce and fetch is identical to the on-disk format specified in [02-storage.md §3.2](./02-storage.md) and [REQUIREMENTS.md §4.4](../REQUIREMENTS.md): the client encodes before sending; the server stores as-is.

See [04-cluster-coordinator.md](./04-cluster-coordinator.md) for the admin operations these RPCs delegate to, [05-data-coordinator.md](./05-data-coordinator.md) for produce/fetch routing, and [08-consumer-groups.md](./08-consumer-groups.md) for consumer group state management.

---

## 1. Service Overview

| Service | Default port | Responsibilities |
|---|---|---|
| `ManagementService` | `:9091` | Topic lifecycle (create, delete, alter); cluster description; partition listing |
| `DataService` | `:9092` | Produce, fetch, offset queries, consumer group lifecycle, offset commit/fetch |

Both ports are configurable in the node config file (`management_api.addr` and `data_api.addr`). The metrics HTTP endpoint runs on a third port (`:9090`, see [09-metrics-logging.md](./09-metrics-logging.md)).

Clients may use any broker node for `ManagementService` and `DataService` calls. For data operations, the node either handles the request (if it is the partition leader) or returns `NotLeader` with the leader's address.

---

## 2. Proto Package and File Layout

```proto
syntax = "proto3";

package bunnymq.v1;

option go_package = "github.com/bunnymq/bunnymq/pkg/proto/v1;bunnymqv1";
```

Source files in `api/` at the repo root (per [01-modules.md §1](./01-modules.md)):

```
api/
├── common.proto       — shared message types (TopicInfo, PartitionInfo, etc.)
├── errors.proto       — BunnyErrorCode enum, NotLeaderDetail
├── management.proto   — ManagementService definition and messages
└── data.proto         — DataService definition and messages
```

Generated Go code committed to `pkg/proto/v1/`. Not hand-edited.

---

## 3. Shared Message Types (`common.proto`, `errors.proto`)

### 3.1 Common domain types

```proto
// TopicInfo is the summary view of a topic returned by ListTopics and CreateTopic.
message TopicInfo {
  string name              = 1;
  int32  partition_count   = 2;
  int32  replication_factor= 3;
  int64  retention_ms      = 4;
  int64  retention_bytes   = 5;  // -1 = unlimited (REQUIREMENTS.md §4.1)
  int64  created_at_ms     = 6;
}

// PartitionInfo describes a single partition's placement and leadership.
message PartitionInfo {
  int32           partition_id      = 1;
  uint64          shard_id          = 2;
  uint64          leader_node_id    = 3;  // 0 if no leader yet elected
  int64           leader_epoch      = 4;
  repeated uint64 replica_node_ids  = 5;
}

// PartitionInfoWithOffsets extends PartitionInfo with the current offset range.
// Used by ListPartitions.
message PartitionInfoWithOffsets {
  PartitionInfo info            = 1;
  int64         earliest_offset = 2;
  int64         latest_offset   = 3;  // next offset to be assigned
}

// NodeInfo describes a broker node visible to the cluster.
message NodeInfo {
  uint64 node_id = 1;
  string address = 2;  // "host:port" of the node's Data API
}

// TopicPartition is a (topic, partition_id) pair used in group assignments
// and offset queries.
message TopicPartition {
  string topic        = 1;
  int32  partition_id = 2;
}

// PartitionOffset is a (topic, partition_id, offset) triple.
message PartitionOffset {
  string topic        = 1;
  int32  partition_id = 2;
  int64  offset       = 3;
}
```

### 3.2 Error types

```proto
// BunnyErrorCode is the application-level error code included in gRPC Status
// details when an RPC fails. The gRPC status code indicates the broad category;
// BunnyErrorCode gives the precise cause. See §7 for the mapping table.
enum BunnyErrorCode {
  OK                    = 0;
  UNKNOWN               = 1;
  INVALID_ARGUMENT      = 2;
  TOPIC_NOT_FOUND       = 3;
  TOPIC_ALREADY_EXISTS  = 4;
  PARTITION_NOT_FOUND   = 5;
  NOT_LEADER            = 6;   // detail: NotLeaderDetail
  OFFSET_OUT_OF_RANGE   = 7;
  MESSAGE_TOO_LARGE     = 8;   // single record > 1 MiB (REQUIREMENTS.md §5)
  BATCH_TOO_LARGE       = 9;   // batch > 4 MiB
  UNAUTHENTICATED       = 10;
  UNAVAILABLE           = 11;  // metadata shard no leader; retry later
  TIMEOUT               = 12;
  INVALID_MESSAGE_FORMAT= 13;  // batch CRC mismatch or truncated batch_data
  OFFSET_NOT_FOUND      = 14;  // GetOffsetByTimestamp: no record at/after timestamp
  STALE_GENERATION      = 15;  // CommitOffset: generation_id is outdated
  NOT_GROUP_MEMBER      = 16;  // operation for a member_id not in the group
}

// BunnyErrorDetail is embedded in gRPC Status.details for all non-OK responses.
message BunnyErrorDetail {
  BunnyErrorCode code    = 1;
  string         message = 2;  // human-readable; not for programmatic use
}

// NotLeaderDetail is additionally embedded in Status.details when the error
// code is NOT_LEADER. The client uses leader_address to retry the request.
message NotLeaderDetail {
  uint64 leader_node_id  = 1;
  string leader_address  = 2;  // "host:port" of the Data API on the leader
}
```

**How errors are returned (Go server side):**

```go
// Example: returning NotLeader from a gRPC handler.
st, _ := status.New(codes.FailedPrecondition, "not the partition leader").
    WithDetails(
        &pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_NOT_LEADER, Message: "..."},
        &pb.NotLeaderDetail{LeaderNodeId: nodeID, LeaderAddress: addr},
    )
return nil, st.Err()
```

**How errors are read (Go client side):**

```go
st := status.Convert(err)
for _, detail := range st.Details() {
    switch d := detail.(type) {
    case *pb.NotLeaderDetail:
        // d.LeaderAddress is the Data API address to retry against
    case *pb.BunnyErrorDetail:
        // d.Code is the application error code
    }
}
```

---

## 4. ManagementService (`management.proto`)

```proto
service ManagementService {
  // Topic lifecycle
  rpc CreateTopic           (CreateTopicRequest)           returns (CreateTopicResponse);
  rpc DeleteTopic           (DeleteTopicRequest)           returns (DeleteTopicResponse);
  rpc ListTopics            (ListTopicsRequest)            returns (ListTopicsResponse);
  rpc DescribeTopic         (DescribeTopicRequest)         returns (DescribeTopicResponse);
  rpc AlterTopicPartitions  (AlterTopicPartitionsRequest)  returns (AlterTopicPartitionsResponse);
  rpc AlterTopicRetention   (AlterTopicRetentionRequest)   returns (AlterTopicRetentionResponse);

  // Cluster description
  rpc DescribeCluster (DescribeClusterRequest) returns (DescribeClusterResponse);
  rpc ListPartitions  (ListPartitionsRequest)  returns (ListPartitionsResponse);
}
```

### 4.1 Topic lifecycle messages

```proto
message CreateTopicRequest {
  string name               = 1;  // must match [a-zA-Z0-9._-]{1,255}
  int32  partition_count    = 2;  // >= 1
  int32  replication_factor = 3;  // >= 1, <= cluster node count
  // retention_ms = 0: use broker default (604_800_000 ms = 7 days).
  // retention_bytes = 0: use broker default (1 GiB).
  // retention_bytes = -1: unlimited.
  int64  retention_ms       = 4;
  int64  retention_bytes    = 5;
}

message CreateTopicResponse {
  TopicInfo topic = 1;
}

message DeleteTopicRequest {
  string name = 1;
}

message DeleteTopicResponse {}

message ListTopicsRequest {}

message ListTopicsResponse {
  repeated TopicInfo topics = 1;
}

message DescribeTopicRequest {
  string name = 1;
}

message DescribeTopicResponse {
  TopicInfo              topic      = 1;
  repeated PartitionInfo partitions = 2;
}

message AlterTopicPartitionsRequest {
  string name               = 1;
  int32  new_partition_count= 2;  // must be > current count
}

message AlterTopicPartitionsResponse {}

message AlterTopicRetentionRequest {
  string name = 1;
  // retention_ms: 0 = no change; positive = new threshold in milliseconds.
  // retention_bytes: 0 = no change; -1 = unlimited; positive = new cap in bytes.
  // Using 0 as "no change" avoids the sentinel-value ambiguity noted in Open
  // Question 1 of 04-cluster-coordinator.md.
  int64 retention_ms    = 2;
  int64 retention_bytes = 3;
}

message AlterTopicRetentionResponse {}
```

### 4.2 Cluster description messages

```proto
message DescribeClusterRequest {}

message DescribeClusterResponse {
  repeated NodeInfo nodes = 1;
}

message ListPartitionsRequest {
  string topic = 1;
}

message ListPartitionsResponse {
  repeated PartitionInfoWithOffsets partitions = 1;
}
```

---

## 5. DataService (`data.proto`)

```proto
service DataService {
  // Data plane
  rpc Produce    (ProduceRequest)    returns (ProduceResponse);
  rpc Fetch      (FetchRequest)      returns (FetchResponse);
  rpc GetOffsets (GetOffsetsRequest) returns (GetOffsetsResponse);

  // Consumer group lifecycle
  rpc JoinGroup             (JoinGroupRequest)              returns (JoinGroupResponse);
  rpc Heartbeat             (HeartbeatRequest)              returns (HeartbeatResponse);
  rpc LeaveGroup            (LeaveGroupRequest)             returns (LeaveGroupResponse);
  rpc CommitOffset          (CommitOffsetRequest)           returns (CommitOffsetResponse);
  rpc FetchCommittedOffsets (FetchCommittedOffsetsRequest)  returns (FetchCommittedOffsetsResponse);
}
```

### 5.1 Produce

```proto
// AcksMode controls the delivery guarantee for a Produce request.
enum AcksMode {
  // ACKS_ALL is the proto3 default (value 0). The server waits for quorum
  // commit before returning the assigned offset. Safest option.
  ACKS_ALL  = 0;
  // ACKS_ZERO: fire and forget. Server returns immediately with offset = -1.
  // No durability guarantee. Batch may be silently lost on leader crash.
  ACKS_ZERO = 1;
}

message ProduceRequest {
  string   topic        = 1;
  // partition_id: if -1, the server selects a partition by round-robin
  // across available partitions for the topic.
  // The client library always sets this explicitly (FNV-1a hash of key,
  // or round-robin if key is absent); see 07-client-library.md.
  int32    partition_id = 2;
  AcksMode acks         = 3;
  // batch_data: a complete batch encoded in the on-disk/on-wire format
  // (REQUIREMENTS.md §4.4 and 02-storage.md §3.2).
  // The base_offset field in the batch header is ignored by the server —
  // Storage overwrites it with the server-assigned offset.
  // Max size: 4 MiB (REQUIREMENTS.md §5). Exceeding this returns BATCH_TOO_LARGE.
  bytes    batch_data   = 4;
}

message ProduceResponse {
  int32 partition_id = 1;  // the partition the batch was written to
  // offset: assigned base_offset of the first record in the batch.
  // Set to -1 for ACKS_ZERO (no offset assigned per REQUIREMENTS.md §3.6.1).
  int64 offset       = 2;
}
```

### 5.2 Fetch

```proto
message FetchRequest {
  string topic        = 1;
  int32  partition_id = 2;
  int64  offset       = 3;      // first offset requested; inclusive
  int32  max_bytes    = 4;      // max bytes to return; server may return fewer
  int64  max_wait_ms  = 5;      // 0 = no long-polling; >0 = wait up to this duration
}

message FetchResponse {
  // records: zero or more complete batches in the on-wire format.
  // Empty when no data is available within max_wait_ms.
  bytes records     = 1;
  // next_offset: the first offset NOT included in records.
  // Use as offset in the subsequent FetchRequest.
  // Set to the requested offset (unchanged) when records is empty.
  int64 next_offset = 2;
}
```

### 5.3 Offset queries

```proto
enum OffsetQueryType {
  EARLIEST     = 0;  // base_offset of the oldest available batch
  LATEST       = 1;  // next offset to be assigned (one past the last written)
  BY_TIMESTAMP = 2;  // base_offset of the first batch with max_timestamp >= timestamp_ms
}

message GetOffsetsRequest {
  string          topic        = 1;
  int32           partition_id = 2;
  OffsetQueryType query_type   = 3;
  // timestamp_ms: used only when query_type == BY_TIMESTAMP.
  int64           timestamp_ms = 4;
}

message GetOffsetsResponse {
  // offset: the requested offset value.
  // For BY_TIMESTAMP: the base_offset of the first batch whose max_timestamp
  // >= timestamp_ms. Returns error OFFSET_NOT_FOUND if no such batch exists.
  int64 offset = 1;
}
```

### 5.4 Consumer group lifecycle

```proto
message JoinGroupRequest {
  string          group_id          = 1;
  // member_id: empty string = server assigns a new UUID.
  // Returning members should send their previously assigned member_id.
  string          member_id         = 2;
  repeated string subscribed_topics = 3;
  // client_host: informational; the client's hostname or IP address.
  string          client_host       = 4;
}

message JoinGroupResponse {
  string                  member_id     = 1;  // server-assigned UUID if request had empty member_id
  int32                   generation_id = 2;
  repeated TopicPartition assignments   = 3;  // partitions assigned to this member
}

// HeartbeatStatus signals whether the group has been rebalanced since
// the member last joined.
enum HeartbeatStatus {
  HEARTBEAT_OK                = 0;
  HEARTBEAT_REBALANCE_REQUIRED= 1;  // member must re-issue JoinGroup
}

message HeartbeatRequest {
  string group_id     = 1;
  string member_id    = 2;
  int32  generation_id= 3;
}

message HeartbeatResponse {
  HeartbeatStatus status = 1;
}

message LeaveGroupRequest {
  string group_id  = 1;
  string member_id = 2;
}

message LeaveGroupResponse {}

message CommitOffsetRequest {
  string                  group_id     = 1;
  string                  member_id    = 2;
  int32                   generation_id= 3;
  repeated PartitionOffset offsets      = 4;
}

// PartitionOffsetError reports per-partition commit failures.
// Absent from CommitOffsetResponse.errors if the partition committed successfully.
message PartitionOffsetError {
  string         topic        = 1;
  int32          partition_id = 2;
  BunnyErrorCode error_code   = 3;
}

message CommitOffsetResponse {
  // errors: empty on full success. Partial success is possible (e.g., some
  // partitions committed, others returned STALE_GENERATION).
  repeated PartitionOffsetError errors = 1;
}

message FetchCommittedOffsetsRequest {
  string                  group_id  = 1;
  repeated TopicPartition partitions = 2;
}

message FetchCommittedOffsetsResponse {
  // offsets: one entry per requested partition that has a committed offset.
  // Partitions with no committed offset are omitted (not returned as 0 or -1).
  repeated PartitionOffset offsets = 1;
}
```

---

## 6. Streaming Considerations

Both `Produce` and `Fetch` are **unary RPCs** in v1. This section documents the trade-offs and defers streaming to future work.

### Produce

| Approach | Latency per batch | Throughput | Implementation cost |
|---|---|---|---|
| Unary (v1) | 1 RTT per RPC | Limited by per-RPC overhead | Minimal |
| Bidirectional streaming | Amortised; multiple batches in-flight | High; pipelining within one shard | Requires stream lifecycle management, flow control, error propagation |

With unary produce, a client sending one batch per 5 ms (200 batches/s) would pay 200 RPC round-trips per second. For the demo-quality throughput target ("tens of thousands of messages per second per partition", REQUIREMENTS.md §6.2), batching within each RPC (multiple records per batch) reduces this cost significantly — the batch is the unit of the RPC, not the individual record.

**Future work:** A bidirectional-streaming `ProduceStream` RPC, where the client keeps one open stream per (producer, partition) pair, would eliminate repeated connection setup and allow the server to pipeline multiple Raft proposals. The proto definition would be `rpc ProduceStream(stream ProduceRequest) returns (stream ProduceResponse)`. Client library support (stream lifecycle, retry on leader change) is non-trivial.

### Fetch

| Approach | Description | Implementation cost |
|---|---|---|
| Unary + long-poll (v1) | Server blocks up to max_wait_ms; client re-issues after each response | Client polling loop |
| Server-streaming | Server pushes batches as they arrive; client receives until stream ends or leader changes | Complex error handling, backpressure, leader-change recovery |

Long-poll unary achieves the same latency as server-streaming for steady-state consumption (wake up within one newDataCh notification, ~microseconds after the batch commits). The difference is connection reuse overhead: with unary, each `Fetch` call establishes a new HTTP/2 stream on an existing connection (cheap in gRPC's H2 multiplexing).

**Future work:** `rpc FetchStream(FetchRequest) returns (stream FetchResponse)` — server sends batches as they become available. The server-side logic would select on `newDataCh` in a loop and stream back each new batch. Client receives until the stream is terminated by a leader change (at which point it reconnects to the new leader).

---

## 7. Error Code to gRPC Status Mapping

The gRPC status code is the transport-level error indicator. `BunnyErrorCode` is embedded in `Status.details` as a `BunnyErrorDetail` for programmatic use. Human-readable status messages are for debugging only.

| BunnyErrorCode | gRPC Status Code | Notes |
|---|---|---|
| `OK` | `OK` | |
| `UNKNOWN` | `INTERNAL` | Unexpected server error |
| `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | Name format, count out of range, etc. |
| `TOPIC_NOT_FOUND` | `NOT_FOUND` | |
| `TOPIC_ALREADY_EXISTS` | `ALREADY_EXISTS` | CreateTopic idempotency response |
| `PARTITION_NOT_FOUND` | `NOT_FOUND` | |
| `NOT_LEADER` | `FAILED_PRECONDITION` | Status.details also includes `NotLeaderDetail` |
| `OFFSET_OUT_OF_RANGE` | `OUT_OF_RANGE` | Consumer must call GetOffsets/EARLIEST to seek |
| `MESSAGE_TOO_LARGE` | `INVALID_ARGUMENT` | Single record exceeds 1 MiB |
| `BATCH_TOO_LARGE` | `INVALID_ARGUMENT` | Entire batch exceeds 4 MiB |
| `UNAUTHENTICATED` | `UNAUTHENTICATED` | Missing or invalid `bunnymq-auth-token` |
| `UNAVAILABLE` | `UNAVAILABLE` | No Raft leader; retry after back-off |
| `TIMEOUT` | `DEADLINE_EXCEEDED` | Context deadline exceeded in Raft propose |
| `INVALID_MESSAGE_FORMAT` | `INVALID_ARGUMENT` | Bad CRC, truncated batch_data |
| `OFFSET_NOT_FOUND` | `NOT_FOUND` | GetOffsetByTimestamp: no record at/after ts |
| `STALE_GENERATION` | `FAILED_PRECONDITION` | CommitOffset with outdated generation_id |
| `NOT_GROUP_MEMBER` | `FAILED_PRECONDITION` | member_id not current member of the group |

**Retriable errors** (client library should retry after back-off): `UNAVAILABLE`, `DEADLINE_EXCEEDED`. `NOT_LEADER` is retriable against the new leader address from `NotLeaderDetail`. `ALREADY_EXISTS` is retriable as a success (idempotent topic creation). All other error codes are non-retriable.

---

## 8. Authentication

### Metadata key

```
bunnymq-auth-token
```

Clients include this key in the gRPC outgoing metadata on every RPC:

```go
// Client-side (gRPC interceptor or per-call):
ctx = metadata.AppendToOutgoingContext(ctx, "bunnymq-auth-token", token)
```

The key is lowercase ASCII. gRPC metadata keys are case-insensitive; servers must read with the lowercase key.

### Server-side validation

The auth interceptor (first in the chain, §9) performs:

```go
func validateToken(ctx context.Context, validTokens []string) error {
    // PLAINTEXT mode: empty token list → bypass validation entirely.
    if len(validTokens) == 0 {
        return nil
    }
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return status.Error(codes.Unauthenticated, "missing metadata")
    }
    tokens := md.Get("bunnymq-auth-token")
    if len(tokens) == 0 {
        return status.Error(codes.Unauthenticated, "missing bunnymq-auth-token")
    }
    for _, valid := range validTokens {
        if tokens[0] == valid {
            return nil
        }
    }
    return status.Error(codes.Unauthenticated, "invalid token")
}
```

**PLAINTEXT mode.** If the configured token list is empty, all requests are accepted without a token. This is the default for local development and demo deployments (REQUIREMENTS.md §8.1). No configuration change beyond setting `auth.tokens: []` is required.

**Token format.** Tokens are opaque strings; any non-empty string is valid as long as it matches a configured token. No JWT parsing, no expiration, no rotation in v1 (REQUIREMENTS.md §8.1).

**TLS.** Sending tokens over unencrypted gRPC exposes them to network observers. For environments where this is a concern, enable gRPC TLS via `tls.cert_file` and `tls.key_file` in the node config (REQUIREMENTS.md §8.3). mTLS is not supported in v1.

---

## 9. Server-Side Interceptor Chain

Both services apply the same interceptor chain in this order:

```
Auth → Logging → Metrics → Handler
```

In Go gRPC, registered using `grpc.ChainUnaryInterceptor` and `grpc.ChainStreamInterceptor`:

```go
grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        auth.UnaryInterceptor(config.AuthTokens),
        logging.UnaryInterceptor(logger),
        metrics.UnaryInterceptor(metricsRegistry),
    ),
    grpc.ChainStreamInterceptor(
        auth.StreamInterceptor(config.AuthTokens),
        logging.StreamInterceptor(logger),
        metrics.StreamInterceptor(metricsRegistry),
    ),
    // TLS credentials appended if config.TLS is enabled.
)
```

### Interceptor responsibilities

**Auth interceptor** — outermost. Validates `bunnymq-auth-token`. Returns `UNAUTHENTICATED` immediately if invalid; does not call the next interceptor. This prevents logging and metric recording for unauthenticated noise, though auth failures are logged by the auth interceptor itself at `warn` level (for security audit).

**Logging interceptor.** Logs the RPC at `debug` level on entry; logs the result (status code, latency) at `info` level on exit. Extracts `request_id` from incoming metadata (if present) and attaches it to the logger for the duration of the call.

**Metrics interceptor.** Increments `bunnymq_grpc_requests_total{method, code}` after the handler completes. Records `bunnymq_grpc_request_duration_seconds{method}`. See [09-metrics-logging.md §2.4.3](./09-metrics-logging.md).

**Handler.** The actual gRPC service implementation (`internal/api/data` or `internal/api/management`). Delegates to the relevant coordinator.

---

## 10. Wire Batch Format

The `batch_data` field in `ProduceRequest` and the `records` field in `FetchResponse` use the identical on-disk/on-wire batch format specified in [REQUIREMENTS.md §4.4](../REQUIREMENTS.md) and [02-storage.md §3.2](./02-storage.md). The format is reproduced here for reference:

```text
Batch (identical on disk and on wire):
  base_offset    : int64   — client sets 0; server overwrites before storage
  batch_length   : int32   — total byte length including header (38 bytes)
  record_count   : int32
  crc32          : uint32  — CRC-32C over records[] (bytes [38, batch_length))
  attributes     : int16   — reserved; must be 0 in v1
  base_timestamp : int64   — ms since Unix epoch; timestamp of first record
  max_timestamp  : int64   — ms since Unix epoch; timestamp of last record
  records[]      : variable-length; one or more Record values

Record:
  length         : varint  — byte length of remaining record fields
  attributes     : int8    — reserved; 0 in v1
  timestamp_delta: varint  — record.timestamp_ms - base_timestamp
  offset_delta   : varint  — record.offset - base_offset
  key_length     : varint  — -1 if key is nil
  key            : bytes   — absent when key_length == -1
  value_length   : varint
  value          : bytes
  headers_count  : varint
  headers        : [Header]*
```

The `FetchResponse.records` field may contain **multiple consecutive batches** (as many as fit within `max_bytes`). The client parses them by reading `batch_length` from each batch header and advancing by that many bytes.

**Server-side validation.** The `DataAPI` handler validates `batch_data` before calling `DataCoordinator.Produce`:
1. Length check: `len(batch_data) < 38` → `INVALID_MESSAGE_FORMAT`
2. Batch length field: `batch_length < 38` or `batch_length > len(batch_data)` → `INVALID_MESSAGE_FORMAT`
3. Batch size: `len(batch_data) > 4 MiB` → `BATCH_TOO_LARGE`
4. CRC-32C: compute over `batch_data[38:batch_length]`; mismatch → `INVALID_MESSAGE_FORMAT`

Individual record size is not validated server-side in v1 (the 1 MiB limit is documented as a client-library concern). VERIFY: whether server-side per-record size validation is needed to prevent oversized records bypassing the client library.

---

## 11. Versioning

The proto package is `bunnymq.v1`. All service definitions, message types, and enum values are part of this versioned package. `v1` is the only version in scope for BunnyMQ v1.

**Forward compatibility rules for `v1`:**
- New fields may be added to any message (proto3 unknown fields are ignored by older clients).
- Existing field numbers must never be reused or removed.
- Enum values may be added but not removed.
- New RPCs may be added to either service.

No breaking changes are expected within v1. A future `bunnymq.v2` package would coexist with `v1` in separate proto files and Go packages, allowing phased migration.

---

## 12. Open Questions

1. **`AlterTopicRetention` sentinel values (resolved at proto level).** This document adopts: `retention_ms = 0` means "no change" (since 0 ms retention is nonsensical); `retention_bytes = 0` means "no change"; `retention_bytes = -1` means "unlimited". This resolves Open Question 5 from [04-cluster-coordinator.md](./04-cluster-coordinator.md) at the proto layer. The coordinator implementation must map these sentinel values to the appropriate FSM command fields.

2. **`FetchResponse.next_offset` on empty response.** When `records` is empty (timeout or no data), `next_offset` is set to the originally requested `offset` (unchanged). This lets the client simply re-use the same offset for the next `Fetch` without tracking state. Confirm this is the intended behaviour vs. returning `0` (ambiguous) or including it in the `FetchResponse.records` field description.

3. **`GetOffsetsResponse` for `BY_TIMESTAMP` — batch base_offset vs. individual record offset.** The current spec returns the `base_offset` of the first matching batch. If the matching record is not the first in the batch, the consumer would re-read records before the target timestamp. Confirm whether Kafka-compatible semantics (return the offset of the first record at or after the timestamp within the matching batch) are required. If so, the server must partially decode the batch to compute this. See Open Question 4 in [05-data-coordinator.md](./05-data-coordinator.md).

4. **Server-side partition selection for `partition_id = -1`.** When `ProduceRequest.partition_id = -1`, the server selects a partition by round-robin. This requires a `LookupMetadata(QueryGetTopic)` call to get the partition count, then a per-topic round-robin counter maintained in the DataCoordinator. Confirm: (a) round-robin state per topic is maintained in DataCoordinator; (b) the counter is not persisted (resets on restart, acceptable for load balancing). Key-hash-based selection is not performed server-side since the key is embedded inside `batch_data` (requiring batch decoding).

5. **`request_id` propagation.** The logging interceptor is specified to extract `request_id` from incoming metadata. Confirm the metadata key name (proposed: `x-request-id`, following common HTTP convention). Clients that set this field enable end-to-end request tracing in logs.

6. **VERIFY — `google.rpc.Status` compatibility.** The `BunnyErrorDetail` and `NotLeaderDetail` messages are embedded in `google.rpc.Status.details` (a `repeated google.protobuf.Any`). Verify that the generated Go code from `google.golang.org/grpc/status` and `google.golang.org/genproto/googleapis/rpc/status` correctly packs/unpacks these custom message types via `status.WithDetails()` and `status.Details()`. No functional concern is expected; this is a build-dependency verification.
