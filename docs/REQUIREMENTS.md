# BunnyMQ — Requirements Specification

This document defines the functional and non-functional requirements for BunnyMQ. It is the source of truth for what the system does. In case of conflict between this document and any design document, this document wins. Designers must update this document explicitly to reflect any agreed change of scope.

## 1. Project summary

BunnyMQ is a Kafka-like distributed message broker written in Go. It supports topics, partitions, consumer groups, replicated durable storage, and configurable delivery guarantees. The system is designed to run as a cluster of 3 or more nodes with horizontal scalability via partitioning.

## 2. Glossary

- **Topic** — a logical message stream identified by a unique name.
- **Partition** — an independent ordered subset of a topic's messages, identified by `(topic, partition_id)`. Order is preserved within a partition only.
- **Offset** — a monotonically increasing 64-bit integer identifying a message's position within a partition.
- **Producer** — a client that sends messages to topics.
- **Consumer** — a client that reads messages from topics.
- **Consumer Group** — a set of consumers cooperatively consuming a set of partitions, where each partition is read by exactly one consumer in the group.
- **Broker / Node** — a single BunnyMQ server process. A cluster has multiple nodes.
- **Shard** — a Raft replication group as defined by dragonboat. One shard exists per partition, plus one shard for cluster metadata.
- **Replica** — a copy of a partition on a specific node, participating in the partition's Raft shard.
- **Leader** — the current Raft leader of a shard. Reads and writes for that shard go through the leader.
- **Committed offset (consumer)** — the last offset within a partition that a consumer group has acknowledged as processed.
- **Committed index (Raft)** — the last log index acknowledged by quorum of a Raft shard. Records up to this index are durable and visible.

## 3. Functional requirements

### 3.1 Topic management (Admin API)

- **3.1.1 Create topic.** Inputs: `name`, `partition_count`, `replication_factor`, optional config map (retention settings). Outputs: success/failure with reason. Idempotent on existence (returns AlreadyExists).
- **3.1.2 Delete topic.** Inputs: `name`. Outputs: success/failure. Asynchronous physical deletion is acceptable.
- **3.1.3 List topics.** Outputs: list of `(name, partition_count, replication_factor)`.
- **3.1.4 Describe topic.** Inputs: `name`. Outputs: full topic metadata including per-partition leader and replica node IDs.
- **3.1.5 Alter topic — increase partition count.** Inputs: `name`, `new_partition_count` (must be >= current). Outputs: success/failure. Decreasing partition count is **not supported**.
- **3.1.6 Alter topic — change retention config.** Inputs: `name`, new `retention_ms` and/or `retention_bytes`. Outputs: success/failure. New retention settings take effect on the next retention enforcement cycle.

### 3.2 Cluster management (Admin API)

- **3.2.1 Describe cluster.** Outputs: list of nodes with `(node_id, address, status)`.
- **3.2.2 List partitions for a topic.** Inputs: `topic`. Outputs: list of `(partition_id, leader_node_id, replica_node_ids, current_offset_range)`.
- **3.2.3 Cluster membership is static.** All nodes are configured at cluster bootstrap and do not change during runtime. Adding or removing nodes at runtime is out of scope for v1.

### 3.3 Producer API (Data API)

- **3.3.1 Produce.** Inputs: `topic`, optional `partition_id` (if absent, choose by key hash or round-robin), optional `key` (bytes), `value` (bytes), optional `headers` (map<string, bytes>), `acks` (one of `0`, `all`). Outputs: on success — `partition_id`, assigned `offset`. On error — error code + description.
- **3.3.2 Batched produce.** Producers may send a batch of records in a single request. Records in the batch are guaranteed to land in the same partition with consecutive offsets, atomically (all or none).

### 3.4 Consumer API (Data API)

- **3.4.1 Fetch.** Inputs: `topic`, `partition_id`, `offset`, `max_bytes`, optional `max_wait_ms`. Outputs: list of records with their offsets. If no records available within `max_wait_ms`, returns empty result. The server may return fewer bytes than `max_bytes` if a record boundary is crossed.
- **3.4.2 Get offset by timestamp.** Inputs: `topic`, `partition_id`, `timestamp`. Outputs: smallest `offset` whose record timestamp is >= input timestamp, or "not found".
- **3.4.3 Get earliest/latest offset.** Inputs: `topic`, `partition_id`. Outputs: smallest/largest valid offset.

### 3.5 Consumer group API

- **3.5.1 Join group.** Inputs: `group_id`, `member_id` (optional, server assigns if empty), subscribed topics. Outputs: assigned `member_id`, current `generation_id`, partition assignment.
- **3.5.2 Heartbeat.** Inputs: `group_id`, `member_id`, `generation_id`. Outputs: success or "rebalance required".
- **3.5.3 Leave group.** Inputs: `group_id`, `member_id`. Triggers rebalance.
- **3.5.4 Commit offset.** Inputs: `group_id`, `member_id`, `generation_id`, list of `(topic, partition_id, offset)`. Outputs: success/failure per partition.
- **3.5.5 Fetch committed offsets.** Inputs: `group_id`, list of `(topic, partition_id)`. Outputs: list of committed offsets.
- **3.5.6 Rebalancing strategy.** v1 uses simple range-based assignment: partitions are sorted by `partition_id`, members are sorted by `member_id`, and partitions are split into contiguous ranges of equal size (with remainder going to earlier members). No sticky assignment, no cooperative rebalance. Every membership change triggers a full reassignment with a bumped `generation_id`.

### 3.6 Delivery guarantees

- **3.6.1 acks=0.** Producer fires and forgets. No offset returned. No durability guarantee. Implemented via `dragonboat.Propose` (asynchronous).
- **3.6.2 acks=all.** Producer waits for quorum commit. Offset returned. Durability: data is replicated to a quorum of partition replicas before ack. Implemented via `dragonboat.SyncPropose`.
- **3.6.3 acks=1 is NOT supported.** Documented as a deliberate omission due to incompatibility with Raft semantics.
- **3.6.4 Consumer side.** At-least-once delivery is the default: consumer commits offsets explicitly after processing. Auto-commit is supported as a client-library convenience but ultimately produces the same semantic if a crash happens between processing and commit.
- **3.6.5 Exactly-once is NOT supported.** No idempotent producer, no transactions.

## 4. Data model

### 4.1 Topic metadata

| Field | Type | Notes |
|-------|------|-------|
| name | string | unique cluster-wide; must match `[a-zA-Z0-9._-]{1,255}` |
| partition_count | int32 | >= 1 |
| replication_factor | int32 | >= 1, <= cluster size |
| retention_ms | int64 | message age threshold; default 7 days (604_800_000 ms) |
| retention_bytes | int64 | per-partition byte cap; default 1 GiB. -1 means unlimited |
| created_at_ms | int64 | server-assigned at creation time |

### 4.2 Partition metadata

| Field | Type | Notes |
|-------|------|-------|
| topic | string | |
| partition_id | int32 | 0-indexed |
| replica_node_ids | []uint64 | size == replication_factor |
| leader_node_id | uint64 | one of replica_node_ids; managed by Raft, reflected in metadata for clients |
| leader_epoch | int64 | incremented on leader change |

### 4.3 Message / record

| Field | Type | Notes |
|-------|------|-------|
| key | bytes | optional; nil allowed |
| value | bytes | required |
| headers | map<string, bytes> | optional |
| timestamp_ms | int64 | client-supplied or server-stamped on produce; choose at API level |
| offset | int64 | server-assigned, returned in produce response |

### 4.4 Batch (on-disk and on-wire format)

A batch is the unit of write. The format must be identical on disk and over the wire (so future zero-copy is feasible). Compression is **not** supported in v1; the `attributes` field is reserved for future use and must always be zero.

```
batch_header:
  base_offset      : int64
  batch_length     : int32   # bytes of the entire batch including header
  record_count     : int32
  crc32            : uint32  # over records
  attributes       : int16   # reserved; must be 0 in v1
  base_timestamp   : int64
  max_timestamp    : int64
records: [record]+

record:
  length           : varint
  attributes       : int8
  timestamp_delta  : varint
  offset_delta     : varint
  key_length       : varint
  key              : bytes
  value_length     : varint
  value            : bytes
  headers_count    : varint
  headers          : [header]*

header:
  key_length       : varint
  key              : utf8
  value_length     : varint
  value            : bytes
```

### 4.5 Consumer group metadata

| Field | Type | Notes |
|-------|------|-------|
| group_id | string | |
| generation_id | int32 | bumped on every rebalance |
| members | map<member_id, member_info> | member_info: client host, subscribed topics, assigned partitions |
| committed_offsets | map<(topic, partition_id), int64> | |

## 5. Constraints and limits

| Constraint | Default value | Notes |
|------------|---------------|-------|
| Maximum message size | 1 MiB | rejected with InvalidMessageSize |
| Maximum batch size | 4 MiB | |
| Maximum topics per cluster | 1000 | soft limit |
| Maximum partitions per topic | 100 | soft limit |
| Maximum consumer groups | 100 | soft limit |
| Max in-flight produce requests per producer | 5 | client-side concern |
| Default segment size | 128 MiB | when this is reached, segment is rolled |
| Default index entries per segment | configurable; sample one per ~4 KiB of log | |

## 6. Non-functional requirements

- **6.1 Latency.** Under normal load on recommended hardware (per ТЗ), p95 latency for produce and fetch operations < 200ms.
- **6.2 Throughput.** No specific target; aim for "tens of thousands of messages per second per partition" as a demo-quality result.
- **6.3 Availability.** With replication factor 3 and a 3-node cluster, the system continues to serve all partitions when any single node fails.
- **6.4 Durability.** With acks=all, no acknowledged write is lost as long as a Raft quorum survives.
- **6.5 Recovery.** Node restart leads to full participation in all partitions it owned within 30 seconds for a typical-sized commit log.
- **6.6 Operating system.** Linux only.
- **6.7 Cluster size.** Minimum 3 nodes for a useful deployment. Single-node mode is supported for development but offers no fault tolerance.

## 7. Operational requirements

- **7.1 Configuration.** Each node accepts a configuration file (YAML or TOML) specifying: node ID, listen addresses (Data API, Management API, Raft), peer node addresses, data directory, log level. CLI flags override file values.
- **7.2 Metrics.** Server exposes Prometheus-format metrics over an HTTP endpoint, including: messages produced/consumed per second per partition, bytes per second, fetch latency histogram, produce latency histogram, replication state per partition, consumer group lag.
- **7.3 Logging.** Structured logging (JSON or logfmt) with configurable level. Logs written to stdout.
- **7.4 Deployment.** Distributed as a Docker container image. Cluster of 3 nodes deployable via docker-compose.
- **7.5 Profiling.** A `net/http/pprof` endpoint is exposed when the node is started with the `--pprof` flag (or equivalent config setting). Disabled by default. Bound to a separate, non-public address.

## 8. Security requirements

- **8.1 Authentication.** Token-based authentication. Each cluster has a list of valid tokens configured at startup (via configuration file). Clients include their token in gRPC request metadata under a fixed key (e.g. `bunnymq-auth-token`). Requests without a valid token are rejected with Unauthenticated.
   - PLAINTEXT mode (no auth) is supported by leaving the token list empty. PLAINTEXT is the default for local development and demo.
   - Tokens are opaque strings; no expiration, no revocation, no rotation in v1. Changing the token list requires restarting the node.
- **8.2 Authorization.** Out of scope for v1. Any authenticated client has full access to all operations. ACLs are not implemented.
- **8.3 Encryption in transit.** TLS support is optional in v1. Plain gRPC is acceptable for the course project demo. If TLS is enabled via configuration, certificates are loaded from disk; mutual TLS is not supported.

## 9. Out of scope (explicit non-goals)

The following are intentionally NOT in scope for v1 and must not be implemented unless explicitly added to this document:

- Idempotent producers
- Transactional producers
- Exactly-once delivery semantics
- Log compaction (Kafka feature: keeping only the latest record per key)
- Tiered storage / S3 offload
- Schema registry
- Connectors / Kafka Connect equivalent
- Streams API / KStreams equivalent
- ACLs and fine-grained authorization
- Mutual TLS authentication
- Token expiration, revocation, rotation
- Quotas (per-client rate limiting)
- Multi-tenant isolation
- Cross-cluster replication (MirrorMaker equivalent)
- Dynamic cluster membership changes (adding/removing nodes at runtime)
- Decreasing partition count
- Compression of message batches
- Sticky or cooperative consumer group rebalancing

## 10. Decisions deferred to design phase

The following are intentionally not specified here. Designers will propose; the user will approve:

- Concrete gRPC service definitions and method signatures.
- Internal Go module layout.
- Snapshot strategy for partition FSM (full vs. incremental).
- Concrete metric names and labels.
- Exact retention enforcement loop frequency.
- Recovery procedure for partial segment writes.
