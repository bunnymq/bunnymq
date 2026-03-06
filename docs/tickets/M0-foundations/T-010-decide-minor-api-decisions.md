# T-010: DECIDE — Minor API and protocol decisions

**Milestone:** M0 — Foundations
**Effort:** XS
**Status:** TODO

## Goal

Resolve three small open questions about the API protocol that affect clean implementation of the DataAPI and DataCoordinator in M3.

## Context

`06-api-protocol.md` raises several minor open questions that, if left unresolved, create implementation ambiguity:

1. **OQ4 — `partition_id = -1` routing:** When `ProduceRequest.partition_id = -1`, the server must select a partition by round-robin. `06-api-protocol.md OQ4` confirms DataCoordinator owns a per-topic `atomic.Int64` counter. Needs formal documentation of counter ownership, reset behavior, and where the `LookupMetadata(QueryGetTopic)` call to fetch partition count lives.

2. **§10 — Server-side per-record size validation:** `06-api-protocol.md §10` notes "Individual record size is not validated server-side in v1 (the 1 MiB limit is documented as a client-library concern)" and adds a VERIFY about whether this is safe. If a malicious or buggy client bypasses the client library and sends a single oversized record (e.g., 10 MiB), it would be stored and replicated without error.

3. **OQ5 — `request_id` metadata key:** The logging interceptor extracts `request_id` from incoming gRPC metadata. `x-request-id` is proposed as the key name (common HTTP convention also used by many gRPC frameworks).

References:
- [06-api-protocol.md §10](../../design/06-api-protocol.md#10-wire-batch-format)
- [06-api-protocol.md OQ4](../../design/06-api-protocol.md#12-open-questions)
- [06-api-protocol.md OQ5](../../design/06-api-protocol.md#12-open-questions)

## Scope

- **partition_id=-1:** Confirm DataCoordinator owns a `map[string]*atomic.Int64` per-topic round-robin counter; counter is not persisted (resets on restart); the partition count is fetched from MetadataFSM via `LookupMetadata(QueryGetTopic)`. Document this pattern for M3 DataCoordinator ticket.
- **Per-record validation:** Decide whether to add server-side per-record size validation. The batch-level 4 MiB limit is enforced; a single record up to 4 MiB - 38 bytes (header) is technically within the batch limit. A separate 1 MiB per-record cap requires iterating all records in the batch on every produce request. Decide: yes (add validation) or no (trust client library, document as known limitation).
- **request_id key:** Confirm `x-request-id` as the metadata key, or choose `bunnymq-request-id` for consistency with the existing `bunnymq-auth-token` key.

## Out of scope

- Implementing any of the above — M3 DataAPI and DataCoordinator tickets.

## Definition of done

- [ ] `partition_id=-1` routing: DataCoordinator counter ownership, reset behavior, and LookupMetadata call location documented.
- [ ] Per-record size validation: decision documented (add server-side check or defer to client).
- [ ] `request_id` metadata key name finalized.

## Tests required

N/A — decision ticket.

## Dependencies

None.

## Notes

For per-record size validation: the 4 MiB batch limit already caps damage from oversized batches (a single oversized record would consume the full batch budget). Server-side per-record validation adds latency on every produce RPC and complicates the batch validation step. Recommend: skip per-record server-side validation in v1; document as a known limitation. For `request_id` key: `bunnymq-request-id` is more consistent with the existing authentication header naming convention.

---

## Decision

### 1. `partition_id = -1` routing (OQ4)

**Decision: DataCoordinator owns the round-robin counter; counter is not persisted.**

- `DataCoordinator` maintains a `map[string]*atomic.Int64` keyed by topic name. Each entry is created lazily on the first `partition_id = -1` produce request for that topic.
- The selected partition index is `counter.Add(1) % partitionCount`. `partitionCount` is fetched from the MetadataFSM via `LookupMetadata(QueryGetTopic)` at produce time.
- The counter resets to zero on server restart. This is acceptable for a course-project load balancer: no consumer or producer protocol depends on round-robin stability, and Raft provides durability at the log level regardless of which partition receives a batch.
- Key-hash-based routing is explicitly out of scope in v1 because the routing key is embedded inside the opaque `batch_data` bytes, which would require decoding on the hot path.

Rationale:
1. Keeping the counter in DataCoordinator avoids any Raft involvement for what is purely a load-distribution hint.
2. `atomic.Int64` makes the counter safe for concurrent produce requests without a mutex.
3. Non-persistent round-robin state is consistent with Kafka's producer-side default behavior (producers maintain their own sequence number).

---

### 2. Per-record size validation (§10 VERIFY)

**Decision: No server-side per-record size validation in v1.**

The existing four-step batch validation (length ≥ 38, `batch_length` bounds, 4 MiB cap, CRC-32C) is sufficient:

- A single oversized record within a 4 MiB batch is bounded in damage: at most 4 MiB stored and replicated per produce call.
- Adding per-record validation requires iterating all records in every batch on the produce hot path — O(records_per_batch) overhead on every RPC.
- It complicates the `DataAPI` handler by coupling it to the wire record format (offset within `batch_data` where records start).
- The 1 MiB per-record limit is enforced in the client library (`pkg/client`), which is the appropriate boundary for a trusted internal cluster.

Known limitation: a buggy or malicious client that bypasses `pkg/client` can submit a record up to ~4 MiB. This is documented as a v2 improvement.

---

### 3. `request_id` metadata key (OQ5)

**Decision: Use `bunnymq-request-id` as the gRPC metadata key.**

- Consistent with the existing `bunnymq-auth-token` header convention — all BunnyMQ-specific metadata keys share the `bunnymq-` prefix.
- Clients that already use `x-request-id` (e.g., from an HTTP gateway) can add a gateway header-rewrite rule; this is a one-line mapping and does not require a protocol change.
- The logging interceptor reads `md["bunnymq-request-id"]`; if absent, generates or omits a request ID from log fields.

---

### Impact on downstream tickets

| Ticket | Impact |
|---|---|
| T-037 (Data API gRPC — Produce + GetOffsets) | Logging interceptor must read `bunnymq-request-id` from incoming metadata. |
| T-041 (DataCoordinator: produce path + offset queries) | `DataCoordinator` must hold `map[string]*atomic.Int64` round-robin counters; fetch `partitionCount` from MetadataFSM via `LookupMetadata(QueryGetTopic)` before computing `counter.Add(1) % partitionCount`. |

No amendments to immutable design documents are required.

---

### Definition of done checklist

- [x] `partition_id=-1` routing: DataCoordinator counter ownership, reset behavior, and LookupMetadata call location documented.
- [x] Per-record size validation: decision documented — deferred to client library in v1; batch-level 4 MiB cap is the server-side guard.
- [x] `request_id` metadata key name finalized: `bunnymq-request-id`.
