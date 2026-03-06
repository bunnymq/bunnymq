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
