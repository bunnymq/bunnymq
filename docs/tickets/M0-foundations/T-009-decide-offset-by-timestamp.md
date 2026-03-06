# T-009: DECIDE — GetOffsetByTimestamp: batch base_offset vs per-record scan

**Milestone:** M0 — Foundations
**Effort:** XS
**Status:** DONE

## Goal

Decide whether `GetOffsetByTimestamp` returns the `base_offset` of the first batch whose `max_timestamp >= timestampMs`, or performs a per-record scan within the matching batch to return the exact record offset at or after the given timestamp.

## Context

`05-data-coordinator.md OQ4` and `06-api-protocol.md OQ3` both ask this question. `storage.ReadByTime(timestampMs, n)` returns the first batch whose `max_timestamp >= timestampMs`. The `base_offset` of that batch is the simplest answer. However, if the target timestamp falls in the middle of a batch (e.g., the batch has 10 records spanning 100 ms, and the requested timestamp matches record 5), returning `base_offset` causes the consumer to re-read records 1–4. Kafka's `offsetsForTimes` returns the offset of the first record at or after the timestamp, which may require partial batch decoding.

References:
- [05-data-coordinator.md §7](../../design/05-data-coordinator.md#7-offset-queries)
- [05-data-coordinator.md OQ4](../../design/05-data-coordinator.md#12-open-questions)
- [06-api-protocol.md §5.3](../../design/06-api-protocol.md#53-offset-queries)
- [06-api-protocol.md OQ3](../../design/06-api-protocol.md#12-open-questions)

## Scope

- Assess whether Kafka-compatible per-record precision is required for the course project.
- Evaluate implementation cost: partial batch decoding requires reading the batch records, iterating by `timestamp_delta`, and finding the record whose `base_timestamp + timestamp_delta >= timestampMs`.
- Assess consumer-visible impact: with batches of 1–100 records, re-reading a few extra records from the start of a batch is negligible overhead.
- Make the decision.

## Out of scope

- Implementing `GetOffsetByTimestamp` — M3 DataCoordinator and DataAPI tickets.

## Definition of done

- [x] Decision documented: batch-level (`base_offset` of matching batch) or record-level (per-record scan).
- [x] If record-level: a note added to the M3 DataCoordinator produce/fetch ticket about partial batch decode. (N/A — batch-level chosen; partial batch decode deferred to v2.)
- [x] Decision referenced in M3 DataAPI GetOffsets ticket: T-037 (Data API gRPC — Produce + GetOffsets) must implement `BY_TIMESTAMP` as `base_offset` of the first batch whose `max_timestamp >= timestamp_ms`; no partial batch decode required.

## Tests required

N/A — decision ticket.

## Dependencies

None.

## Notes

For a course-project demo, batch-level precision is acceptable. Producers typically create small batches (1–10 records); re-reading a few extra records is O(records_per_batch) overhead, not O(partition_size). The `timestamp_delta` varint encoding makes per-record scan straightforward if chosen, but adds ~20 lines of decode logic. Recommend batch-level for v1 simplicity, with a note that record-level is the Kafka-compatible path for v2.

---

## Findings

_References: `05-data-coordinator.md §7`, `05-data-coordinator.md OQ4`, `06-api-protocol.md §5.3`, `06-api-protocol.md OQ3`._

---

### Kafka-compatible per-record precision — is it required?

Kafka's `offsetsForTimes` returns the offset of the first **record** at or after the requested timestamp — not the first batch. This means if a batch spans timestamps 100–200 ms and the client requests `timestampMs = 150`, Kafka returns the offset of the specific record at 150 ms, not the batch's `base_offset`. BunnyMQ v1 is a course-project demo, not a Kafka drop-in replacement. None of the design documents mandate Kafka-wire-protocol compatibility for offset semantics. Record-level precision is not required for v1.

---

### Implementation cost of per-record scan

Partial batch decoding involves:

1. Read the batch header (fixed 61 bytes) to obtain `base_timestamp`, `base_offset`, and `record_count`.
2. Iterate over variable-length records, decoding the `timestamp_delta` varint for each record until `base_timestamp + timestamp_delta >= timestampMs`.
3. Return `base_offset + offset_delta` of that record.

This adds approximately 20 lines of decode logic and requires the caller to hold a fully-buffered batch in memory. The logic is straightforward given the `timestamp_delta` varint encoding documented in `06-api-protocol.md §6`, but it constitutes a non-trivial dependency on the batch wire format inside `DataCoordinator.GetOffsetByTimestamp`. Any future change to the record encoding would require updating this scan path as well.

---

### Consumer-visible impact of batch-level precision

With batch-level semantics, a consumer that requests `GetOffsetByTimestamp(t)` may receive `base_offset` of a batch whose **first** record has timestamp slightly earlier than `t`. The consumer will re-read those earlier records on the next `Fetch` call. For typical small batches (1–10 records, each ~100–500 bytes), re-reading the extra records is O(records_per_batch) — negligible for the course-project load. The `05-data-coordinator.md §7` design already documents `base_offset` as the intended return value, confirming this is the designed behaviour.

---

### Decision

**Use batch-level semantics: return the `base_offset` of the first batch whose `max_timestamp >= timestampMs`.**

Rationale:

1. The `05-data-coordinator.md §7` and `06-api-protocol.md §5.3` design documents already specify batch-level semantics — this decision confirms and closes the open question rather than introducing a new design.
2. Kafka-compatible per-record precision is not required for a course-project demo. No consumer test case depends on sub-batch timestamp resolution.
3. Partial batch decode adds ~20 lines of code and couples `DataCoordinator` to the record wire format. Batch-level is simpler and carries zero maintenance risk at v1 scale.
4. Consumer overhead from re-reading a few extra records is O(records_per_batch), not O(partition_size) — acceptable at course-project scale.

The record-level path is the Kafka-compatible upgrade for v2 if needed. Implementing it requires only the decode loop described above; no interface changes to Storage or the FSM are required.

---

### Impact on downstream tickets

| Ticket | Impact |
|---|---|
| T-037 (Data API gRPC — Produce + GetOffsets) | Implement `BY_TIMESTAMP` as: call `DataCoordinator.GetOffsetByTimestamp`, which delegates to `storage.ReadByTime(timestampMs, smallMaxBytes)` and returns the `base_offset` field of the first batch in the result. No partial decode. |
| T-041 (DataCoordinator: produce path + offset queries) | `GetOffsetByTimestamp` decodes `base_offset` from the first returned batch header (bytes 0–7). No record-level iteration. |

No amendments to immutable design documents are required.

---

### Definition of done checklist

- [x] Decision documented: batch-level precision chosen.
- [x] If record-level: note in M3 DataCoordinator ticket. (N/A — batch-level chosen.)
- [x] Decision referenced in M3 DataAPI GetOffsets ticket: T-037 must use `base_offset` of the matching batch; no partial batch decode required.
