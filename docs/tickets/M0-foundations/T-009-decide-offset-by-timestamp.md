# T-009: DECIDE — GetOffsetByTimestamp: batch base_offset vs per-record scan

**Milestone:** M0 — Foundations
**Effort:** XS
**Status:** TODO

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

- [ ] Decision documented: batch-level (`base_offset` of matching batch) or record-level (per-record scan).
- [ ] If record-level: a note added to the M3 DataCoordinator produce/fetch ticket about partial batch decode.
- [ ] Decision referenced in M3 DataAPI GetOffsets ticket.

## Tests required

N/A — decision ticket.

## Dependencies

None.

## Notes

For a course-project demo, batch-level precision is acceptable. Producers typically create small batches (1–10 records); re-reading a few extra records is O(records_per_batch) overhead, not O(partition_size). The `timestamp_delta` varint encoding makes per-record scan straightforward if chosen, but adds ~20 lines of decode logic. Recommend batch-level for v1 simplicity, with a note that record-level is the Kafka-compatible path for v2.
