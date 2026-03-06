# T-058: Group integration test — session timeout eviction and offset persistence

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Add two integration tests that verify: (1) a consumer that stops heartbeating is evicted after `session_timeout_ms` and remaining members rebalance; (2) committed offsets survive a consumer restart and the consumer resumes from the committed position.

## Context

Session timeout eviction and offset persistence are the two correctness properties that most directly affect data integrity in consumer groups. A consumer that crashes without sending `LeaveGroup` must be evicted by the server-side sweep (T-052) so other consumers can take over its partitions. Committed offsets must survive across restarts so a restarted consumer does not re-read already-processed records.

References:
- [08-consumer-groups.md §9 — Session timeout enforcement](../../design/08-consumer-groups.md#9-session-timeout-enforcement)
- [08-consumer-groups.md §12 — Offset commit flow](../../design/08-consumer-groups.md#12-offset-commit-flow)
- [08-consumer-groups.md §16 — Failure scenarios](../../design/08-consumer-groups.md#16-failure-scenarios)
- [CLAUDE.md — M4 milestone DoD](../../CLAUDE.md#m4--consumer-groups)

## Scope

- Add to `internal/integration/cluster_test.go` (build tag `integration`):
  - **`TestGroup_SessionTimeout_EvictsMember`:**
    1. Start 3 brokers.
    2. Topic "timeout-topic", 2 partitions, RF=3.
    3. Produce 20 batches (10 per partition).
    4. Create `Consumer1` and `Consumer2` with `GroupID="timeout-group"`, `SessionTimeoutMs=3000` (3 s), `HeartbeatIntervalMs=1000`.
    5. Both `Subscribe`; verify each gets 1 partition.
    6. Kill `Consumer2`'s heartbeat goroutine by closing its context (simulate crash: call `consumer2.pool.Close()` directly or stop its underlying goroutines without `LeaveGroup`). Alternatively: start Consumer2 as a sub-process and kill the process.
    7. Wait up to `SessionTimeoutMs * 2 + SweepInterval` = ~13 s for Consumer1 to detect rebalance via heartbeat.
    8. After rebalance: `Consumer1.assignedPartitions` contains both partitions.
    9. `Consumer1.Poll(ctx, 5000)` — receives records from partition previously held by Consumer2.
    10. Assert: all 20 batches eventually fetched by Consumer1 (from offset 0, since Consumer2 committed nothing).
  - **`TestGroup_OffsetCommit_SurvivesRestart`:**
    1. 3 brokers, topic "offset-topic", 1 partition, RF=3.
    2. Produce 20 batches (offsets 0–19).
    3. `Consumer1` with `GroupID="offset-group"`, `AutoOffsetReset=EARLIEST`; `Subscribe`.
    4. `Consumer1.Poll(ctx, 3000)` — receives first 10 records (offsets 0–9).
    5. `Consumer1.CommitOffsets(ctx, {TP{topic, 0}: 10})` — commit offset 10 (next-to-read).
    6. `Consumer1.Close()` — sends `LeaveGroup`.
    7. Create `Consumer2` with same `GroupID` and `MemberID` (or empty — will re-join), `AutoOffsetReset=EARLIEST`; `Subscribe`.
    8. `initFetchOffsets` must fetch committed offset 10 and seek there.
    9. `Consumer2.Poll(ctx, 3000)` — receives records starting at offset 10 (not 0).
    10. Assert: `Consumer2` records contain offsets 10–19 only; offsets 0–9 not re-delivered.

## Out of scope

- Voluntary leave rebalance — T-057.
- Multi-consumer range assignment tests — T-057.

## Definition of done

- [ ] `go test ./internal/integration/... -tags integration -timeout 240s` passes.
- [ ] `TestGroup_SessionTimeout_EvictsMember`: Consumer1 detects rebalance within `2×SessionTimeoutMs + SweepInterval` (≤ 13 s with 3 s timeout + 5 s sweep).
- [ ] `TestGroup_SessionTimeout_EvictsMember`: after eviction, Consumer1 holds both partitions and fetches all records including those from Consumer2's former partition.
- [ ] `TestGroup_OffsetCommit_SurvivesRestart`: Consumer2 starts polling from offset 10, not 0.
- [ ] `TestGroup_OffsetCommit_SurvivesRestart`: offsets 0–9 not re-delivered to Consumer2.

## Tests required

- `TestGroup_SessionTimeout_EvictsMember` (see Scope).
- `TestGroup_OffsetCommit_SurvivesRestart` (see Scope).

## Dependencies

- T-047 (startBroker, waitClusterReady helpers).
- T-052 (GroupCoordinator session timeout sweep — must be running on server).
- T-053 (GroupCoordinator CommitOffset / FetchCommittedOffsets).
- T-054 (DataServer consumer group handlers).
- T-055 (Client Consumer group-mode Subscribe, CommitOffsets, initFetchOffsets).
- T-056 (Client Consumer heartbeat loop — needed to detect eviction rebalance in session timeout test).

## Notes

For `TestGroup_SessionTimeout_EvictsMember`: use `SessionTimeoutMs=3000` and `SweepIntervalMs=5000` in the broker config so the eviction happens within a predictable window. The test must set the broker's sweep interval explicitly via a config flag (verify that `cmd/bunnymq` accepts `--group-sweep-interval-ms` or similar). To simulate a crash without `LeaveGroup`, the simplest approach is to stop Consumer2's heartbeat by cancelling its internal context (if exposed as a test hook) or by replacing it with a no-op transport at the gRPC level. If the test infrastructure does not support this, start Consumer2 as a subprocess and `SIGKILL` it — same pattern as T-048. For `TestGroup_OffsetCommit_SurvivesRestart`: the committed offset is `10` (the next offset to read), so Consumer2 should seek to 10, not 9.
