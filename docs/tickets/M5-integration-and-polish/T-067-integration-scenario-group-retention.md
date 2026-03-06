# T-067: Integration scenario — consumer group rebalance and retention enforcement

**Milestone:** M5 — Integration and polish
**Effort:** M
**Status:** TODO

## Goal

Add two docker-compose-backed integration tests that verify: (1) a consumer group rebalances correctly when one consumer container is killed; (2) storage retention (by size) deletes sealed segments and advances `EarliestOffset` on the broker.

## Context

Consumer group rebalance under container kill (not graceful leave) exercises the session timeout sweep path (T-052) in a realistic environment. Retention enforcement verifies that the background retention goroutine in the DataCoordinator (T-041) correctly deletes segments and updates metrics; it is tested here rather than in unit tests because it requires real time passing or a configurable low-threshold that triggers under a modest produce workload.

References:
- [CLAUDE.md — M5 integration scenario: consumer group rebalance on consumer kill](../../CLAUDE.md#m5--integration-observability-and-polish)
- [CLAUDE.md — M5 integration scenario: retention](../../CLAUDE.md#m5--integration-observability-and-polish)
- [05-data-coordinator.md §7 — Retention enforcement](../../design/05-data-coordinator.md)

## Scope

- Create `internal/integration/docker_group_retention_test.go` (build tags `integration,docker`):
  - **`TestDocker_ConsumerGroupRebalance_OnKill`:**
    1. `waitDockerClusterReady(t, 30s)`.
    2. `AdminClient.CreateTopic("docker-rebalance", partitionCount=4, rf=3)`.
    3. Produce 80 batches (20 per partition), `acks=all`.
    4. Start `Consumer1` and `Consumer2` (both `GroupID="docker-group"`, `AutoOffsetReset=EARLIEST`); `Subscribe`.
    5. Wait up to 15 s for both to have stable assignments (2 partitions each); verify by checking `assignedPartitions`.
    6. Kill `Consumer1` process (close its underlying gRPC connections without `LeaveGroup` — simulate crash by `Consumer1.pool.Close()` without calling `Consumer1.Close()`; or forcibly cancel its context).
    7. Wait up to `SessionTimeoutMs * 2 + SweepInterval` (configure brokers with `--group-session-timeout-max-ms=5000` and `--group-sweep-interval-ms=3000`) for `Consumer2` to detect rebalance via heartbeat.
    8. After rebalance: `Consumer2.assignedPartitions` should cover all 4 partitions.
    9. `Consumer2.Poll(ctx, 5000ms)` collects records from partitions formerly held by Consumer1; verifies at least some records from those partitions are received.
    10. `Consumer2.Commit(ctx)`; close.
  - **`TestDocker_RetentionBySize`:**
    1. `waitDockerClusterReady(t, 30s)`.
    2. `AdminClient.CreateTopic("retention-topic", partitionCount=1, rf=3)` with `RetentionBytes=2MB` (via `AlterTopicRetention` or at create time if supported).
    3. Produce batches totalling > 6 MB to partition 0, `acks=all` (enough to exceed retention threshold by 3×).
    4. Wait up to 30 s for retention enforcement to delete at least one segment.
    5. Poll `GET localhost:19090/metrics`; assert `bunnymq_storage_segments_deleted_total{reason="bytes"} > 0`.
    6. `Consumer.Seek(0, 0, offset=0)` → `Poll`; expect `NOT_FOUND` or empty (earliest offset has advanced past 0).
    7. Alternatively: `AdminClient.ListPartitions("retention-topic")` → verify `EarliestOffset > 0`.

## Out of scope

- Leader failover — T-066.
- Produce/fetch bootstrap — T-065.
- Retention by time — tested here only by size; time-based retention requires clock manipulation which is out of scope for v1 integration tests.

## Definition of done

- [ ] `go test ./internal/integration/... -tags integration,docker -run TestDocker_Consumer -timeout 240s` passes.
- [ ] `TestDocker_ConsumerGroupRebalance_OnKill`: Consumer2 covers all 4 partitions after Consumer1 crash within `2×SessionTimeout + SweepInterval`.
- [ ] `TestDocker_ConsumerGroupRebalance_OnKill`: Consumer2 successfully fetches records from Consumer1's former partitions.
- [ ] `TestDocker_RetentionBySize`: at least one segment deleted within 30 s; `segments_deleted_total{reason="bytes"} > 0` in metrics.
- [ ] `TestDocker_RetentionBySize`: consumer fetch from offset 0 returns empty or error (earliest offset has advanced).

## Tests required

- `TestDocker_ConsumerGroupRebalance_OnKill` (see Scope).
- `TestDocker_RetentionBySize` (see Scope).

## Dependencies

- T-059 (docker-compose cluster).
- T-064 (harness helpers).
- T-052 (GroupCoordinator session timeout sweep — must be enabled and configurable via broker flag).
- T-055, T-056 (Client Consumer group mode).
- T-060 (Storage metrics — asserted in retention test).
- T-062 (Metrics HTTP endpoint — scraped in retention test).

## Notes

For `TestDocker_ConsumerGroupRebalance_OnKill`: pass `--group-session-timeout-max-ms=5000` and `--group-sweep-interval-ms=3000` to broker processes via docker-compose environment variables so the test completes in a reasonable time (without these flags the defaults of 300 s / 5 s would make the test take 5+ minutes). For `TestDocker_RetentionBySize`: configure the topic with the smallest retention bytes that the implementation supports (2 MB recommended; adjust if `RetentionBytes` minimum is larger). Each produced batch should be large enough to fill segments quickly — produce 1 KB payloads in a tight loop until total > 6 MB. The retention goroutine in DataCoordinator runs on a fixed interval (design doc suggests every 60 s by default); configure it to run every 5 s in the test broker via a flag so the test does not time out.
