# T-057: Group integration test — join, assignment, and voluntary rebalance

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Add two integration tests to `internal/integration/cluster_test.go` that exercise consumer group join, range-based partition assignment, and voluntary rebalance triggered by a member leaving.

## Context

M4's milestone DoD requires an end-to-end test showing that a group with multiple consumers splits partitions via range assignment and that a voluntary `LeaveGroup` triggers a rebalance giving remaining members full coverage. These tests run against real 3-broker processes (same helpers as T-047), build on the full stack (ClusterCoordinator + GroupCoordinator + DataService + client library), and require the `integration` build tag.

References:
- [08-consumer-groups.md §6 — Range assignment algorithm](../../design/08-consumer-groups.md#6-range-based-partition-assignment)
- [08-consumer-groups.md §11 — Rebalance flow](../../design/08-consumer-groups.md#11-rebalance-flow)
- [CLAUDE.md — M4 milestone DoD](../../CLAUDE.md#m4--consumer-groups)

## Scope

- Add to `internal/integration/cluster_test.go` (build tag `integration`):
  - **`TestGroup_TwoConsumers_RangeAssignment`:**
    1. Start 3 brokers (reuse `startBroker` from T-047).
    2. `AdminClient.CreateTopic("group-topic", partitionCount=4, rf=3)`.
    3. `waitPartitionsLeaders(t, adminAddr, "group-topic", 4, 15s)`.
    4. Produce 40 batches (10 per partition, round-robin across 4 partitions, `acks=all`).
    5. Create `Consumer1` with `GroupID="test-group"`, `AutoOffsetReset=EARLIEST`; `Subscribe(["group-topic"])`.
    6. Create `Consumer2` with same `GroupID`; `Subscribe(["group-topic"])` — triggers rebalance; each consumer now holds 2 partitions.
    7. `Consumer1.Poll(ctx, 3000)` — collect records; `Consumer2.Poll(ctx, 3000)` — collect records.
    8. Assert: union of records from both consumers covers all 40 batches; no record appears in both consumers' results (no overlap).
    9. Assert: each consumer has exactly 2 partitions assigned (from `assignedPartitions` field or via `DescribeTopic` leader map).
  - **`TestGroup_VoluntaryLeave_TriggersRebalance`:**
    1. 3 brokers, topic "leave-topic" with 3 partitions RF=3.
    2. Produce 30 batches.
    3. Start `Consumer1`, `Consumer2`, `Consumer3`, all in `GroupID="leave-group"`.
    4. Wait for assignment: each consumer holds 1 partition.
    5. `Consumer3.Close()` — triggers `LeaveGroup`; remaining consumers detect rebalance on next heartbeat.
    6. Wait up to 15 s for `Consumer1` and `Consumer2` to detect rebalance and re-join (poll `assignedPartitions` or watch heartbeat `rebalance_required`).
    7. After rebalance: assert `Consumer1` + `Consumer2` together cover all 3 partitions.
    8. Produce 10 more batches; both consumers poll and receive the new batches with no gaps.

## Out of scope

- Session timeout eviction — T-058.
- Offset commit survival across restart — T-058.

## Definition of done

- [ ] `go test ./internal/integration/... -tags integration -timeout 180s` passes.
- [ ] `TestGroup_TwoConsumers_RangeAssignment`: 40 pre-produced batches fully consumed across 2 consumers with no overlap or gap.
- [ ] `TestGroup_TwoConsumers_RangeAssignment`: each consumer assigned exactly 2 of 4 partitions.
- [ ] `TestGroup_VoluntaryLeave_TriggersRebalance`: after `Consumer3.Close()`, remaining 2 consumers cover all 3 partitions within 15 s.
- [ ] `TestGroup_VoluntaryLeave_TriggersRebalance`: 10 post-rebalance batches received correctly.

## Tests required

- `TestGroup_TwoConsumers_RangeAssignment` (see Scope).
- `TestGroup_VoluntaryLeave_TriggersRebalance` (see Scope).

## Dependencies

- T-047 (startBroker, waitClusterReady, waitPartitionsLeaders helpers).
- T-051 (GroupCoordinator JoinGroup/LeaveGroup — server must be running).
- T-052 (GroupCoordinator Heartbeat — needed for rebalance detection).
- T-054 (DataServer consumer group handlers wired up).
- T-055 (Client Consumer group-mode Subscribe, initFetchOffsets).
- T-056 (Client Consumer heartbeat goroutine and rebalance handling).

## Notes

`Consumer2.Subscribe` may race with `Consumer1`'s first poll — both join independently and each join increments `GenerationID`. After the second join, `Consumer1` will detect `rebalance_required=true` on its next heartbeat and re-join. In the test, wait up to 10 s for both consumers' `assignedPartitions` to stabilise (total = 4) before asserting distribution. The "no overlap" assertion can be verified by tracking which partitions each consumer's records come from (each `Record` carries its `PartitionID`).
