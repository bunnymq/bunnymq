# T-048: Leader-failover smoke test

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Write an integration test that kills the leader broker process mid-stream, verifies that a new Raft leader is elected for each affected partition shard, and confirms that produce and fetch resume correctly against the new leader after a client retry.

## Context

Leader failover is a core correctness requirement of the M3 milestone. dragonboat elects a new leader automatically within a few election RTTs (ElectionRTT × RTTMillisecond ≈ 10 × 200ms = 2s max). The client library's `NOT_LEADER` retry logic then re-routes subsequent requests. This test validates both the dragonboat election and the client-side retry path under real network conditions.

References:
- [CLAUDE.md — M3 milestone DoD: leader kill + resume](../../CLAUDE.md#m3--3-node-cluster-with-producefetch)
- [04-cluster-coordinator.md §6 — Leader epoch tracking](../../design/04-cluster-coordinator.md#6-leader-epoch-tracking)
- [07-client-library.md §6.4 — NOT_LEADER retry](../../design/07-client-library.md#64-send-flow)

## Scope

- Add to `internal/integration/cluster_test.go`:
  - **`TestCluster_LeaderFailover`:**
    - Start 3 brokers as in T-047.
    - Create topic "failover-topic" with 1 partition, RF=3.
    - Identify which broker is the partition 0 leader (from `DescribeTopic`).
    - Produce 5 batches to partition 0 with `acks=all`; verify offsets 0–4.
    - Kill the leader broker process (`cmd.Process.Kill()` or `SIGKILL`).
    - Attempt produce of batch 6 immediately; expect at most `MaxRetries` `NOT_LEADER` or `UNAVAILABLE` errors before success.
    - Verify batch 6 base_offset = 5 (sequential, no gap).
    - Produce 4 more batches (7–10); all succeed.
    - Fetch from offset 0: all 10 batches returned in order.
    - Restart the killed broker; wait for it to rejoin the cluster.
    - Verify `DescribeCluster` shows 3 nodes again within 30s.
  - **`TestCluster_LeaderFailover_FetchDuringElection`:**
    - Produce 5 batches; kill leader; immediately issue a long-poll `Fetch` (maxWaitMs=5000ms) to the surviving nodes for the next record; produce batch 6 to the new leader; verify the long-poll returns batch 6 without the client needing to reissue the request (the NOT_LEADER detection inside long-poll kicks in).

## Out of scope

- Consumer group failover — M4.
- Multi-partition leader split — M5 (covered by full integration tests).

## Definition of done

- [ ] `go test ./internal/integration/... -tags integration -timeout 180s` passes.
- [ ] After leader kill: new produce succeeds within `MaxRetries` retry attempts.
- [ ] Offsets are sequential across the failover boundary (no gap, no duplicate).
- [ ] All batches (pre- and post-failover) are fetchable in order.
- [ ] Killed broker can rejoin; `DescribeCluster` shows 3 nodes after rejoin.
- [ ] `TestCluster_LeaderFailover_FetchDuringElection`: long-poll consumer receives batch 6 after leader change.

## Tests required

- `TestCluster_LeaderFailover` (see Scope).
- `TestCluster_LeaderFailover_FetchDuringElection` (see Scope).

## Dependencies

T-047 (cluster test helpers: `startBroker`, `waitClusterReady`, `waitPartitionsLeaders`).
T-039, T-040 (ClusterCoordinator leader sweep — critical for clients to discover new leader).
T-044 (Producer retry logic).
T-046 (Consumer fetch + NOT_LEADER handling).

## Notes

`SIGKILL` immediately terminates the process without a graceful dragonboat shutdown. This simulates a crash (not a clean shutdown), which is the more realistic and more stressful scenario. After the kill, dragonboat's remaining two nodes will elect a new leader within `ElectionRTT × 10 × RTTMillisecond` = approximately 2s. The test should wait up to 10s for the new leader to appear in `DescribeTopic.LeaderNodeID`. The `MaxRetries=3` default retry policy gives the client 3 attempts during the election window; increase `MaxRetries` to 10 in the test's `ProducerConfig` to account for the election window in CI environments with scheduling jitter. The broker rejoin test (restart and re-register) verifies that dragonboat correctly replays the partition log on the restarted node — an implicit check of the `PartitionFSM.Open()` crash recovery path.
