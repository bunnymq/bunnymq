# T-066: Integration scenario — leader kill failover and full cluster restart

**Milestone:** M5 — Integration and polish
**Effort:** M
**Status:** TODO

## Goal

Add two docker-compose-backed integration tests that verify: (1) killing the leader broker container mid-stream causes a new leader election and produce/fetch resumes after client retry; (2) stopping all three broker containers and restarting them recovers the full cluster state (topics + records) from durable storage.

## Context

Leader failover and crash recovery are the two most critical correctness properties of the distributed system. The docker-compose scenario tests them against real containers (not process-level kills as in T-048), which exercises the container networking stack and docker health-check restart policies. The restart test verifies that all three FSMs — Metadata, Partition, and Storage — recover correctly from their on-disk state.

References:
- [CLAUDE.md — M5 integration scenario: leader-kill failover](../../CLAUDE.md#m5--integration-observability-and-polish)
- [CLAUDE.md — M5 integration scenario: restart + recovery](../../CLAUDE.md#m5--integration-observability-and-polish)
- [04-cluster-coordinator.md §8 — Bootstrap behavior](../../design/04-cluster-coordinator.md#8-bootstrap-behavior)

## Scope

- Create `internal/integration/docker_failover_test.go` (build tags `integration,docker`):
  - **`TestDocker_LeaderFailover`:**
    1. `waitDockerClusterReady(t, 30s)`.
    2. `AdminClient.CreateTopic("failover-docker", partitionCount=1, rf=3)`.
    3. `waitDockerPartitionsLeaders(t, "failover-docker", 1, 15s)`.
    4. Produce 5 batches to partition 0, `acks=all`; verify offsets 0–4.
    5. Identify the leader broker container name from `DescribeTopic.LeaderNodeID`; map node ID to container name (node1/node2/node3 via known mapping).
    6. `docker-compose stop <leader-container>` (via `exec.Command("docker-compose", "stop", name)`).
    7. Retry produce of batch 6 with `MaxRetries=10`; expect success within retry window.
    8. Verify batch 6 `base_offset == 5` (no gap).
    9. Produce 4 more batches; all succeed.
    10. Fetch from offset 0: all 10 batches returned in order.
    11. `docker-compose start <leader-container>`; wait for it to rejoin (`waitDockerClusterReady` with 3 nodes, 30 s).
    12. `DescribeCluster` shows 3 nodes.
  - **`TestDocker_FullClusterRestart`:**
    1. `waitDockerClusterReady(t, 30s)`.
    2. `AdminClient.CreateTopic("persist-topic", partitionCount=2, rf=3)`.
    3. Produce 20 batches to each partition (40 total), `acks=all`.
    4. `docker-compose stop broker1 broker2 broker3` (all 3).
    5. Wait 2 s; `docker-compose start broker1 broker2 broker3`.
    6. `waitDockerClusterReady(t, 60s)` — allow longer for log replay.
    7. `AdminClient.ListTopics()` — "persist-topic" present.
    8. `waitDockerPartitionsLeaders(t, "persist-topic", 2, 15s)`.
    9. For each partition: Fetch from offset 0; all 20 batches returned with correct content.
    10. Produce 5 more batches to each partition; succeed (no offset gap after restart).

## Out of scope

- Consumer group rebalance — T-067.
- Bootstrap produce/fetch (already T-065).

## Definition of done

- [ ] `go test ./internal/integration/... -tags integration,docker -run TestDocker_Leader -timeout 180s` passes.
- [ ] `TestDocker_LeaderFailover`: batch 6 succeeds within `MaxRetries=10`; offset = 5.
- [ ] `TestDocker_LeaderFailover`: all 10 batches fetchable after failover.
- [ ] `TestDocker_LeaderFailover`: killed broker rejoins; `DescribeCluster` shows 3 nodes.
- [ ] `TestDocker_FullClusterRestart`: topic survives restart; all 40 pre-restart batches fetchable.
- [ ] `TestDocker_FullClusterRestart`: post-restart produces succeed with sequential offsets.

## Tests required

- `TestDocker_LeaderFailover` (see Scope).
- `TestDocker_FullClusterRestart` (see Scope).

## Dependencies

- T-059 (docker-compose cluster).
- T-064 (harness helpers: `waitDockerClusterReady`, `waitDockerPartitionsLeaders`).
- T-044 (Producer — retry on NOT_LEADER).
- T-045 (AdminClient — DescribeCluster, DescribeTopic).
- T-046 (Consumer — fetch after restart).
- T-048 (leader failover logic proven at process level; this tests it at container level).

## Notes

Use `exec.Command("docker", "compose", "stop", containerName)` (Docker Compose v2 syntax) or `exec.Command("docker-compose", "stop", containerName)` (v1). Detect which is available at test setup. For `TestDocker_FullClusterRestart`, stopping all 3 containers simultaneously simulates a complete outage; the 60 s `waitDockerClusterReady` timeout accounts for log replay on all three nodes. Data volumes are persistent across `docker-compose stop/start` (volumes are only removed on `docker-compose down -v`), which is why the topic and records survive. Ensure that `docker-compose.yml` uses `restart: "no"` for the integration test setup (not `restart: always`) so stopped containers stay stopped until explicitly restarted.
