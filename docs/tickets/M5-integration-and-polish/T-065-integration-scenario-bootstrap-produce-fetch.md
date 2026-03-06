# T-065: Integration scenario — cluster bootstrap and produce/fetch

**Milestone:** M5 — Integration and polish
**Effort:** M
**Status:** TODO

## Goal

Add two docker-compose-backed integration tests that verify: (1) a fresh 3-node cluster bootstraps and reaches quorum within 30 s; (2) topic creation, produce with `acks=all`, and fetch all succeed end-to-end against the running cluster.

## Context

These are the foundational correctness scenarios for M5: if bootstrap or produce/fetch fail, all other M5 scenarios are meaningless. They exercise the full stack over real TCP against docker-mapped ports, including TLS-less plaintext auth (default), multi-replica replication, and the client library's metadata cache. They differ from T-047 (which uses `exec.Cmd` against local processes) in that they target the docker-compose cluster via the `docker` build tag.

References:
- [CLAUDE.md — M5 integration scenario: cluster bootstrap](../../CLAUDE.md#m5--integration-observability-and-polish)
- [CLAUDE.md — M5 integration scenario: produce/fetch happy path](../../CLAUDE.md#m5--integration-observability-and-polish)

## Scope

- Create `internal/integration/docker_produce_fetch_test.go` (build tags `integration,docker`):
  - **`TestDocker_ClusterBootstrap`:**
    1. Call `waitDockerClusterReady(t, 30s)` — polls `AdminClient.DescribeCluster` on `localhost:19091` until 3 nodes appear or fails.
    2. Verify `ClusterDescription.Nodes` contains exactly 3 entries with distinct `NodeID` values.
    3. Verify `DescribeCluster.MetadataLeaderNodeID != 0` (a leader elected for the metadata shard).
    4. Assert all 3 nodes report the same `MetadataLeaderNodeID` (queried in parallel from all 3 management ports).
  - **`TestDocker_ProduceFetch_AcksAll`:**
    1. `waitDockerClusterReady(t, 30s)`.
    2. `AdminClient.CreateTopic("docker-smoke", partitionCount=3, rf=3)`.
    3. `waitDockerPartitionsLeaders(t, "docker-smoke", 3, 15s)`.
    4. For each partition (0, 1, 2): produce 20 batches with `acks=all`; collect returned offsets; verify offsets are monotonically increasing from 0.
    5. For each partition: `Consumer.Seek(0)` + `Poll(5000ms)`; verify all 20 batches returned in order with correct content.
    6. Assert `bunnymq_storage_batches_appended_total` gauge on `localhost:19090/metrics` is non-zero (metrics endpoint reachable and populated).
  - **`TestDocker_ProduceFetch_AcksZero`:**
    1. `waitDockerClusterReady(t, 30s)`.
    2. `AdminClient.CreateTopic("docker-acks0", partitionCount=1, rf=3)`.
    3. Produce 10 batches with `acks=0`; all return offset == -1 (fire-and-forget).
    4. Wait 500 ms; fetch from offset 0; verify at least 8 of 10 records arrived (acks=0 permits loss).

## Out of scope

- Leader failover — T-066.
- Consumer group rebalance — T-067.

## Definition of done

- [ ] `go test ./internal/integration/... -tags integration,docker -run TestDocker_Cluster -timeout 120s` passes.
- [ ] `TestDocker_ClusterBootstrap`: 3 nodes visible; same metadata leader across all 3 nodes.
- [ ] `TestDocker_ProduceFetch_AcksAll`: 60 batches (3 × 20) produced and fetched with no gaps.
- [ ] `TestDocker_ProduceFetch_AcksAll`: `GET localhost:19090/metrics` returns 200 with `bunnymq_storage_batches_appended_total > 0`.
- [ ] `TestDocker_ProduceFetch_AcksZero`: produce succeeds (no error from client); at least 8/10 records fetchable.

## Tests required

- `TestDocker_ClusterBootstrap` (see Scope).
- `TestDocker_ProduceFetch_AcksAll` (see Scope).
- `TestDocker_ProduceFetch_AcksZero` (see Scope).

## Dependencies

- T-059 (docker-compose cluster must be running when these tests execute).
- T-064 (harness helpers: `waitDockerClusterReady`, `waitDockerPartitionsLeaders`, `brokerAddrs`).
- T-044 (Producer client — used for produce).
- T-045 (AdminClient — used for CreateTopic + DescribeCluster).
- T-046 (Consumer client — used for fetch).
- T-062 (metrics HTTP endpoint — asserted in `TestDocker_ProduceFetch_AcksAll`).

## Notes

Parse the Prometheus text format in `TestDocker_ProduceFetch_AcksAll` using `github.com/prometheus/common/expfmt` (already a transitive dependency of the Prometheus client), or simply `strings.Contains(body, "bunnymq_storage_batches_appended_total")` for a lightweight check. For the `acks=0` test, 500 ms is typically sufficient for records to be replicated; if the test is flaky in CI, increase the wait to 2 s. Do not assert all 10 records arrive with `acks=0` — the design explicitly permits loss at the cost of lower latency.
