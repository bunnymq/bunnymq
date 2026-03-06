# T-047: Multi-process 3-node cluster smoke test

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Write a test (or test script) that starts three `cmd/bunnymq` broker processes on localhost with distinct ports and data directories, creates a topic with RF=3 and 3 partitions using the client `AdminClient`, produces 100 batches with `acks=all`, and verifies all 100 batches are fetchable from the leader of each partition.

## Context

This is the first end-to-end test that exercises the full stack: dragonboat multi-node Raft, ClusterCoordinator, DataCoordinator, gRPC APIs, and the client library — all running as real OS processes communicating over loopback TCP. It validates that M3's milestone DoD "3-node cluster serves produce/fetch" is met before M4 work begins.

References:
- [CLAUDE.md — M3 milestone DoD](../../CLAUDE.md#m3--3-node-cluster-with-producefetch)
- [04-cluster-coordinator.md §8 — Bootstrap](../../design/04-cluster-coordinator.md#8-bootstrap-behavior)
- [05-data-coordinator.md §5–6 — Produce/Fetch flows](../../design/05-data-coordinator.md)

## Scope

- Create `internal/integration/cluster_test.go` (Go integration test) or `scripts/smoke_test.go`:
  - Helper `startBroker(t *testing.T, nodeID uint64, port int, dataDir string, peers map[uint64]string) *exec.Cmd` — runs `cmd/bunnymq` with config flags; waits for health check or gRPC to be ready (poll `DescribeCluster` until 3 nodes registered, max 30s).
  - **`TestCluster_ProduceFetch`:**
    - Start 3 brokers on ports 19091/19092, 29091/29092, 39091/39092 with `t.TempDir()` data dirs.
    - Wait for cluster readiness (3 nodes in `DescribeCluster`).
    - `AdminClient.CreateTopic("smoke-topic", partitionCount=3, rf=3)`.
    - Wait for partition shards ready (poll `DescribeTopic` until all 3 partitions have `LeaderNodeID != 0`, max 15s).
    - For each partition (0, 1, 2): `Producer.SendBatch` × 10 batches with `acks=all`; record base offsets.
    - For each partition: `Consumer.Seek(offset=0)` + `Poll`; verify all 10 batches' records are returned with correct offsets and content.
  - Helper `waitClusterReady(t *testing.T, adminAddr string, expectedNodes int, timeout time.Duration)`.
  - Helper `waitPartitionsLeaders(t *testing.T, adminAddr string, topic string, partitionCount int, timeout time.Duration)`.

## Out of scope

- Leader-failover test — T-048.
- Consumer group smoke test — M4.

## Definition of done

- [ ] `go test ./internal/integration/... -tags integration -timeout 120s` passes.
- [ ] 3 broker processes start and reach quorum within 30 s.
- [ ] Topic created with RF=3; all 3 partitions have a leader node.
- [ ] 10 batches per partition produced with `acks=all`; offsets returned are monotonically increasing.
- [ ] All 30 batches (3 × 10) fetchable by consumer; content matches produced values.
- [ ] Test uses `t.TempDir()` and `t.Cleanup` for port reuse and data dir cleanup.

## Tests required

- `TestCluster_ProduceFetch` (see Scope).

## Dependencies

T-039, T-040 (ClusterCoordinator — needed to run brokers).
T-041, T-042 (DataCoordinator — needed for produce/fetch).
T-036, T-037, T-038 (gRPC servers — needed for API).
T-044, T-045, T-046 (client library — needed for test driver).
T-010 (`cmd/bunnymq` entry point — must exist for `exec.Cmd`).

## Notes

Use build tag `//go:build integration` to prevent these tests from running in unit test mode (`go test ./...`); run explicitly with `-tags integration`. Choose high-numbered ports (19091+) to reduce collision with system services. Broker config should use `localhost:PORT` for Raft addresses and a JSON/YAML config file written to `t.TempDir()`. The `waitClusterReady` helper polls `AdminClient.DescribeCluster` every 200ms; the broker readiness check can also be a simple TCP dial to the management port. Keep the test self-contained — no docker, no external dependencies beyond the compiled `cmd/bunnymq` binary.
