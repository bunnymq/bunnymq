# T-064: Integration test harness and Makefile targets

**Milestone:** M5 — Integration and polish
**Effort:** S
**Status:** TODO

## Goal

Create a `make integration-test` target that brings up the 3-node docker-compose cluster, waits for it to be healthy, runs all M5 integration test scenarios (T-065–T-067) in sequence, and tears the cluster down cleanly regardless of test outcome.

## Context

The M5 integration scenarios (T-065–T-067) require a running 3-node cluster. Rather than duplicating cluster lifecycle code in each test, a shared harness manages docker-compose up/down, cluster health polling, and test-binary invocation. The harness can be a Makefile recipe (shell) or a Go test binary with `TestMain`; this ticket specifies the Makefile approach since it composes naturally with CI pipelines.

References:
- [CLAUDE.md — M5 milestone DoD: `make integration-test`](../../CLAUDE.md#m5--integration-observability-and-polish)

## Scope

- Extend `Makefile` with the following targets:
  - `make integration-test`:
    ```
    docker-compose up -d --build
    ./scripts/wait-healthy.sh 60
    go test ./internal/integration/... -tags integration,docker -timeout 300s -v
    docker-compose down -v
    ```
    Uses `trap` to ensure `docker-compose down -v` runs even if go test fails.
  - `make cluster-up`: `docker-compose up -d --build`
  - `make cluster-down`: `docker-compose down -v`
  - `make cluster-logs`: `docker-compose logs -f`
  - `make integration-test-local`: same as `integration-test` but with `-tags integration` only (uses `startBroker` from T-047 instead of docker-compose — runs the existing M3/M4 integration tests locally without docker).
- Create `scripts/wait-healthy.sh`:
  - Polls `docker-compose ps` until all 3 services are `healthy` or the timeout (arg 1, in seconds) expires.
  - On timeout: print container logs; exit 1.
- Add build tag `docker` (alongside `integration`) to distinguish docker-compose-backed tests (T-065–T-067) from process-backed tests (T-047, T-057, T-058).
- Create `internal/integration/docker_helpers_test.go` (build tags `integration,docker`):
  - `brokerAddrs() (mgmt []string, data []string)` — returns hardcoded `["localhost:19091","localhost:29091","localhost:39091"]` and data ports.
  - `waitDockerClusterReady(t *testing.T, timeout time.Duration)` — polls `AdminClient.DescribeCluster` on broker1 until 3 nodes visible or timeout; calls `t.Fatal` on timeout.
  - `waitDockerPartitionsLeaders(t *testing.T, topic string, count int, timeout time.Duration)` — same as `waitPartitionsLeaders` from T-047 but uses docker-mapped ports.

## Out of scope

- Integration test scenarios — T-065, T-066, T-067.
- Dockerfile and docker-compose — T-059.

## Definition of done

- [ ] `make integration-test` completes without error on a machine with Docker and docker-compose installed.
- [ ] If `go test` fails, `docker-compose down -v` still runs (trap works).
- [ ] `scripts/wait-healthy.sh 60` exits 0 when cluster is healthy within 60 s.
- [ ] `scripts/wait-healthy.sh 5` exits 1 when cluster is not healthy (e.g., if invoked before `cluster-up`).
- [ ] `make cluster-down` removes all volumes (verified by: `cluster-up`, `cluster-down`, `cluster-up` produces empty state).

## Tests required

N/A — this ticket produces infrastructure, not testable Go code. The tests in T-065–T-067 validate that the harness works end-to-end.

## Dependencies

- T-059 (Dockerfile + docker-compose — must exist for `make integration-test` to bring up cluster).
- T-047 (integration test package already exists — this ticket adds a new file to it).

## Notes

The `trap` pattern in Makefile requires `.ONESHELL` to be set, or the commands must be written as a single shell script. Use a wrapper script `scripts/run-integration-tests.sh` if `ONESHELL` complicates the Makefile structure. The docker build tag is separate from `integration` so that CI environments without Docker can still run the M3/M4 process-based integration tests (`-tags integration`) without attempting to dial docker-mapped ports.
