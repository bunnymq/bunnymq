# T-059: Dockerfile and docker-compose for 3-node cluster

**Milestone:** M5 — Integration and polish
**Effort:** S
**Status:** TODO

## Goal

Produce a multi-stage `Dockerfile` that builds and packages `cmd/bunnymq`, and a `docker-compose.yml` that starts a 3-node BunnyMQ cluster with all ports mapped and health checks wired.

## Context

M5 requires the cluster to run in docker-compose so that integration scenarios (T-065–T-067) can drive it programmatically. The Dockerfile must produce a minimal image (distroless or alpine) with the compiled binary; docker-compose must give each broker a stable hostname, distinct data volumes, and distinct port mappings so that the test harness can address each node independently.

References:
- [CLAUDE.md — M5 milestone DoD](../../CLAUDE.md#m5--integration-observability-and-polish)
- [09-metrics-logging.md §2.3 — Metrics HTTP endpoint (port :9090)](../../design/09-metrics-logging.md#23-http-endpoint)
- [06-api-protocol.md §2 — Port assignments (:9091 management, :9092 data)](../../design/06-api-protocol.md)

## Scope

- Create `Dockerfile` at repo root:
  - Stage 1 (`builder`): `FROM golang:1.23-alpine AS builder`; `COPY . .`; `go build -o /bunnymq ./cmd/bunnymq`.
  - Stage 2 (`runner`): `FROM gcr.io/distroless/static:nonroot`; copy `/bunnymq`; `ENTRYPOINT ["/bunnymq"]`.
  - No embedded config files; all configuration via flags/env at runtime.
- Create `docker-compose.yml` at repo root:
  - Services: `broker1`, `broker2`, `broker3`.
  - Each service: `build: .`; named volume for data dir (`/data`); environment variables for `NODE_ID`, `RAFT_ADDR`, `MGMT_ADDR`, `DATA_ADDR`, `METRICS_ADDR`, `INITIAL_MEMBERS` (peer list for bootstrap).
  - Port mappings (host → container):
    - `broker1`: `19091:9091` (mgmt), `19092:9092` (data), `19090:9090` (metrics).
    - `broker2`: `29091:9091`, `29092:9092`, `29090:9090`.
    - `broker3`: `39091:9091`, `39092:9092`, `39090:9090`.
  - Health check: `test: ["CMD", "/bunnymq", "--health-check"]` or TCP dial to management port every 5 s, 30 s timeout, 3 retries. Verify that `cmd/bunnymq` supports a `--health-check` flag that dials itself and exits 0 if ready (add this small flag to `cmd/bunnymq` scope).
  - `depends_on` not used (all 3 start simultaneously; they discover each other via `INITIAL_MEMBERS`).
  - Volumes: named `broker1-data`, `broker2-data`, `broker3-data`.
- Create `Makefile` targets (or extend existing):
  - `make docker-build` — `docker build -t bunnymq:dev .`
  - `make cluster-up` — `docker-compose up -d`
  - `make cluster-down` — `docker-compose down -v` (removes volumes for clean restart)
  - `make cluster-logs` — `docker-compose logs -f`

## Out of scope

- Integration test harness — T-064.
- Client container / test runner container — T-064.
- Metrics and pprof servers — T-062.

## Definition of done

- [ ] `docker build -t bunnymq:dev .` succeeds; image size < 50 MB.
- [ ] `docker-compose up -d` starts 3 broker containers without errors.
- [ ] All 3 brokers reach healthy status within 30 s (docker-compose `health: healthy`).
- [ ] `curl http://localhost:19091/` (or gRPC dial) reaches broker1 management port.
- [ ] `make cluster-down` stops and removes all containers and volumes cleanly.

## Tests required

N/A — no executable Go tests. Verified by `make cluster-up` + manual health-check confirmation. The integration tests in T-064–T-067 provide functional coverage.

## Dependencies

- T-010 (`cmd/bunnymq` binary — must compile and accept config flags).

## Notes

Verify that `cmd/bunnymq` accepts either a config file (JSON/YAML) or individual flags for all required parameters (`--node-id`, `--raft-addr`, `--mgmt-addr`, `--data-addr`, `--initial-members`, `--data-dir`, `--metrics-addr`). If only a config file is supported, write the config file to `t.TempDir()` in the integration test harness and mount it into the container. The `--health-check` flag should be a lightweight check: dial the management gRPC address with a short timeout and exit 0 on success, 1 on failure — this is < 20 lines and avoids a separate health-check binary.
