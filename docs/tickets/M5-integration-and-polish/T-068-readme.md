# T-068: README.md

**Milestone:** M5 — Integration and polish
**Effort:** S
**Status:** TODO

## Goal

Write `README.md` at the repository root with: a one-paragraph project description, a high-level architecture diagram, instructions for running the cluster locally (docker-compose and native), instructions for running tests (unit, integration, docker), and links to all design documents.

## Context

A README is the entry point for anyone opening the repository. After five milestones of implementation the codebase is functional; README.md makes it navigable. It is the final deliverable of the design and implementation phase.

References:
- [CLAUDE.md — M5 milestone DoD: README.md](../../CLAUDE.md#m5--integration-observability-and-polish)
- [docs/design/00-overview.md](../../design/00-overview.md)

## Scope

- Create `README.md` at repo root with these sections:

  **1. Overview** (~3 sentences): BunnyMQ is a Kafka-compatible distributed message broker written in Go. It uses dragonboat (Multi-Raft) for replication, gRPC + Protobuf for its wire protocol, and a segmented append-only log for storage.

  **2. Architecture** (mermaid diagram): node-level diagram showing 3 brokers, each with: gRPC (`:9091`/`:9092`) → ClusterCoordinator → MetadataFSM + DataCoordinator → PartitionFSM → Storage. Arrow from client to any broker. Note that group RPCs go to the metadata shard leader. Keep it simple — the detailed diagrams are in `docs/design/`.

  **3. Prerequisites**: Go 1.23+, Docker + docker-compose (for cluster mode), protoc + protoc-gen-go (for proto codegen).

  **4. Quick start — docker-compose**:
  ```bash
  make cluster-up     # starts 3-node cluster
  make cluster-logs   # follow logs
  make cluster-down   # stop and remove volumes
  ```

  **5. Quick start — native (3 processes)**:
  ```bash
  make build          # builds cmd/bunnymq
  # Start 3 terminals:
  ./bunnymq --node-id=1 --raft-addr=localhost:8001 ...
  ./bunnymq --node-id=2 --raft-addr=localhost:8002 ...
  ./bunnymq --node-id=3 --raft-addr=localhost:8003 ...
  ```

  **6. Running tests**:
  ```bash
  make test                 # unit tests
  make test-integration     # process-based integration (no docker)
  make integration-test     # docker-compose integration suite
  ```

  **7. Configuration**: brief table of the most important flags (`--node-id`, `--raft-addr`, `--mgmt-addr`, `--data-addr`, `--metrics-addr`, `--pprof-addr`, `--data-dir`, `--log-level`).

  **8. Observability**: metrics on `:9090/metrics` (Prometheus); pprof on `--pprof-addr`; structured JSON logs on stdout.

  **9. Design documents**: links to all 10 design files in `docs/design/` with one-line descriptions.

  **10. Ticket index**: link to `docs/tickets/README.md` (the master index produced separately).

## Out of scope

- Generating the master ticket index — that is `docs/tickets/README.md`, produced after all ticket files exist.
- Operator runbooks, performance tuning guides, or API reference docs.

## Definition of done

- [ ] `README.md` exists at repo root.
- [ ] All `make` commands listed in the README exist in `Makefile`.
- [ ] Architecture mermaid diagram renders correctly (verify with `mermaid.live` or GitHub preview).
- [ ] Links to all 10 design documents are valid relative paths.
- [ ] Link to `docs/tickets/README.md` is present (file exists after master index is produced).

## Tests required

N/A — documentation file; no executable tests. Verified by visual inspection and broken-link check (`find . -name "*.md" | xargs grep -oP '\(.*\.md\)' | verify-paths`).

## Dependencies

- T-059 (`make cluster-up` / `make cluster-down` targets must exist in `Makefile`).
- T-064 (`make integration-test` target must exist).
- All prior tickets (binary must build and cluster must start for the quick-start instructions to be accurate).

## Notes

Write the README after all other M5 tickets are implemented so that the quick-start commands reflect the final state of the `Makefile`. Keep the README concise — this is a project-level entry point, not a full user manual. The detailed design rationale lives in `docs/design/`; link to it rather than duplicating.
