# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

BunnyMQ is a Kafka-like distributed message broker written in Go. It is currently in the **implementation phase** — the design phase (sessions 1–4) is complete and produced detailed design documents in `docs/design/`. The ticket breakdown and recommended implementation order live in `docs/tickets/README.md`.

## Commands

```bash
# Build
go build ./...

# Test (all)
go test ./...

# Test (single package)
go test ./internal/storage/...

# Test (single test function)
go test ./internal/storage/... -run TestSegmentRollAtSizeThreshold

# Benchmarks
go test ./benchmarks/... -bench=. -benchmem

# Lint (golangci-lint, once configured)
golangci-lint run ./...

# Proto codegen (once api/*.proto and buf.yaml exist)
buf generate
```

## Architecture

BunnyMQ uses a **multi-Raft model** via [dragonboat v4](https://github.com/lni/dragonboat): one Raft shard per partition plus one shard for cluster metadata.

**Control plane** handles cluster topology and consumer group state:
- `internal/coordinator/cluster` — sole writer to the Metadata FSM; owns topic lifecycle, partition assignment across nodes
- `internal/coordinator/group` — consumer group lifecycle: join/leave/heartbeat/rebalance, offset commit/fetch
- `internal/metadata` — `IStateMachine` (in-memory, JSON snapshots); keyed by topic/partition/node
- `internal/api/management` — gRPC server exposing topic admin and group RPCs

**Data plane** handles produce/fetch traffic:
- `internal/coordinator/data` — routes produce/fetch to the correct partition shard leader; dispatches `Propose`/`SyncPropose`
- `internal/partition` — `IOnDiskStateMachine` wrapping Storage; `Update()` is strictly deterministic (no `time.Now()`, no network I/O)
- `internal/storage` — segmented append-only log; see `docs/design/02-storage.md` for full spec
- `internal/api/data` — gRPC server exposing Produce, Fetch, and offset query RPCs

**Cross-cutting:**
- `internal/raft` — thin wrapper around dragonboat `NodeHost`; registers FSMs, provides typed Propose helpers
- `internal/auth` — token-based gRPC interceptor (PLAINTEXT mode if token list is empty)
- `pkg/client` — public `Producer`, `Consumer`, `AdminClient` with leader discovery and retry
- `pkg/proto` — generated protobuf + gRPC stubs (do not hand-edit); source `.proto` files live in `api/`

## Key architectural constraints (fixed)

- **Acks:** `acks=0` via `dragonboat.Propose`; `acks=all` via `dragonboat.SyncPropose`. `acks=1` is intentionally absent.
- **Wire format:** gRPC + Protobuf. Batch format on disk equals batch format on wire (Kafka-compatible layout).
- **Reads:** only the Raft shard leader serves fetches and produce. Clients that hit a non-leader receive `NotLeader` with the leader's address and retry directly.
- **Partition FSM determinism:** `Update()` must be free of side effects beyond calling into Storage. Retention is a Raft command (`DeleteSegmentsBefore`), not a local-only operation, so that all replicas delete the same segments.
- **Cluster membership is static** — no runtime node add/remove in v1.
- **Consumer group assignment** — range-based only; no cooperative rebalance, no sticky assignment.
- **Long-poll fetch** — implemented via a channel notification from Storage (`newDataCh`) on each `Append`; the Partition FSM broadcasts after a successful `Update`.

## Design documents

All detailed specs live in `docs/design/`. Read these before implementing a module:

| Doc | Covers |
|-----|--------|
| `00-overview.md` | Architecture diagram, module map |
| `01-modules.md` | Package layout (`internal/`, `pkg/`, `cmd/`), dependency graph |
| `02-storage.md` | Storage, SegmentStorage, LogSegment, index files, batch encoding |
| `03-raft-fsm.md` | Metadata FSM commands, Partition FSM commands, snapshot strategy |
| `04-cluster-coordinator.md` | Topic lifecycle, partition assignment algorithm, replica placement |
| `05-data-coordinator.md` | Produce/fetch routing, long-poll, retention loop |
| `06-api-protocol.md` | Protobuf message definitions, error codes, interceptor chain |
| `07-client-library.md` | Producer/Consumer/AdminClient public API, leader discovery, retry |
| `08-consumer-groups.md` | Group state model, JoinGroup/Heartbeat/LeaveGroup flows, offset commit |
| `09-metrics-logging.md` | Prometheus metric catalog, structured logging conventions |

Sequence diagrams for all major flows are in `docs/design/sequence/`.

## Storage internals (most complex module — start here for M1)

```
Storage
└── []SegmentStorage          // ordered by base offset; last is active
    ├── LogSegment            // .log file (O_WRONLY|O_APPEND when active, O_RDONLY when sealed)
    ├── OffsetIndexSegment    // .index file (mmap'd); 8 bytes/entry: 4B relative offset + 4B file position
    └── TimeIndexSegment      // .timeindex file (mmap'd); 12 bytes/entry: 8B timestamp + 4B relative offset
```

- Index entries are **sparse** (written at a configurable byte sampling interval, not per-batch).
- Read path: binary-search index → seek log file → linear scan to first matching batch → return up to `maxBytes`.
- On crash recovery, the active segment is truncated to the last complete batch before loading.

## Ticket workflow

Each ticket maps to one PR. Use the `/ticket <number>` skill to implement a ticket end-to-end:

```text
/ticket 015          # implements T-015, opens a PR
/ticket 047          # implements T-047, opens a PR
```

The skill will: find the ticket file → read design docs → check dependencies → create branch `ticket/T-NNN-slug` → implement + write tests → verify DoD → commit → open PR.

**Before starting any ticket:**

1. Confirm its dependencies (listed in the ticket's Dependencies section) are merged to `main`. If not, implement those first.
2. For M0 VERIFY tickets (T-001–T-006): these are research tasks. Their output is a short written note in the ticket file. They do not produce Go code.

**Branch naming:** `ticket/T-NNN-short-slug` (derived from the ticket filename).

**Commit message format:**

```text
T-NNN: <ticket title>

<one-sentence summary>

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
```

## Coding standards

### Go conventions

- No comments that describe *what* the code does — use them only for non-obvious *why* (hidden constraint, subtle invariant, workaround for a specific bug).
- No global loggers. Every module receives a `*zap.Logger` via its constructor. In tests use `zap.NewNop()`.
- No global Prometheus registry. Every module receives a `prometheus.Registerer` and registers its own metrics. In tests use an isolated `prometheus.NewRegistry()` or the `NoopXxxMetrics()` helper.
- Package names follow `01-modules.md`: `internal/storage`, `internal/cluster`, `internal/data`, `internal/api/management`, `internal/api/data`, `pkg/client`, etc.
- Integration tests carry `//go:build integration` at the top; docker-backed tests carry `//go:build integration,docker`.

### FSM rules (critical)

- `PartitionFSM.Update()` must be **strictly deterministic**: no `time.Now()`, no network I/O, no randomness. Only Storage calls are allowed.
- Retention is triggered by a Raft command (`DeleteSegmentsBefore`) so all replicas delete identical segments. Do not call storage retention from a local timer inside the FSM.
- `MetadataFSM.Update()` stores pre-computed assignments sent in the command payload — it does not call `DescribeTopic` or any external service.

### Error handling

- Validate at system boundaries (user input, gRPC ingress, proto deserialization). Trust internal invariants.
- Map domain errors to gRPC status codes in the API layer, not deeper. Domain packages return typed sentinel errors (`ErrNotFound`, `ErrStaleGeneration`, etc.).
- `SyncPropose` failures that indicate a leader change return `ErrNotLeader`; map to `FAILED_PRECONDITION` with `BunnyErrorDetail{NOT_LEADER}`.

### Tests

- Every ticket's **Tests required** section lists exact test function names — implement all of them.
- Use `zaptest/observer` (`go.uber.org/zap/zaptest/observer`) to assert on log output, not stdout capture.
- Use table-driven tests for codec round-trips and error-mapping checks.
- Stub `nodeHostIface` / `GroupCoordinatorIface` / `DataCoordinatorIface` in unit tests; use real processes only in `integration`-tagged tests.

## VERIFY items (resolve in M0 before implementing M1+)

Five dragonboat v4 API questions must be answered before the first Raft code is written. Each has its own ticket (T-001–T-006). Resolution is a short written note; no Go code required.

| Ticket | Question |
| ------ | -------- |
| T-001 | `IOnDiskStateMachine.SaveSnapshot` goroutine relationship with `Update` |
| T-002 | `GetLeaderID(shardID)` signature and `valid=false` semantics during election |
| T-003 | `StartCluster(join=false)` vs `join=true` for multi-node bootstrap |
| T-005 | `IStateMachine.Lookup` thread-safety with concurrent `Update` |
| T-006 | Propose error codes on leader change (`ErrNotLeader` vs gRPC status) |

These answers affect T-024, T-030, T-031, T-039, T-040, T-052, and T-054. Getting them wrong early means rework in multiple later tickets.
