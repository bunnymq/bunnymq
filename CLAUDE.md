# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

BunnyMQ is a Kafka-like distributed message broker written in Go. It is currently in the **implementation phase** — the design phase (sessions 1–4) is complete and produced detailed design documents in `docs/design/`. Ticket breakdown lives in `CLAUDE_pending_tickets.md`.

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
