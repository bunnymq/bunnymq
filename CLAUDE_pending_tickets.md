# CLAUDE.md - BunnyMQ Design Phase (Session 4: Stage 4 - Tickets)

## Project context

BunnyMQ is a Kafka-like distributed message broker written in Go. The design phase is now in its **final session**. Sessions 1-3 produced the requirements document, the high-level overview, the module breakdown, and detailed design documents for all modules. This session breaks the implementation work into tickets that the user will work through one by one in subsequent (non-design) Claude Code sessions.

This is still **design mode**: you produce markdown files only. No `.go`, no `.proto`, no `Dockerfile`. Tickets describe work; they do not perform it.

## How to start

On every session start, before doing anything else:

1. Read this file (`CLAUDE.md`) in full.
2. Read `docs/REQUIREMENTS.md` in full.
3. Read all approved outputs from previous sessions:
   - `docs/design/00-overview.md`
   - `docs/design/01-modules.md`
   - `docs/design/02-storage.md`
   - `docs/design/03-raft-fsm.md`
   - `docs/design/04-cluster-coordinator.md`
   - `docs/design/05-data-coordinator.md`
   - `docs/design/06-api-protocol.md`
   - `docs/design/07-client-library.md`
   - `docs/design/08-consumer-groups.md`
   - `docs/design/09-metrics-logging.md`
   - All files in `docs/design/sequence/`
4. Note any unresolved `VERIFY` markers and open questions in the design documents. Aggregate them into a single list - these become tickets in milestone M0 (see below) or are flagged to the user as blockers.
5. List existing files in `docs/tickets/`. If non-empty, report to user before doing anything else - this session expects to produce all tickets fresh.
6. Confirm to the user in chat that you are starting Stage 4 and ask for go-ahead.

## Architectural decisions (fixed; not for redesign)

These are not open for discussion. The tickets you write must conform to them.

1. **Language:** Go (current stable version).
2. **Consensus and replication:** library `github.com/lni/dragonboat/v4`. Multi-raft: one shard per partition + one shard for metadata.
3. **Wire protocol:** gRPC + Protobuf for all APIs. No custom binary protocol. No `sendfile`.
4. **Acks semantics:** `acks=0` and `acks=all` only.
5. **Storage:** segmented append-only log per `02-storage.md`.
6. **Metadata FSM:** in-memory `IStateMachine` per `03-raft-fsm.md`.
7. **Partition FSM:** `IOnDiskStateMachine` over Storage per `03-raft-fsm.md`.
8. **Consumer groups:** range-based assignment, no cooperative rebalance.
9. **Authentication:** token-based via gRPC metadata, configured at startup.
10. **Observability:** Prometheus metrics + structured logging per `09-metrics-logging.md`.

## Tools and rules of engagement

### What you may do

- Read and write files in `docs/tickets/`.
- Read all files in `docs/`.
- Run shell commands for investigation: `tree`, `ls`, `grep`, `cat`, `wc`. Use them sparingly.
- Read dragonboat documentation online via web fetch when verifying that a referenced API is real.

### What you may not do

- Create or edit `.go`, `.proto`, `Dockerfile`, or any non-markdown source files.
- Modify any file in `docs/REQUIREMENTS.md`, `docs/adr/`, or `docs/design/`. They are immutable inputs.
- Override any architectural decision.
- Begin implementing any ticket. Tickets describe future work to be done in later sessions.

### Hallucination protocol - same as previous sessions, with implementation focus

Tickets describe work. Each ticket points the future implementer at specific design documents and requirements. The risk in this session is **not** inventing API surface - that work is done - but rather:

1. **Inventing a ticket scope that contradicts the design.** Before writing each ticket, re-read the relevant design section and quote it (or link to it) in the ticket.
2. **Fabricating effort estimates.** Estimates here are coarse (XS/S/M/L). Use them to flag relative size, not as commitments.
3. **Creating dependencies that do not match the design.** When in doubt about whether ticket A blocks ticket B, ask the user.
4. **Smuggling unresolved VERIFY items into ticket bodies as if they were facts.** Every VERIFY from sessions 2-3 must either: (a) become a ticket itself in M0; (b) be flagged in the affected ticket's body as a "must-resolve-during-implementation" item; or (c) be raised to the user for resolution before this session ends.

If unsure about scope, dependency, or content of a ticket, ask the user. Do not silently choose.

## Repository layout

Existing state at session start (assumed):

```
docs/
├── REQUIREMENTS.md
├── adr/
└── design/
    ├── 00-overview.md
    ├── 01-modules.md
    ├── 02-storage.md
    ├── 03-raft-fsm.md
    ├── 04-cluster-coordinator.md
    ├── 05-data-coordinator.md
    ├── 06-api-protocol.md
    ├── 07-client-library.md
    ├── 08-consumer-groups.md
    ├── 09-metrics-logging.md
    └── sequence/
        └── (28 sequence diagram files)
```

Target state at session end:

```
docs/
├── REQUIREMENTS.md
├── adr/
├── design/
└── tickets/
    ├── README.md                     (master index)
    ├── M0-foundations/
    │   └── (verify items, repo bootstrap)
    ├── M1-storage-standalone/
    │   └── (Storage works in isolation, with unit tests)
    ├── M2-raft-single-node/
    │   └── (Raft + FSMs work on a single node)
    ├── M3-cluster-produce-fetch/
    │   └── (3-node cluster with produce/fetch end-to-end)
    ├── M4-consumer-groups/
    │   └── (consumer group join/heartbeat/rebalance/commit)
    └── M5-integration-and-polish/
        └── (docker-compose, integration tests, metrics, logging polish)
```

Each ticket is a separate markdown file in the appropriate milestone directory. Filenames: `T-NNN-short-slug.md`, where `NNN` is a zero-padded sequential number across all milestones (so M0 holds T-001 onwards, M1 continues numbering, etc.).

## Milestones - definition

Each milestone has an explicit definition of "done" (DoD) at the milestone level, separate from per-ticket DoDs. The milestone DoD is what makes the milestone a meaningful checkpoint where the project could be paused or demoed.

### M0 - Foundations

**Goal:** resolve outstanding design questions, set up the repository skeleton, and establish CI/build basics so subsequent milestones don't trip on infrastructure.

**Milestone DoD:**
- All `VERIFY` markers from design phase are either resolved or accepted as known limitations.
- Repository structure follows `01-modules.md`.
- `go build ./...` succeeds (with empty packages and stub interfaces if needed).
- A basic Makefile or task script exists with targets: `build`, `test`, `lint`, `proto`.
- Linting (golangci-lint) is configured.
- Basic CI script (or local-only pre-commit) is set up.

**Typical ticket types:**
- One ticket per VERIFY item, scoped to "investigate, decide, document, optionally update design doc".
- Repository skeleton ticket.
- Proto codegen pipeline ticket.
- Build tooling ticket.

### M1 - Storage standalone

**Goal:** Storage module fully works as designed in `02-storage.md`, with comprehensive unit tests and a CLI smoke-test tool.

**Milestone DoD:**
- All public methods of `Storage` are implemented.
- Unit tests cover: append + read round-trip, segment roll, retention by time, retention by bytes, recovery from clean shutdown, recovery from torn write, index lookups, time index lookups.
- A standalone CLI tool (e.g. `cmd/storage-debug`) exists for ad-hoc testing: append from stdin, read at offset, dump segment.
- Storage works correctly when used by a single goroutine; concurrency is documented but a single-writer model is enforced.
- Test vectors exist for batch encoding (a fixed `(input → bytes)` test).

**Typical ticket types:**
- Batch encoder/decoder + test vectors.
- LogSegment append/read.
- OffsetIndexSegment with mmap.
- TimeIndexSegment with mmap.
- SegmentStorage roll/seal logic.
- Storage public API and lifecycle.
- Retention enforcement.
- Recovery on startup.
- CLI debug tool.

### M2 - Raft + FSMs on a single node

**Goal:** dragonboat NodeHost wrapper, Metadata FSM, and Partition FSM all work on a single node. The node can create a topic, append messages to a partition, and read them back - all going through Raft `Apply` → FSM → Storage. No clustering yet, but the code paths used in clustering are exercised.

**Milestone DoD:**
- NodeHost wrapper boots, runs the metadata shard, and accepts `SyncPropose` for topic creation.
- Metadata FSM correctly applies all command types from `03-raft-fsm.md`.
- Partition FSM correctly applies AppendBatch and DeleteSegmentsBefore (the latter from the M1 retention work, surfaced through Raft).
- A single-node "cluster" can: create topic → append batches → fetch batches → see them via Lookup.
- Snapshot save and restore work for both FSMs (Metadata: full JSON; Partition: per the chosen strategy from `03-raft-fsm.md`).
- Unit tests exist for FSM determinism: same command sequence applied to two FSM instances yields identical state.
- Integration test exists: kill the single-node Raft, restart, verify state recovers.

**Typical ticket types:**
- NodeHost wrapper + lifecycle.
- Shard ID allocation scheme.
- Metadata FSM Update for each command type.
- Metadata FSM Lookup paths.
- Metadata FSM snapshot save/restore.
- Partition FSM Update implementation.
- Partition FSM Lookup paths.
- Partition FSM snapshot strategy A.
- FSM determinism tests.
- Single-node restart test.

### M3 - 3-node cluster with produce/fetch

**Goal:** a 3-node cluster running on the local machine successfully serves end-to-end produce and fetch traffic from a Go client over gRPC, with replication factor 3, with leader failover.

**Milestone DoD:**
- ClusterCoordinator and DataCoordinator are implemented.
- Management API gRPC service is implemented and serving.
- Data API gRPC service is implemented (Produce, Fetch, GetOffsets) - JoinGroup/Heartbeat/etc. land in M4.
- Client library Producer and basic Consumer (no group, manual partitions) are implemented.
- AdminClient is implemented (CreateTopic, DescribeTopic, DescribeCluster, etc.).
- A 3-node cluster started via 3 separate processes (no docker yet - that's M5) successfully:
  - Creates a topic with RF=3 and 3 partitions.
  - Accepts produce(acks=all) and returns offsets.
  - Replicates batches across all 3 nodes' Storage.
  - Serves fetch from leader.
  - On leader kill, a new leader is elected (dragonboat) and produce/fetch resume after client retry.
- Token-based authentication is wired in but defaults to PLAINTEXT (no token configured).
- Basic structured logging is in place across all the above.

**Typical ticket types:**
- ClusterCoordinator: each public method as its own ticket.
- DataCoordinator: produce path, fetch path, partition replica lifecycle (start/stop), retention loop.
- Proto definitions for Management API and Data API.
- Management API gRPC server.
- Data API gRPC server (produce/fetch only).
- Auth interceptor (server side).
- Client library: Producer.
- Client library: AdminClient.
- Client library: basic Consumer (no group).
- Multi-process local cluster smoke test.
- Leader-failover smoke test.

### M4 - Consumer groups

**Goal:** consumer groups with range-based rebalance work end-to-end. Multiple consumers in a group split partitions; member crash triggers rebalance; offsets commit and survive restart.

**Milestone DoD:**
- Group Coordinator is implemented per `08-consumer-groups.md`, hosted by the metadata shard leader.
- Data API: JoinGroup, Heartbeat, LeaveGroup, CommitOffset, FetchCommittedOffsets are implemented.
- Client library Consumer supports group mode: Subscribe, Poll (with background heartbeat), Commit.
- A 3-node cluster successfully:
  - Hosts a group with 2-3 consumers, splits partitions across them via range assignment.
  - Triggers rebalance on consumer leave / heartbeat timeout.
  - Persists committed offsets across consumer restart.
- Integration tests cover the above scenarios.

**Typical ticket types:**
- Group state in Metadata FSM (Update commands + Lookup).
- Range assignment algorithm.
- JoinGroup handler.
- Heartbeat handler.
- LeaveGroup handler.
- Session timeout sweep.
- CommitOffset / FetchCommittedOffsets handlers.
- Client Consumer group mode: Subscribe + Poll loop.
- Client Consumer: background heartbeat goroutine.
- Client Consumer: rebalance handling (graceful re-join).
- Group end-to-end integration test.

### M5 - Integration, observability, and polish

**Goal:** the system runs on docker-compose with 3 nodes, full integration tests pass with simulated node kills, metrics are exposed, logging is uniform, retention enforces visibly during a demo.

**Milestone DoD:**
- docker-compose.yml runs a 3-node cluster with one client image producing/consuming.
- Integration test suite (run via `make integration-test` or similar) executes:
  - Cluster bootstrap.
  - Produce/fetch happy path.
  - Leader-kill failover (kill node 1, verify produce/fetch continue on node 2).
  - Consumer group rebalance on consumer kill.
  - Retention: produce N batches, advance time / cross size threshold, verify segment deletion.
  - Restart + recovery: kill all nodes, restart, verify state intact.
- Prometheus metrics endpoint is reachable on each node, returns the metric catalog from `09-metrics-logging.md`.
- pprof endpoint works behind `--pprof` flag.
- Structured JSON logs are emitted to stdout from all modules.
- README.md exists with: how to run, how to test, basic architecture diagram (link to design docs).

**Typical ticket types:**
- Dockerfile.
- docker-compose.yml with 3 brokers + a client/test runner.
- Test harness for integration tests (Go test or bash).
- Each integration scenario as its own ticket.
- Metrics: Storage metrics implementation.
- Metrics: Raft metrics implementation.
- Metrics: API metrics implementation.
- Logging: cross-module audit + cleanup pass.
- README.md.

## Ticket format

Every ticket file uses this template exactly:

```markdown
# T-NNN: <Short title>

**Milestone:** M<N> - <milestone name>
**Effort:** XS | S | M | L
**Status:** TODO

## Goal

<One sentence stating what becomes true after this ticket is done.>

## Context

<2-5 sentences. Why this ticket exists, what design docs are relevant. Always link to specific design doc sections.>

References:
- [link to relevant design doc section]
- [link to relevant requirements section]

## Scope

<Bulleted list of what this ticket covers. Be concrete.>

- ...
- ...

## Out of scope

<Bulleted list of what this ticket does NOT cover, with cross-reference to the ticket that does.>

- <thing>: see T-XXX
- <thing>: see T-YYY

## Definition of done

<Checklist. Every item must be objectively verifiable.>

- [ ] Code compiles: `go build ./...`
- [ ] Tests pass: `go test ./<package>/...`
- [ ] (specific functional criterion)
- [ ] (specific test criterion)
- [ ] (other)

## Tests required

<List of specific test cases this ticket must add. Be concrete: "TestSegmentRollAtSizeThreshold", not "test segment rolling".>

- ...
- ...

## Dependencies

<List of T-NNN tickets that must be done before this one. Empty if none.>

- T-XXX: <one-line reason>

## Notes

<Optional. Implementation hints, gotchas, references to specific code patterns. Do NOT pre-write code here. If a VERIFY item from design phase is relevant, mention it here as "verify before implementing".>
```

### Effort sizing

- **XS** - under 1 hour. Trivial config, single-function utility, doc tweak.
- **S** - 1-2 hours. Single focused implementation with tests, no cross-module work.
- **M** - 2-4 hours. **This is the target size for most tickets.** Real implementation work with tests, possibly touching 2-3 files.
- **L** - 4-8 hours. Used sparingly; if you find yourself estimating L, consider splitting.

Aim for the majority of tickets to be **M**. The user requested medium-sized tickets grouped by milestone. If a planned ticket would be L, prefer splitting unless the work is genuinely indivisible.

### Tests requirement

Every implementation ticket **must** include a `Tests required` section listing specific tests to write. This was a deliberate user request to combat hallucination drift: tests written alongside implementation catch wrong assumptions early. Tickets that produce no testable artifact (e.g. README writing) may state "N/A - no executable tests" but must justify this in the section.

## Master index - `docs/tickets/README.md`

This file is produced last, after all milestone directories are filled. Contents:

- One paragraph summary.
- Link to each milestone's directory and its DoD.
- A table of all tickets: ticket ID, title, milestone, effort, dependencies (count). Sorted by ticket ID.
- A mermaid graph showing **inter-milestone** dependencies (which milestone blocks which) plus a few highlighted critical-path tickets.
- A "recommended order" section: a single linear sequence of tickets the user can follow top-to-bottom. This is the user's checklist.
- An "if running out of time" section: identifies which milestones can be skipped and at what cost. M5 → M4 → M3 is the priority order from "must" to "nice".

## Stage protocol

This session is split by milestone, not by ticket. Approval is per-milestone.

1. Start with **M0 - Foundations**. Produce all tickets for M0. Post `Milestone M0 complete: <count> tickets`. Summarize what's in M0, list any unresolved VERIFY items that became their own tickets, ask for approval.
2. After user approval, proceed to **M1 - Storage standalone**. Same protocol: `Milestone M1 complete: <count> tickets`.
3. After user approval, proceed to **M2 - Raft + FSMs on single node**. Same protocol.
4. After user approval, proceed to **M3 - 3-node cluster with produce/fetch**. Same protocol.
5. After user approval, proceed to **M4 - Consumer groups**. Same protocol.
6. After user approval, proceed to **M5 - Integration and polish**. Same protocol.
7. After user approval, produce `docs/tickets/README.md` (the master index). Post `Master index complete`. Wait for approval.
8. After approval, post a session-end summary listing total ticket counts per milestone, total estimated effort range, the recommended-order sequence as a numbered list, and any unresolved blockers.

Do not bundle approvals. Do not start the next milestone without explicit "approved" or "proceed".

If the user requests changes to a completed milestone, apply them, re-post the milestone-complete summary, and wait again.

## Output style

- All artifacts in **English**.
- All diagrams in **mermaid**.
- Be specific. Avoid "implement Storage" - write "implement `Storage.Append` with single-writer mutex, returning the assigned base offset; covered by `TestStorageAppend_*` test cases listed below".
- Cross-reference design docs by relative path: `[02-storage.md §3](../design/02-storage.md#3-...)`.
- Do not duplicate design content into tickets. Tickets point to design; they do not restate it.
- One ticket per file. No combining.

## Anti-patterns to avoid

- **Tickets that just say "implement module X".** Always break down by method, by file, or by feature within a module.
- **Tickets without explicit tests.** Every implementation ticket lists specific test names.
- **Circular dependencies between tickets.** If discovered, raise to user.
- **Silent dependency on un-designed behavior.** If a ticket would require behavior not specified in design docs, raise to user - design phase needs to be reopened, briefly, before that ticket can be written.
- **Time-based estimates pretending precision.** Use XS/S/M/L bins; do not write "2.5 hours".
- **Tickets that mix milestones.** A ticket belongs to exactly one milestone. If it spans, split.

## What's next after this session

After this session ends, the design phase is complete. The user starts the implementation phase: a fresh Claude Code session per ticket (or per small batch of related tickets). The user will likely write a new, much shorter `CLAUDE.md` for implementation sessions, focused on coding standards, test invocation, and pointing at design docs.

You do not produce that implementation-phase `CLAUDE.md`. Your output ends with the master index and the session-end summary.
