# CLAUDE.md - BunnyMQ Design Phase (Session 3: Stage 3-B)

## Project context

BunnyMQ is a Kafka-like distributed message broker written in Go. This is a course project with a one-week implementation budget after the design phase ends. This file governs your behavior during the **design phase only**: producing markdown documents and mermaid diagrams. No source code is written in this phase.

You are working in design mode. Your output is markdown files in `docs/design/`. Any attempt to create `.go` or `.proto` files is an error. If illustrating a design document requires Go-style type signatures or proto-style message definitions, embed them as fenced code blocks inside markdown.

This is **session 3** of a multi-session design phase. Sessions 1 and 2 are complete. This session covers **Stage 3-B**: detailed design for the upper-layer modules (coordinators, APIs, client library, consumer groups). Session 4 will produce the ticket breakdown.

## How to start

On every session start, before doing anything else:

1. Read this file (`CLAUDE.md`) in full.
2. Read `docs/REQUIREMENTS.md` in full. This is the source of truth for what the system does. In case of any conflict between this file and `REQUIREMENTS.md`, raise it in chat.
3. Read all approved outputs from previous sessions:
   - `docs/design/00-overview.md` (session 1)
   - `docs/design/01-modules.md` (session 1)
   - `docs/design/02-storage.md` (session 2)
   - `docs/design/03-raft-fsm.md` (session 2)
   - `docs/design/09-metrics-logging.md` (session 2)
   - All files in `docs/design/sequence/` produced by session 2.
4. List existing files in `docs/design/` and `docs/design/sequence/`. Anything beyond the files mentioned above is unexpected - report to user before doing anything else.
5. Skim `docs/adr/` only if you need historical context for a specific decision. These are pre-dragonboat sketches and contradict current architecture in many places. Do not modify them.
6. Confirm to the user in chat which module you are starting with and ask for go-ahead.

## Architectural decisions (fixed; not for redesign)

These decisions are not open for discussion. If a design constraint forces reconsidering one, raise the conflict to the user in chat - do not unilaterally change.

1. **Language:** Go (current stable version).
2. **Consensus and replication:** library `github.com/lni/dragonboat/v4`. Multi-raft model: one Raft shard per partition, plus one shard for cluster metadata.
3. **Wire protocol:** gRPC + Protobuf for all APIs. No custom binary protocol. No `sendfile` zero-copy. Storage retains a Kafka-compatible batch format on disk to preserve the option of switching to a binary protocol later.
4. **Acks semantics:** `acks=0` via `dragonboat.Propose` (fire-and-forget); `acks=all` via `dragonboat.SyncPropose` (quorum commit). `acks=1` is intentionally not implemented.
5. **Storage:** segmented append-only log, Kafka-style. Index files are mmap'd. Log file is append-only. Batch format on disk equals batch format on wire. Detailed in `02-storage.md`.
6. **Metadata FSM:** `IStateMachine` (in-memory state, JSON snapshots). Sole writer is the cluster coordinator module. Detailed in `03-raft-fsm.md`.
7. **Partition FSM:** `IOnDiskStateMachine` wrapping `Storage`. The FSM `Update()` method is strictly deterministic. Detailed in `03-raft-fsm.md`.
8. **Consumer groups:** minimal v1 - simple range-based assignment, no cooperative rebalance. Architecture must allow future extension to richer protocols.
9. **Tests:** unit tests for Storage and FSM are mandatory; integration tests via docker-compose with node-kill scenarios are scheduled for the final implementation milestone.
10. **Authentication:** token-based via gRPC metadata, configured at startup, no rotation/expiration in v1. PLAINTEXT mode (empty token list) is supported and is default for the demo. See `REQUIREMENTS.md` §8.

## Tools and rules of engagement

### What you may do

- Read and write files in `docs/design/`.
- Write sequence diagrams in **mermaid** format only (not plantuml).
- Run shell commands for investigation: `tree`, `ls`, `grep`, `cat`. Use them sparingly.
- Read dragonboat documentation online via web fetch when available. **Verify dragonboat API specifics before relying on them in design documents** - see hallucination protocol below.
- Read gRPC and protobuf documentation online when needed.
- Point-look at the local Kafka repository at `/Users/bbagaviev/source/kafka/` only for concrete byte-level details or specific protocol message layouts. Limit: up to 3 files per look-up, with targeted `grep`/`cat`.

### What you may not do

- Create or edit `.go`, `.proto`, or any non-markdown source files. Proto-style message definitions for design illustration go inside markdown fenced code blocks.
- Read Kafka source code for conceptual questions. For conceptual questions, see the relay protocol below.
- Modify any file in `docs/adr/`, `docs/design/00-*`, `docs/design/01-*`, `docs/design/02-*`, `docs/design/03-*`, `docs/design/09-*`, or any approved sequence diagrams from session 2. They are immutable. If you find an issue, raise it in chat.
- Override any decision in the **Architectural decisions** section of this file.
- Override any requirement in `docs/REQUIREMENTS.md`.
- Start work on Stage 4 (ticket breakdown). It is out of scope for this session.

### Hallucination protocol

This session's modules touch areas where LLM-generated designs are at higher risk of containing plausible-but-wrong details. Specifically: dragonboat's API surface beyond `SyncPropose`/`Update`/`Lookup`, gRPC streaming patterns, consumer group rebalance semantics. Apply the following rules:

1. **For dragonboat:** before specifying a method signature, configuration field, or behavior, either (a) cite a source - link to dragonboat docs or recall a verifiable fact - or (b) mark it as `// VERIFY: <what to verify>` in the design doc and surface it in the open questions list. Do not write design that locks in API details you are not sure about.

2. **For gRPC streaming and bidirectional RPCs:** the same rule. Streaming patterns have non-obvious semantics around backpressure, error propagation, and connection lifecycle. If unsure, mark as VERIFY and ask the user.

3. **For protobuf message layouts:** these are generally low-risk to specify, but explicitly note any field that is "Kafka-compatible" or "matches Kafka X.Y" without verification - mark as VERIFY.

4. **General rule:** if you find yourself writing "this should work because…" or "Claude knows that…" - stop, mark as VERIFY, and surface in open questions. The design phase exists to make the implementation phase boring; ambiguity smuggled into the design becomes implementation-phase debugging.

### Conceptual questions about Kafka - relay through the user

Do not browse Kafka source for concepts. Format a question for the user to relay to a separate Claude Code session opened in the Kafka repository. Use this exact template, and emit it to the chat:

```
## QUESTION_FOR_KAFKA_AGENT

**Topic:** <one-line topic>

**Question:** <specific question>

**Likely files to inspect:** <best guesses, may be empty>

**Expected answer format:** <markdown / list / paragraph / mermaid>
```

Use this mechanism economically - one outstanding question at a time. Wait for the user's reply before asking the next.

### Architectural ambiguity

If a design choice is ambiguous and not resolved by `REQUIREMENTS.md`, the **Architectural decisions** section, or session-1/session-2 outputs, ask the user in chat. Do not silently choose.

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
    ├── 09-metrics-logging.md
    └── sequence/
        ├── storage-append.md
        ├── storage-read.md
        ├── storage-segment-roll.md
        ├── storage-recovery.md
        ├── storage-retention.md
        ├── raft-metadata-apply.md
        ├── raft-partition-apply.md
        ├── raft-leader-election.md
        ├── raft-snapshot-metadata.md
        └── raft-snapshot-partition.md
```

Target state at session end:

```
docs/
├── REQUIREMENTS.md
├── adr/
└── design/
    ├── 00-overview.md
    ├── 01-modules.md
    ├── 02-storage.md
    ├── 03-raft-fsm.md
    ├── 04-cluster-coordinator.md       (new)
    ├── 05-data-coordinator.md          (new)
    ├── 06-api-protocol.md              (new)
    ├── 07-client-library.md            (new)
    ├── 08-consumer-groups.md           (new)
    ├── 09-metrics-logging.md
    └── sequence/
        ├── (session-2 diagrams)
        ├── topic-create.md             (new)
        ├── topic-delete.md             (new)
        ├── topic-list-describe.md      (new)
        ├── produce-acks-all.md         (new)
        ├── produce-acks-0.md           (new)
        ├── consumer-fetch.md           (new)
        ├── consumer-fetch-empty-wait.md (new)
        ├── offset-commit.md            (new)
        ├── offset-fetch.md             (new)
        ├── group-join.md               (new)
        ├── group-heartbeat.md          (new)
        ├── group-leave.md              (new)
        ├── group-rebalance.md          (new)
        ├── client-producer-send.md     (new)
        ├── client-consumer-poll.md     (new)
        ├── client-leader-discovery.md  (new)
        ├── startup-cluster.md          (new)
        ├── startup-node-join.md        (new)
        ├── shutdown-graceful.md        (new)
        └── (any additional flow diagrams the agent finds necessary)
```

The numeric prefix gap (00, 01, 02, 03, 04, 05, 06, 07, 08, 09) is now closed.

## Modules covered in this session

Five modules. Each gets its own design doc and a set of sequence diagrams. Approval is per-module. After all five are approved, you produce a session-end summary.

The modules form layers: ClusterCoordinator and DataCoordinator sit on top of the Raft layer designed in session 2. APIs sit on top of coordinators. ClientLibrary sits on top of APIs (logically - it is a separate process). ConsumerGroups is a feature that touches coordinators, metadata FSM, and APIs.

### Module 1: Cluster Coordinator (`04-cluster-coordinator.md`)

**Scope:** the control-plane brain. Owns the Metadata FSM lifecycle from BunnyMQ's perspective, handles all admin operations (topic management, cluster description), and is the **sole writer** to the Metadata FSM. Does not touch partition data.

**Required contents:**

- Summary (3-5 sentences).
- Responsibilities, in bullets. Explicit non-responsibilities.
- Public interface: Go-style method signatures with godoc comments. Cover at minimum:
  - `CreateTopic(ctx, name, partitionCount, replicationFactor, configOverrides) (TopicInfo, error)`
  - `DeleteTopic(ctx, name) error`
  - `ListTopics(ctx) ([]TopicInfo, error)`
  - `DescribeTopic(ctx, name) (TopicDescription, error)`
  - `AlterTopicPartitionCount(ctx, name, newCount) error`
  - `AlterTopicRetention(ctx, name, retentionMs, retentionBytes) error`
  - `DescribeCluster(ctx) (ClusterDescription, error)`
- For each method: describe the flow - which Raft commands are issued, which lookups are performed, what side effects happen on success.
- Topic creation: detailed flow. (a) Validate inputs. (b) Choose replica assignments - algorithm for placing N partitions × RF replicas across the M nodes (e.g. round-robin starting at a hash of the topic name). (c) Allocate shard IDs for the new partition shards (deterministic - derived from a counter in metadata or from `topic_name + partition_id`). (d) Issue `CreateTopic` Raft command to the metadata shard. (e) On commit, the cluster coordinator on every node sees the new topic in its local metadata FSM lookup. (f) Each node spawns the Raft replicas for partition shards it is assigned to (this happens in the partition lifecycle layer - describe the trigger; actual implementation is in DataCoordinator).
- Topic deletion: detailed flow. Mark topic as `Deleting` in metadata, all subsequent reads and writes against it return TopicNotFound. Background process (described here) tears down partition Raft shards and deletes Storage directories.
- Partition shard ID allocation: specify the deterministic mapping. Reference `03-raft-fsm.md`.
- Leader epoch updates: when the partition shard reports a new leader (via a Raft command from the leading replica or via a periodic background sweep), the cluster coordinator commits an `UpdatePartitionLeader` command to the metadata FSM. Specify which mechanism is used. **Mark as VERIFY** the exact mechanism by which dragonboat surfaces leader changes.
- Bootstrap behavior: what does the cluster coordinator do at process start? Spin up metadata shard replica, wait for it to elect a leader, read current state, signal readiness to the rest of the system.
- Routing: this module does NOT route partition-data requests. It is purely admin-plane. State this explicitly.
- Concurrency model: which methods can be called concurrently from multiple gRPC handlers? What is the locking strategy? How does this interact with Raft single-leader writes?
- Failure modes: what does a method return if the metadata shard has no leader, if Raft propose times out, if validation fails?
- Open questions for the user.

**Sequence diagrams:**
- `topic-create.md` - full flow from gRPC entry to clients seeing the topic.
- `topic-delete.md` - async deletion, partition shard teardown.
- `topic-list-describe.md` - read paths.

### Module 2: Data Coordinator (`05-data-coordinator.md`)

**Scope:** the data-plane brain on each node. Owns the lifecycle of Partition FSM replicas on this node. Handles produce and fetch requests, routing them to the right partition's Raft shard, calling `SyncPropose` / `Propose` / `Lookup`. Reports leader changes upward to the cluster coordinator.

**Required contents:**

- Summary.
- Responsibilities and non-responsibilities.
- Public interface - Go-style methods. Cover at minimum:
  - `Produce(ctx, topic, partitionID, batch, acks) (offset int64, err error)`
  - `Fetch(ctx, topic, partitionID, offset, maxBytes, maxWaitMs) (records [][]byte, nextOffset int64, err error)`
  - `GetEarliestOffset(ctx, topic, partitionID) (int64, error)`
  - `GetLatestOffset(ctx, topic, partitionID) (int64, error)`
  - `GetOffsetByTimestamp(ctx, topic, partitionID, timestampMs) (int64, error)`
  - Internal: `StartPartitionReplica(topic, partitionID)`, `StopPartitionReplica(topic, partitionID)`
- Partition replica lifecycle: when a partition is created and this node is in its replica set, DataCoordinator joins the partition Raft shard. When a partition is deleted, it leaves the shard and asks Storage to clean up. How does DataCoordinator learn about new/deleted partitions? Via watching the metadata FSM (polling at a configurable interval, or via a notification mechanism - specify which and **mark VERIFY** if dragonboat exposes notifications).
- Routing logic for Produce: (a) Look up partition leader from metadata FSM. (b) If this node is the leader, call SyncPropose/Propose on the local Raft shard. (c) If this node is not the leader, return NotLeader error with the leader's address. The gRPC layer surfaces this to the client; the client retries against the leader. (Forwarding within the cluster is **not done in v1** - keep it client-side. Document this decision and rationale.)
- Routing logic for Fetch: same - only the leader serves fetches in v1. Reads via `Lookup` on the local Partition FSM if this node is the leader; NotLeader otherwise.
- Long-poll fetch: when `maxWaitMs > 0` and no records are available beyond `offset`, the request blocks until either (a) new records arrive, (b) the timeout expires, (c) leader changes (return NotLeader), (d) context cancellation. Specify the mechanism - channel-based notification from Partition FSM on each `Update`, or polling with backoff. Recommend channel-based; specify the implementation outline. **Mark VERIFY** if there are concerns about Partition FSM notifying out-of-band given dragonboat's threading model.
- Acks=all flow: SyncPropose, wait for result, return offset from `sm.Result.Value`.
- Acks=0 flow: Propose (async), do not wait for confirmation, return immediately. Document that the client receives no offset.
- Concurrency: how many goroutines per partition shard, how do produce and fetch interleave, how does retention enforcement (called from the Partition FSM side) interact with in-flight reads.
- Retention enforcement schedule: a background goroutine per partition replica calls `Storage.EnforceRetention` on a configurable interval (e.g. every 60 seconds). This runs **only on the leader** to avoid divergent state; followers' Storage cleanup is driven by their own FSM applying delete commands. Discuss whether retention is itself a Raft command or a local-only operation. **Recommend: Raft command** so that retention deletions are deterministic across replicas. Add a `DeleteSegmentsBefore(offset int64)` command type to the Partition FSM command set in `03-raft-fsm.md` (note this in the open questions for that document - the user can decide whether to update `03-raft-fsm.md` or accept the addition here).
- Failure modes.
- Open questions.

**Sequence diagrams:**
- `produce-acks-all.md`
- `produce-acks-0.md`
- `consumer-fetch.md` - basic fetch returning data.
- `consumer-fetch-empty-wait.md` - long-poll fetch waiting for new records or timeout.

### Module 3: API Protocol (`06-api-protocol.md`)

**Scope:** gRPC service definitions for Data API and Management API. Protobuf message types. Error codes. Authentication metadata convention.

**Required contents:**

- Summary.
- Two services: `DataService` and `ManagementService`. Each on its own port (configurable). Both share the same authentication mechanism.
- Protobuf message definitions (in markdown code blocks, not in real `.proto` files): every request, response, and shared type used by either service.
- For `ManagementService`, RPC methods covering admin operations:
  - `CreateTopic`, `DeleteTopic`, `ListTopics`, `DescribeTopic`
  - `AlterTopicPartitions`, `AlterTopicRetention`
  - `DescribeCluster`, `ListPartitions`
- For `DataService`, RPC methods:
  - `Produce` (unary; can be unary even for batched produce since each request is one batch)
  - `Fetch` (unary, with `max_wait_ms` parameter for long-polling)
  - `GetOffsets` (earliest, latest, by timestamp)
  - `JoinGroup`, `Heartbeat`, `LeaveGroup`, `CommitOffset`, `FetchCommittedOffsets`
- Streaming considerations: discuss whether Produce or Fetch should be streaming. Recommend: unary in v1 for simplicity. Streaming Produce (one stream per producer-partition) could be added later as an optimization. Document this as future work.
- Error code enumeration: a single `BunnyError` enum used in all error responses. Cover at minimum: `OK`, `Unknown`, `InvalidArgument`, `TopicNotFound`, `TopicAlreadyExists`, `PartitionNotFound`, `NotLeader` (with `leader_node_id` and `leader_address` fields in the error details), `OffsetOutOfRange`, `MessageTooLarge`, `Unauthenticated`, `Unavailable` (e.g. no Raft leader), `Timeout`, `InvalidMessageFormat`. Specify each with intended meaning.
- gRPC error mapping: which gRPC status codes correspond to which `BunnyError` values. Use standard mappings (`InvalidArgument` → `INVALID_ARGUMENT`, `Unauthenticated` → `UNAUTHENTICATED`, `NotLeader` → `FAILED_PRECONDITION`, etc.).
- Authentication: clients send `bunnymq-auth-token: <token>` in gRPC metadata. Server interceptor validates against the token list from config. PLAINTEXT mode (empty token list) bypasses validation. Specify the exact metadata key. Reference `REQUIREMENTS.md` §8.
- Server-side interceptor chain: auth → logging → metrics → handler. Specify the intended order.
- Versioning: every service is versioned in the proto package (`bunnymq.v1`). v1 is the only version in scope.
- Wire batch format: the `Produce` request carries a batch in a `bytes batch_data` field. The format of `batch_data` is the on-disk batch format from `02-storage.md` and `REQUIREMENTS.md` §4.4. The client encodes; the server stores as-is. Symmetric for `Fetch` response.
- Open questions.

**Sequence diagrams:** the API itself does not need sequence diagrams beyond what is already covered in coordinator diagrams. Skip unless a specific cross-cutting concern is found.

### Module 4: Client Library (`07-client-library.md`)

**Scope:** the public Go client library - `pkg/client`. Three types of clients: `Producer`, `Consumer`, `AdminClient`. Each is a thin wrapper over the gRPC client with retry, leader discovery, and reconnect logic.

**Required contents:**

- Summary.
- Package layout: `pkg/client` exports `NewProducer(config) (*Producer, error)`, `NewConsumer(config) (*Consumer, error)`, `NewAdminClient(config) (*AdminClient, error)`. Internal helpers in `internal/client`.
- Common configuration: bootstrap servers (list of broker addresses), token, TLS settings, request timeout, retry policy.
- `Producer`:
  - Public methods: `Send(ctx, topic, key, value, headers, acks) (offset int64, err error)`, `SendBatch(ctx, topic, partitionID, records, acks) (offset int64, err error)`, `Flush(ctx) error` (no-op in v1 if no internal batching is implemented), `Close() error`.
  - Partition selection: by key hash if key is present, round-robin otherwise. Specify the hash function (FNV-1a 32-bit on the key bytes, then modulo partition count). Cache topic metadata (partition count, leader per partition) with TTL or with refresh-on-NotLeader.
  - Leader discovery: on first Produce to a topic, fetch metadata from any bootstrap server. Cache `(topic, partition_id) → leader_address`. On NotLeader response, refresh metadata and retry on the new leader.
  - Retry policy: configurable max retries, exponential backoff. Retryable errors: NotLeader, Unavailable, Timeout. Non-retryable: InvalidArgument, MessageTooLarge, Unauthenticated, TopicNotFound (after metadata refresh).
  - Internal batching: out of scope for v1. Each `Send` makes one RPC. Document this and note `SendBatch` as the path for explicit user-batched sends.
- `Consumer`:
  - Public methods: `Subscribe(topics []string) error`, `Poll(ctx, maxWaitMs) ([]Record, error)`, `Commit(ctx) error`, `CommitOffsets(ctx, offsets map[TP]int64) error`, `Seek(topic, partition, offset)`, `Close() error`.
  - Group vs no-group consumers: a Consumer with `GroupID` set joins a consumer group; without it, the user manually specifies partitions and offsets.
  - Group consumer lifecycle: `Subscribe` triggers JoinGroup. Poll loop handles heartbeats in the background (separate goroutine). On rebalance signal from heartbeat response, the Poll loop coordinates: pause polling, finish in-flight commits, re-join group.
  - Long-poll fetch: Poll calls Fetch with `max_wait_ms = min(remaining_poll_timeout, max_fetch_wait)`. Returns when records arrive or timeout expires.
  - Auto-commit: optional, configurable interval. Auto-commit happens in the heartbeat goroutine.
  - Non-group consumer: no JoinGroup/Heartbeat. User sets the read position via `Seek`. Commits go to a per-(group_id, partition) offset store; for non-group consumers, offsets are kept only locally (no server-side commit unless the user explicitly provides a group_id).
- `AdminClient`:
  - One method per ManagementService RPC. Thin wrapper.
- Connection management: one gRPC connection per broker, lazily established. Connection pool. Reconnect with backoff on failure.
- Open questions.

**Sequence diagrams:**
- `client-producer-send.md` - full path including metadata cache, leader discovery, NotLeader retry.
- `client-consumer-poll.md` - group consumer poll loop including background heartbeat.
- `client-leader-discovery.md` - what happens when a producer or consumer hits NotLeader.

### Module 5: Consumer Groups (`08-consumer-groups.md`)

**Scope:** the Group Coordinator design. Group state lives in the Metadata FSM. Members of a group communicate with the group's coordinator node, which is the leader of a designated metadata shard partition (or, simply, the leader of the metadata shard itself in v1, since there is only one metadata shard).

**Required contents:**

- Summary.
- Group state model. Reference `REQUIREMENTS.md` §4.5. Specify exactly what is stored and how it is keyed in the Metadata FSM state.
- Coordinator discovery: in v1, the metadata shard leader is the group coordinator for all groups (single coordinator). Clients discover it via DescribeCluster or by sending JoinGroup to any node, which returns NotLeader with the leader's address if not the leader of the metadata shard.
- Member ID assignment: server-assigned UUID at JoinGroup time. Returned in the response.
- Generation ID: incremented on every membership change.
- Range-based partition assignment algorithm (specify in pseudocode or prose):
  - Inputs: list of subscribed topics with their partition counts; sorted list of group members by member_id.
  - For each topic: split partition_ids into len(members) contiguous ranges. Assign rangeᵢ to memberᵢ. Remainder partitions go to earlier members.
  - Output: per-member assignment.
- JoinGroup flow: client sends JoinGroup. Server validates topics exist. Server adds member to the group state via a Raft command (`JoinConsumerGroup`). On commit, all members observe the new group state. The coordinator computes the assignment and includes it in the JoinGroup response. Concurrent JoinGroups from different members are serialized by Raft.
- Heartbeat flow: client sends Heartbeat at a configurable interval (default ~3 seconds). If the group has been rebalanced since the client last joined (i.e. `client_generation_id < current_generation_id`), the response signals "rebalance required" and the client must re-issue JoinGroup. If no heartbeat is received within the session timeout (configurable, default 30 seconds), the coordinator removes the member via a `LeaveConsumerGroup` Raft command.
- LeaveGroup flow: client sends LeaveGroup. Coordinator issues `LeaveConsumerGroup` Raft command. On commit, generation_id is bumped and remaining members will discover the change on next heartbeat.
- Rebalance flow: triggered by member join, member leave, or session timeout. Coordinator issues `RebalanceConsumerGroup` Raft command which atomically updates membership and generation_id. Members discover via heartbeat response and re-join.
- Offset commit flow: client sends CommitOffset with `(group_id, member_id, generation_id, offsets)`. Coordinator validates `member_id` is a current member and `generation_id` is current. If valid, issues `CommitConsumerOffset` Raft command. If not valid (member kicked or generation stale), returns error indicating rebalance.
- Offset fetch flow: simple Lookup against Metadata FSM.
- Session timeout enforcement: a background sweep on the coordinator checks for stale members every N seconds. **Mark VERIFY**: this check writes to metadata via Raft, so it must run only on the metadata shard leader. Specify the leader-detection logic.
- Failure scenarios:
  - Coordinator (metadata shard leader) fails: dragonboat elects a new leader. The new leader takes over coordinator duties. Members may experience transient errors during election but recover on retry.
  - Member crashes silently: detected by session timeout. Triggers rebalance.
  - Network partition isolating a member: the member's heartbeats time out; coordinator removes it. When the member returns, its next heartbeat says "you are not in the group", member re-joins.
- Limitations: explicit list of what is NOT implemented (sticky assignment, cooperative rebalance, static membership, group-level configuration overrides). Reference `REQUIREMENTS.md` §9.
- Open questions.

**Sequence diagrams:**
- `group-join.md`
- `group-heartbeat.md`
- `group-leave.md`
- `group-rebalance.md` - triggered by membership change.
- `offset-commit.md`
- `offset-fetch.md`

### Cross-cutting sequence diagrams

These are not tied to a single module but should be produced as part of this session because all modules involved are now designed. Place them under `docs/design/sequence/` alongside the module-specific ones.

- `startup-cluster.md` - fresh cluster bootstrap: 3 nodes start simultaneously, metadata shard forms, no topics yet, all nodes become ready.
- `startup-node-join.md` - one node restarts in an existing cluster, rejoins metadata shard and all partition shards it was previously a member of, recovers Storage state.
- `shutdown-graceful.md` - single-node graceful shutdown: stop accepting new RPCs, drain in-flight requests, leave Raft shards (or transfer leadership), flush Storage, exit.

These are not assigned to any single module document. The agent produces them after Module 5 is approved and before the session-end summary.

## Stage protocol

Approval is per-module, in the order listed. After all five modules are approved, produce the cross-cutting startup and shutdown diagrams. After all diagrams are in place, produce a session-end summary.

1. Start with `04-cluster-coordinator.md` and its sequence diagrams. When done, post `Module 1/5 complete: Cluster Coordinator`. Summarize what was produced, list open questions and VERIFY items, ask for approval.
2. After user approval, proceed to `05-data-coordinator.md` and its diagrams. Same protocol: `Module 2/5 complete: Data Coordinator`.
3. After user approval, proceed to `06-api-protocol.md`. Same protocol: `Module 3/5 complete: API Protocol`.
4. After user approval, proceed to `07-client-library.md`. Same protocol: `Module 4/5 complete: Client Library`.
5. After user approval, proceed to `08-consumer-groups.md`. Same protocol: `Module 5/5 complete: Consumer Groups`.
6. After user approval, produce the three cross-cutting sequence diagrams (`startup-cluster.md`, `startup-node-join.md`, `shutdown-graceful.md`). Post `Cross-cutting diagrams complete`. Wait for approval.
7. After approval, post a session-end summary listing all files produced, all VERIFY items still outstanding, all open questions still outstanding, and a recommendation on readiness for session 4 (ticket breakdown).

Do not bundle approvals. Do not start the next module without explicit "approved" or "proceed to module N+1".

If the user requests changes to a completed module, apply them, re-post the module-complete summary, and wait again.

## Output style

- All artifacts in **English**.
- All diagrams in **mermaid**.
- Documents start with a 3-5 sentence summary, then detailed sections.
- Avoid filler. Replace vague claims with concrete statements.
- Use tables for tabular data. Use prose for narrative.
- Cross-reference other documents by relative path.
- Code blocks for Go signatures use `go` fenced syntax. Code blocks for proto definitions use `proto` fenced syntax.
- Mark all uncertain technical details as `VERIFY: <what to verify>` inline and surface in the open questions section.

## What's next

Session 4 will produce ticket breakdowns based on these designs. You do not produce tickets in this session. If you find yourself drawn into specifying implementation order or work granularity, stop and note it as a topic for session 4.