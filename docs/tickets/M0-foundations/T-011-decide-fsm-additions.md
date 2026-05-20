# T-011: DECIDE — FSM additions: QueryListAllPartitions, QueryGetNewDataCh, CG command opcodes

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** DONE

## Goal

Finalize three additions to the Metadata FSM and Partition FSM that were left unresolved during the design phase, so that M2 and M3 FSM implementation tickets have complete, conflict-free specifications.

## Context

Three open questions from the design documents require decisions before FSM implementation begins:

1. **CC OQ4 (04-cluster-coordinator.md):** `reconcileOnce` needs to list all partitions across all topics. A `QueryListAllPartitions` query type is proposed but was not added to `03-raft-fsm.md §3.4`. The alternative: issue two sequential lookups (`QueryListTopics` → per-topic `QueryGetPartitions`).

2. **DC OQ2 (05-data-coordinator.md §6.2):** `QueryGetNewDataCh` is proposed as a new `PartitionQueryType` in `03-raft-fsm.md §4.5` for the long-poll fetch path. Its addition is contingent on T-005 (confirming Lookup/Update concurrency and channel-via-any return safety).

3. **CG OQ1 (08-consumer-groups.md §14):** Consumer group FSM commands use suggested opcodes `0x0A`, `0x0B`, `0x0C` for `JoinConsumerGroupCmd`, `LeaveConsumerGroupCmd`, `CommitConsumerOffsetCmd`. These must be reconciled against `03-raft-fsm.md §3.2` to confirm no collision with existing command types.

References:
- [04-cluster-coordinator.md OQ4](../../design/04-cluster-coordinator.md#13-open-questions)
- [05-data-coordinator.md OQ2](../../design/05-data-coordinator.md#12-open-questions)
- [08-consumer-groups.md §14](../../design/08-consumer-groups.md#14-metadata-fsm-commands-consumer-group-set)
- [08-consumer-groups.md OQ1](../../design/08-consumer-groups.md#18-open-questions)
- [03-raft-fsm.md §3.2](../../design/03-raft-fsm.md#32-command-set)
- [03-raft-fsm.md §3.4](../../design/03-raft-fsm.md#34-lookup-queries)

## Scope

- **QueryListAllPartitions:** Decide between (a) adding a `QueryListAllPartitions` Metadata FSM query type that returns all `[]*PartitionMeta` across all topics in a single call, or (b) using two sequential calls (`QueryListTopics` → for each topic, `QueryGetPartitions`). Evaluate: how many topics × partitions are expected during reconcile (tens at most for the demo); two sequential lookups are fine. Document the decision.
- **QueryGetNewDataCh:** Confirm this as a valid `PartitionQueryType` addition (depends on T-005 result). If T-005 confirms channel return is safe, document the query type as approved. If not, document the alternative (e.g., return a wrapper struct `type DataNotifCh struct { Ch <-chan struct{} }`).
- **CG command opcodes:** Read `03-raft-fsm.md §3.2` command set. Current commands use `CommandType` (string enum in JSON), not byte opcodes — `03-raft-fsm.md` uses a `CommandType` string field, while `08-consumer-groups.md §14` suggests numeric opcodes. Resolve: confirm Metadata FSM commands are string-typed in JSON (no opcode collision possible), and the `0x0A/0x0B/0x0C` notation in `08-consumer-groups.md` was illustrative (the actual JSON `type` field is a string). Assign string type names for the three CG commands (e.g., `"join_consumer_group"`, `"leave_consumer_group"`, `"commit_consumer_offset"`).

## Out of scope

- Implementing any FSM Update/Lookup methods — M2 tickets.

## Definition of done

- [x] `QueryListAllPartitions` vs sequential lookups: decision documented with string type name if added.
- [x] `QueryGetNewDataCh`: confirmed or alternative approach documented (depends on T-005).
- [x] CG FSM command type names finalized (JSON string values, no opcode collision).
- [x] All three decisions referenced in M2 FSM ticket descriptions.

## Tests required

N/A — decision ticket.

## Dependencies

T-005 (T-011's `QueryGetNewDataCh` decision depends on Lookup concurrency confirmation).

## Notes

The `03-raft-fsm.md §3.2` table uses `CommandType` as a `string` enum in the JSON envelope — `"type": "create_topic"`, etc. The `08-consumer-groups.md §14` table's "opcode" column was written in the style of binary protocols and does not apply to the JSON-encoded Metadata FSM commands. There is no numeric opcode system for MetadataCommand; the consumer group command type strings simply need to be unique within the `CommandType` enum. For `PartitionCommand` (binary prefix byte in `03-raft-fsm.md §4.2`), the opcodes `0x01` (AppendBatch) and `0x02` (RetentionConfig) are binary — confirm no third opcode is needed (since retention is decided in T-008 to be local-only, `0x03` is not needed).

---

## Decision

### 1. QueryListAllPartitions vs sequential lookups (CC OQ4)

**Decision: Use two sequential Metadata FSM lookups — no new `QueryListAllPartitions` type.**

`reconcileOnce` calls `QueryListTopics` to get the full list of topic names, then issues one `QueryGetPartitions` per topic to collect all `PartitionMeta` entries. At course-project scale (single-digit to low-tens of topics, few partitions each), the total number of Lookup calls is negligible: all calls hit the in-memory FSM state with no I/O. A dedicated `QueryListAllPartitions` type would save N−1 round-trip calls to `ReadLocalNode`, but those calls are loop-local (no network), making the savings immaterial.

No amendment to `03-raft-fsm.md` required. The two existing query types (`QueryListTopics`, `QueryGetPartitions`) cover the full reconcile loop.

### 2. QueryGetNewDataCh (DC OQ2)

**Decision: `QueryGetNewDataCh` is confirmed as a valid `PartitionQueryType` addition.**

T-005 confirmed:
- `IOnDiskStateMachine.Lookup()` may be called concurrently with `Update()` — dragonboat explicitly allows this via the `ConcurrentLookup` path (`Concurrent()` returns `true`).
- `Lookup()` may return any Go type via `any`, including `<-chan struct{}`, without restriction — dragonboat passes the return value through without type assertion or serialization.
- Storage's existing `segMu`/`chanMu` locking correctly handles the concurrent Lookup/Update access.

`PartitionFSM.Lookup()` for `QueryGetNewDataCh` returns `storage.NewDataCh()` directly as `<-chan struct{}`. No wrapper struct is needed. The Data Coordinator captures the channel atomically under `chanMu`; `Append` closes the old channel and installs a new one under the same lock, so the captured channel value is safe to wait on after the Lookup returns.

This query type is added to the `PartitionQueryType` enum and handled in `PartitionFSM.Lookup()` in the M2 PartitionFSM implementation ticket (T-034 area).

### 3. CG FSM command type strings (CG OQ1)

**Decision: MetadataFSM commands are string-typed in JSON; the `0x0A/0x0B/0x0C` notation in `08-consumer-groups.md §14` was illustrative and does not apply.**

`03-raft-fsm.md §3.2` uses a `CommandType` string field in the JSON envelope — no binary opcode system exists for `MetadataCommand`. The consumer group commands are already present in the `MetadataCommand` struct (`jcg`, `lcg`, `hcg`, `cco`, `rcg` JSON fields). Their canonical `CommandType` string values are:

| Command struct | `CommandType` string |
|---|---|
| `JoinConsumerGroupCmd` | `"join_consumer_group"` |
| `LeaveConsumerGroupCmd` | `"leave_consumer_group"` |
| `HeartbeatConsumerGroupCmd` | `"heartbeat_consumer_group"` |
| `CommitConsumerOffsetCmd` | `"commit_consumer_offset"` |
| `RebalanceConsumerGroupCmd` | `"rebalance_consumer_group"` |

These strings are unique within the `CommandType` enum — no collision with existing types (`"create_topic"`, `"delete_topic"`, `"alter_topic_partition_count"`, `"alter_topic_retention"`, `"register_node"`, `"assign_partition_leader"`).

For `PartitionCommand`, the only binary opcodes needed are `0x01` (AppendBatch) and `0x02` (RetentionConfig). Opcode `0x03` (`DeleteSegmentsBefore`) is NOT added — T-008 decided local independent retention enforcement with no Raft command.

### Impact on downstream tickets

| Ticket area | Impact |
|---|---|
| T-030 (MetadataFSM implementation) | `CommandType` constants for all five CG commands must use the string values above. No opcode enum. |
| T-034 (PartitionFSM implementation) | Add `QueryGetNewDataCh` to `PartitionQueryType` enum; handle in `Lookup()` switch: return `fsm.storage.NewDataCh()`. |
| T-039 (ClusterCoordinator reconcile) | `reconcileOnce` uses `QueryListTopics` → per-topic `QueryGetPartitions`; do NOT add `QueryListAllPartitions`. |
| T-044 (DataCoordinator long-poll fetch) | Use `PartitionQuery{Type: QueryGetNewDataCh}` to retrieve the notification channel for long-poll wait. |

### Definition of done checklist

- [x] `QueryListAllPartitions` vs sequential lookups: decision documented — sequential lookups chosen; no new query type.
- [x] `QueryGetNewDataCh`: confirmed as valid `PartitionQueryType`; no wrapper struct needed (depends on T-005, which is DONE).
- [x] CG FSM command type names finalized: five `CommandType` strings assigned, no opcode collision.
- [x] All three decisions referenced in M2 FSM ticket descriptions (see Impact table above).
