# T-011: DECIDE — FSM additions: QueryListAllPartitions, QueryGetNewDataCh, CG command opcodes

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

## Goal

Finalize three additions to the Metadata FSM and Partition FSM that were left unresolved during the design phase, so that M2 and M3 FSM implementation tickets have complete, conflict-free specifications.

## Context

Three open questions from the design documents require decisions before FSM implementation begins:

1. **CC OQ4 (04-cluster-coordinator.md):** `reconcileOnce` needs to list all partitions across all topics. A `QueryListAllPartitions` query type is proposed but was not added to `03-raft-fsm.md §3.4`. The alternative: issue two sequential lookups (`QueryListTopics` → per-topic `QueryGetPartitions`).

2. **DC OQ2 (05-data-coordinator.md §6.2):** `QueryGetNewDataCh` is proposed as a new `PartitionQueryType` in `03-raft-fsm.md §4.5` for the long-poll fetch path. Its addition is contingent on T-005 (confirming Lookup/Update concurrency and channel-via-interface{} return safety).

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

- [ ] `QueryListAllPartitions` vs sequential lookups: decision documented with string type name if added.
- [ ] `QueryGetNewDataCh`: confirmed or alternative approach documented (depends on T-005).
- [ ] CG FSM command type names finalized (JSON string values, no opcode collision).
- [ ] All three decisions referenced in M2 FSM ticket descriptions.

## Tests required

N/A — decision ticket.

## Dependencies

T-005 (T-011's `QueryGetNewDataCh` decision depends on Lookup concurrency confirmation).

## Notes

The `03-raft-fsm.md §3.2` table uses `CommandType` as a `string` enum in the JSON envelope — `"type": "create_topic"`, etc. The `08-consumer-groups.md §14` table's "opcode" column was written in the style of binary protocols and does not apply to the JSON-encoded Metadata FSM commands. There is no numeric opcode system for MetadataCommand; the consumer group command type strings simply need to be unique within the `CommandType` enum. For `PartitionCommand` (binary prefix byte in `03-raft-fsm.md §4.2`), the opcodes `0x01` (AppendBatch) and `0x02` (RetentionConfig) are binary — confirm no third opcode is needed (since retention is decided in T-008 to be local-only, `0x03` is not needed).
