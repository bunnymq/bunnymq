# T-027: Metadata FSM — Update: consumer group commands

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** M
**Status:** TODO

## Goal

Implement `MetadataFSM.Update()` handlers for the five consumer group commands — `JoinConsumerGroup`, `LeaveConsumerGroup`, `HeartbeatConsumerGroup`, `CommitConsumerOffset`, `RebalanceConsumerGroup` — including the range-based rebalance algorithm.

## Context

Consumer group state is stored in the Metadata FSM rather than a separate service, giving it the same fault-tolerance as topic metadata. The rebalance algorithm runs deterministically inside `Update()` (no external I/O, no time.Now()). The `HeartbeatConsumerGroup` command updates `LastHeartbeatMs` from the command payload timestamp — this is the only heartbeat persistence; session timeout is enforced externally by the Group Coordinator.

References:
- [03-raft-fsm.md §3.2 — Command set (CG commands)](../../design/03-raft-fsm.md#32-command-set)
- [03-raft-fsm.md §3.3 — Range-based rebalance algorithm](../../design/03-raft-fsm.md#33-range-based-rebalance-algorithm)
- [08-consumer-groups.md §4-7](../../design/08-consumer-groups.md#4-join-flow)
- [T-011 — CG command type names](T-011-decide-fsm-additions.md)

## Scope

- Implement handler `applyJoinConsumerGroup(cmd *JoinConsumerGroupCmd) sm.Result`:
  - Creates `ConsumerGroupMeta` if group absent.
  - Assigns `member_id` = `cmd.MemberID` if non-empty; otherwise generates a server-assigned string (deterministic, e.g., `fmt.Sprintf("member-%s-%d", cmd.ClientHost, cmd.JoinedAtMs)`).
  - Adds/updates `MemberInfo{MemberID, ClientHost, SubscribedTopics, LastHeartbeatMs: cmd.JoinedAtMs}`.
  - Calls `rebalance(group, topics, partitions)`.
  - Increments `group.GenerationID`.
  - Returns `sm.Result` with `Value=0` and `Data` = JSON `{"member_id": "...", "assigned_partitions": [...], "generation_id": N}`.
- Implement handler `applyLeaveConsumerGroup(cmd *LeaveConsumerGroupCmd) sm.Result`:
  - Returns `ResultErrNotFound` if group or member absent.
  - Removes member.
  - Calls `rebalance`; increments `GenerationID`.
- Implement handler `applyHeartbeatConsumerGroup(cmd *HeartbeatConsumerGroupCmd) sm.Result`:
  - Returns `ResultErrNotFound` if group or member absent.
  - Updates `member.LastHeartbeatMs = cmd.TimestampMs`.
  - Returns `sm.Result{Value: 1, Data: ...}` if `cmd.GenerationID != group.GenerationID` (signals "rebalance needed" to the caller).
  - Returns `OKResult()` otherwise.
- Implement handler `applyCommitConsumerOffset(cmd *CommitConsumerOffsetCmd) sm.Result`:
  - Returns `ResultErrNotFound` if group absent.
  - Updates `group.CommittedOffsets[PartitionKey{topic, partitionID}] = offset` for each entry in `cmd.Offsets`.
- Implement handler `applyRebalanceConsumerGroup(cmd *RebalanceConsumerGroupCmd) sm.Result`:
  - Removes all members in `cmd.ExpiredMemberIDs`.
  - Calls `rebalance`; increments `GenerationID`.
- Implement `rebalance(group *ConsumerGroupMeta, topics map[string]*TopicMeta, partitions map[PartitionKey]*PartitionMeta)`:
  - Exactly as specified in `03-raft-fsm.md §3.3`: sort partitions (topic asc, partitionID asc), sort member IDs, range-split.

## Out of scope

- Session timeout sweep goroutine — M4 ticket.
- Group Coordinator that proposes `RebalanceConsumerGroup` — M4 ticket.

## Definition of done

- [ ] `go build ./internal/metadata/...` passes.
- [ ] `go test ./internal/metadata/...` passes.
- [ ] Rebalance produces deterministic partition assignment given same member set.
- [ ] `JoinConsumerGroup` response includes assigned partitions and new generation ID.
- [ ] `HeartbeatConsumerGroup` returns "rebalance needed" when generation ID mismatches.
- [ ] `CommitConsumerOffset` stores offsets keyed by `(topic, partition_id)`.
- [ ] No `time.Now()` inside any handler.

## Tests required

- `TestCGFSM_JoinGroup_NewGroup` — join to a group that doesn't exist; group created; member assigned all topic partitions; generation=1.
- `TestCGFSM_JoinGroup_RebalanceTwoMembers` — two joins to same group; partitions split between two members; generation=2.
- `TestCGFSM_JoinGroup_ServerAssignedID` — empty `member_id` in command; server assigns deterministic ID; returned in result.
- `TestCGFSM_LeaveGroup` — three members, one leaves; partitions rebalanced among remaining two; generation incremented.
- `TestCGFSM_Heartbeat_OK` — correct generation ID; `LastHeartbeatMs` updated; Result.Value=0.
- `TestCGFSM_Heartbeat_Stale` — stale generation ID; Result.Value=1 (rebalance needed).
- `TestCGFSM_CommitOffset` — commit offsets for 2 partitions; both retrievable from state.
- `TestCGFSM_Rebalance_Determinism` — same FSM state + same command applied to two separate FSM instances produces identical `AssignedPartitions` maps.

## Dependencies

T-025 (ConsumerGroupMeta, MemberInfo types).
T-026 (MetadataFSM struct and Update dispatcher exist).
T-011 (CG command type name strings confirmed).

## Notes

The rebalance algorithm must sort inputs before any iteration that affects output — Go's `range map` is non-deterministic. The server-assigned member ID strategy (`fmt.Sprintf(...)`) must be deterministic given the same command payload — it must not use `rand` or `time.Now()`. Using `ClientHost + JoinedAtMs` (from the command) is deterministic and unique enough for course-project scale. For course-project scale, `RebalanceConsumerGroup` is proposed by the Group Coordinator's session-timeout sweep (M4); in M2, we just implement the FSM handler.
