# T-026: Metadata FSM — Update: topic and node commands

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** M
**Status:** TODO

## Goal

Implement `MetadataFSM.Update()` in `internal/metadata` for the six topology-management commands: `CreateTopic`, `DeleteTopic`, `AlterTopicPartCount`, `AlterTopicRetention`, `RegisterNode`, and `AssignPartitionLeader`, including the `NextShardID` counter and the `sm.Result` error encoding pattern.

## Context

The Metadata FSM is the source of truth for all cluster topology. `CreateTopic` is the most complex command: it assigns shard IDs, validates replica counts, and stores both `TopicMeta` and N `PartitionMeta` entries atomically. All commands must be deterministic (no time.Now(), no external I/O) and express logical errors through `sm.Result`, never through error returns.

References:
- [03-raft-fsm.md §3.2 — Command set (table)](../../design/03-raft-fsm.md#32-command-set)
- [03-raft-fsm.md §3.6 — Determinism rules](../../design/03-raft-fsm.md#36-determinism-rules)

## Scope

- Implement `MetadataFSM` struct in `internal/metadata/fsm.go`:
  - Fields: `state *MetadataState`.
- Implement `(*MetadataFSM).Update(e sm.Entry) (sm.Result, error)` (the `IStateMachine` method):
  - JSON-unmarshal `e.Cmd` into `MetadataCommand`.
  - Dispatch on `cmd.Type` to the appropriate handler.
  - Return `ErrorResult(...)` for all application-level errors; never return a non-nil Go error.
- Implement handler `applyCreateTopic(cmd *CreateTopicCmd) sm.Result`:
  - Returns `ErrorResult(ResultErrAlreadyExists, ...)` if topic exists.
  - Validates `partition_count > 0` and `replication_factor > 0`; returns `ResultErrInvalidArg` if not.
  - Validates `len(replica_node_ids) == partition_count`; returns `ResultErrInvalidArg` if not.
  - Stores `TopicMeta`.
  - For each partition `i`: stores `PartitionMeta{ShardID: state.NextShardID, ReplicaNodeIDs: replica_node_ids[i]}`.
  - Increments `state.NextShardID` by `partition_count`.
  - Returns `OKResult()`.
- Implement handler `applyDeleteTopic(cmd *DeleteTopicCmd) sm.Result`:
  - No-op if topic absent.
  - Deletes `TopicMeta` and all `PartitionMeta` entries for the topic.
- Implement handler `applyAlterTopicPartCount(cmd *AlterTopicPartCountCmd) sm.Result`:
  - Returns `ResultErrNotFound` if topic absent.
  - Returns `ResultErrInvalidArg` if `new_partition_count <= current`.
  - Appends new `PartitionMeta` entries; increments `NextShardID`.
  - Updates `TopicMeta.PartitionCount`.
- Implement handler `applyAlterTopicRetention(cmd *AlterTopicRetentionCmd) sm.Result`:
  - Returns `ResultErrNotFound` if topic absent.
  - Updates `TopicMeta.RetentionMs` and `RetentionBytes`.
- Implement handler `applyRegisterNode(cmd *RegisterNodeCmd) sm.Result`:
  - Idempotent: upserts `NodeInfo`. Returns `OKResult()`.
- Implement handler `applyAssignPartitionLeader(cmd *AssignPartitionLeaderCmd) sm.Result`:
  - Returns `ResultErrNotFound` if partition absent.
  - Returns `ResultErrInvalidArg` if `leader_epoch <= current`.
  - Updates `PartitionMeta.LeaderNodeID` and `LeaderEpoch`.

## Out of scope

- Consumer group commands — T-027.
- Lookup paths — T-028.
- Snapshot — T-029.

## Definition of done

- [ ] `go build ./internal/metadata/...` passes.
- [ ] `go test ./internal/metadata/...` passes.
- [ ] `CreateTopic` assigns contiguous shard IDs starting at `NextShardID`; `NextShardID` increments by `partition_count`.
- [ ] `CreateTopic` on duplicate topic returns `ResultErrAlreadyExists`; state unchanged.
- [ ] `AssignPartitionLeader` rejects stale epochs.
- [ ] No call to `time.Now()` inside any handler.
- [ ] Invalid command payload (JSON decode error) returns `ErrorResult(ResultErrInvalidArg, ...)`, does NOT panic.

## Tests required

- `TestMetadataFSM_CreateTopic` — create topic with 3 partitions; verify 3 PartitionMeta entries with ShardIDs 1, 2, 3 (NextShardID started at 1); NextShardID = 4.
- `TestMetadataFSM_CreateTopic_Duplicate` — create same topic twice; second returns ResultErrAlreadyExists; state has exactly one topic.
- `TestMetadataFSM_DeleteTopic` — create then delete; state has no topic or partition entries.
- `TestMetadataFSM_AlterPartCount_Invalid` — new_count < current → ResultErrInvalidArg; state unchanged.
- `TestMetadataFSM_AlterRetention` — update retention; TopicMeta reflects new values.
- `TestMetadataFSM_RegisterNode_Idempotent` — register same node twice; state has exactly one NodeInfo.
- `TestMetadataFSM_AssignLeader_StaleEpoch` — assign leader with epoch=5 then epoch=3; second returns ResultErrInvalidArg; leader still from first assignment.
- `TestMetadataFSM_BadJSON` — `Update` with malformed JSON; returns ErrorResult; state unchanged.

## Dependencies

T-025 (MetadataState types, sm.Result helpers).
T-012 (package stub).

## Notes

`IStateMachine.Update(e sm.Entry) (sm.Result, error)` — note the dragonboat v4 signature takes a single `sm.Entry` (not a slice). Verify this against the actual dragonboat v4 API; the design doc may reference a batch-entry variant. If dragonboat v4 passes a slice `[]sm.Entry`, process each entry in order and return the last `sm.Result`. This is a detail to confirm via T-001/T-005 VERIFY results. Keep all handler methods on the same file for easy side-by-side review of determinism.
