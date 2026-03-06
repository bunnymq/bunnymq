# T-025: Metadata FSM — types and state model

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** S
**Status:** TODO

## Goal

Define all types used by the Metadata FSM — `MetadataState`, `TopicMeta`, `PartitionMeta`, `NodeInfo`, `ConsumerGroupMeta`, `MemberInfo`, `MetadataCommand`, `MetadataQuery`, `PartitionCommand`, `PartitionQuery` — with their JSON tags and the `sm.Result` error encoding pattern.

## Context

These types are shared across `internal/raft`, `internal/metadata`, and `internal/coordinator/cluster`. Defining them in one place (the `internal/metadata` package) before writing FSM Update and Lookup methods prevents type duplication and circular imports. The `MetadataCommand` JSON field names (short single-letter aliases) are specified in `03-raft-fsm.md §3.2` and must match exactly for snapshot compatibility.

References:
- [03-raft-fsm.md §3.1 — In-memory state](../../design/03-raft-fsm.md#31-in-memory-state)
- [03-raft-fsm.md §3.2 — Command set](../../design/03-raft-fsm.md#32-command-set)
- [03-raft-fsm.md §3.4 — Lookup queries](../../design/03-raft-fsm.md#34-lookup-queries)
- [03-raft-fsm.md §4.2 — Partition command wire format](../../design/03-raft-fsm.md#42-partition-command-wire-format)

## Scope

- Create `internal/metadata/types.go` defining:
  - `MetadataState`, `TopicMeta`, `PartitionMeta`, `PartitionKey`, `NodeInfo`, `ConsumerGroupMeta`, `MemberInfo` exactly as in `03-raft-fsm.md §3.1`.
  - `CommandType string` (type alias) and constants: `CmdCreateTopic`, `CmdDeleteTopic`, `CmdAlterTopicPartCount`, `CmdAlterTopicRetention`, `CmdRegisterNode`, `CmdAssignPartitionLeader`, `CmdJoinConsumerGroup`, `CmdLeaveConsumerGroup`, `CmdHeartbeatConsumerGroup`, `CmdCommitConsumerOffset`, `CmdRebalanceConsumerGroup`.
  - `MetadataCommand` with short JSON tags (`"ct"`, `"dt"`, etc.) as in `03-raft-fsm.md §3.2`.
  - Per-command payload structs: `CreateTopicCmd`, `DeleteTopicCmd`, `AlterTopicPartCountCmd`, `AlterTopicRetentionCmd`, `RegisterNodeCmd`, `AssignPartitionLeaderCmd`, `JoinConsumerGroupCmd`, `LeaveConsumerGroupCmd`, `HeartbeatConsumerGroupCmd`, `CommitConsumerOffsetCmd`, `RebalanceConsumerGroupCmd`.
  - `MetadataQuery` with `QueryType` string and fields: `TopicName`, `PartitionID`, `GroupID`, `PartKey`.
  - `QueryType` constants: `QueryGetTopic`, `QueryListTopics`, `QueryGetPartition`, `QueryGetPartitions`, `QueryGetNode`, `QueryListNodes`, `QueryGetGroup`, `QueryGetCommittedOffset`.
- Create `internal/metadata/result.go` defining `sm.Result` error encoding:
  - `const ResultOK uint64 = 0`; error codes as constants (e.g., `ResultErrAlreadyExists`, `ResultErrNotFound`, `ResultErrInvalidArg`).
  - `func ErrorResult(code uint64, msg string) sm.Result` — encodes `sm.Result{Value: code, Data: []byte(msg)}`.
  - `func OKResult() sm.Result`.
- Create `internal/partition/types.go` (partition FSM types):
  - `PartitionCommand` with `Type uint8` (CmdAppendBatch=0x01, CmdRetentionConfig=0x02) and `Payload []byte`.
  - `PartitionQuery` with `PartitionQueryType`, `Offset int64`, `TimestampMs int64`, `MaxBytes int`.
  - `PartitionQueryType` constants: `QueryRead`, `QueryReadByTime`, `QueryEarliestOffset`, `QueryLatestOffset`.
  - `RetentionConfigPayload` JSON struct `{"retention_ms": int64, "retention_bytes": int64}`.

## Out of scope

- FSM Update logic — T-026, T-027.
- FSM Lookup logic — T-028.
- Snapshot logic — T-029.

## Definition of done

- [ ] `go build ./internal/metadata/... ./internal/partition/...` passes.
- [ ] All `MetadataCommand` JSON field names match `03-raft-fsm.md §3.2` exactly.
- [ ] `PartitionCommand` wire format: byte 0 = type, bytes [1:] = payload.
- [ ] `sm.Result` error encoding: Value = error code, Data = message bytes.

## Tests required

- `TestMetadataCommand_JSON` — marshal and unmarshal a `CreateTopicCmd` command; verify JSON field names are short aliases (`"type"`, `"ct"`); zero fields are omitted (`omitempty`).
- `TestPartitionCommand_Wire` — `PartitionCommand{Type: CmdAppendBatch, Payload: []byte("abc")}` serializes to `[0x01, 'a', 'b', 'c']`.
- `TestResultEncoding` — `ErrorResult(ResultErrAlreadyExists, "topic exists").Value == ResultErrAlreadyExists`.

## Dependencies

T-012 (package stubs exist).
T-011 (CG command type names decided — JSON string constants confirmed, no binary opcode collision).

## Notes

Place all Metadata FSM types in `internal/metadata` (not `internal/raft`) to keep `internal/raft` free of application-specific types. The `internal/raft` package imports `internal/metadata` and `internal/partition` for type definitions; no other package should import `internal/raft` except coordinators and API servers. `PartitionKey` must implement `encoding.TextMarshaler`/`TextUnmarshaler` if used as a JSON map key (Go maps require string keys in JSON); alternatively, store `CommittedOffsets` as `[]CommittedOffset` structs in the JSON snapshot.
