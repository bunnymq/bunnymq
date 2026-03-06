# T-049: Metadata FSM — consumer group commands and queries

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Extend the Metadata FSM with three new write commands (`JoinConsumerGroupCmd`, `LeaveConsumerGroupCmd`, `CommitConsumerOffsetCmd`) and two new read queries (`QueryGetGroup`, `QueryGetGroupOffsets`), and add the `GroupState` / `MemberState` / `TopicPartition` types to the FSM's type set.

## Context

All consumer group state lives in the Metadata FSM so it is replicated and survives coordinator failover. The group state model (`GroupState`, `MemberState`, `TopicPartition`) is defined in `08-consumer-groups.md §2`; the command opcodes are defined in `08-consumer-groups.md §14`. These commands extend the FSM implemented in T-027 (Update) and T-028 (Lookup); this ticket adds new cases to those files without modifying existing logic.

References:
- [08-consumer-groups.md §2 — Group state model](../../design/08-consumer-groups.md#2-group-state-model)
- [08-consumer-groups.md §14 — Metadata FSM commands](../../design/08-consumer-groups.md#14-metadata-fsm-commands-consumer-group-set)
- [03-raft-fsm.md §4 — MetadataFSM snapshot format](../../design/03-raft-fsm.md#4-metadatafsm)

## Scope

- Add to `internal/cluster/metadata_types.go` (or `metadata.go`):
  - `GroupState` struct: `GroupID string`, `Members map[string]MemberState`, `GenerationID int32`, `Assignments map[string][]TopicPartition`, `Offsets map[TopicPartition]int64`.
  - `MemberState` struct: `MemberID string`, `SubscribedTopics []string`, `SessionTimeoutMs int32`, `HeartbeatIntervalMs int32`, `JoinedAt time.Time`.
  - `TopicPartition` struct: `Topic string`, `PartitionID int32`.
- Add to `internal/cluster/metadata_commands.go` (alongside existing commands):
  - `JoinConsumerGroupCmd` — opcode `0x0A` (reconcile against `03-raft-fsm.md` opcode table; use next free value if taken): fields `GroupID`, `MemberID`, `MemberState`, `NewAssignment map[string][]TopicPartition`.
  - `LeaveConsumerGroupCmd` — opcode `0x0B`: fields `GroupID`, `MemberID`, `Reason string`.
  - `CommitConsumerOffsetCmd` — opcode `0x0C`: fields `GroupID`, `Offsets map[TopicPartition]int64`.
- Add new `switch` cases to the MetadataFSM `Update` method (T-027's file):
  - `JoinConsumerGroupCmd`: upsert member in `fsm.groups[GroupID]`; update `Assignments`; increment `GenerationID`.
  - `LeaveConsumerGroupCmd`: remove member from `fsm.groups[GroupID]`; update `Assignments` from `cmd.NewAssignment` (pre-computed by coordinator); increment `GenerationID`. If group becomes empty, optionally remove it.
  - `CommitConsumerOffsetCmd`: merge `cmd.Offsets` into `GroupState.Offsets` (overwrite each key).
- Add new query types to `internal/cluster/metadata_queries.go` (T-028's file):
  - `QueryGetGroup{GroupID string}` → returns `*GroupState` (nil if not found).
  - `QueryGetGroupOffsets{GroupID string, Partitions []TopicPartition}` → returns `map[TopicPartition]int64`; missing partitions get value `-1`.
- Update MetadataFSM JSON snapshot save/restore (`SaveSnapshot`/`RecoverFromSnapshot`) to include `groups map[string]*GroupState`.

## Out of scope

- The GroupCoordinator business logic that proposes these commands — T-051, T-052, T-053.
- Range assignment computation — T-050.
- Server-side gRPC handlers — T-054.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] `JoinConsumerGroupCmd`: FSM upserts member, increments `GenerationID`, stores new assignment.
- [ ] `LeaveConsumerGroupCmd`: FSM removes member, increments `GenerationID`, stores updated assignment.
- [ ] `CommitConsumerOffsetCmd`: FSM merges offsets; existing offsets for other partitions are preserved.
- [ ] `QueryGetGroup`: returns nil for unknown group; returns populated `*GroupState` for known group.
- [ ] `QueryGetGroupOffsets`: missing partition returns `-1`; committed partition returns correct offset.
- [ ] Snapshot round-trip: save with 2 groups + offsets; restore; both groups with all fields intact.

## Tests required

- `TestMetadataFSM_JoinGroup_NewGroup` — apply `JoinConsumerGroupCmd` to empty FSM; group created with GenerationID=1, member present, assignment stored.
- `TestMetadataFSM_JoinGroup_ExistingGroup` — second `JoinConsumerGroupCmd`; GenerationID=2, both members present.
- `TestMetadataFSM_LeaveGroup` — join then leave; GenerationID=2, member removed, assignment updated.
- `TestMetadataFSM_LeaveGroup_LastMember` — last member leaves; group either empty or deleted (consistent behaviour documented in test comment).
- `TestMetadataFSM_CommitOffset` — `CommitConsumerOffsetCmd` with 3 partitions; `QueryGetGroupOffsets` returns all 3 correct values.
- `TestMetadataFSM_QueryGetGroup_NotFound` — unknown group → nil.
- `TestMetadataFSM_QueryGetGroupOffsets_MissingPartition` — offset for unset partition returns -1.
- `TestMetadataFSM_GroupSnapshot` — join 2 groups, commit offsets, save snapshot, restore into new FSM, verify groups intact.

## Dependencies

- T-027 (MetadataFSM Update — file and types to extend).
- T-028 (MetadataFSM Lookup — query dispatch to extend).

## Notes

The `NewAssignment` field in `LeaveConsumerGroupCmd` carries the pre-computed assignment from the coordinator (T-050). The FSM does not compute assignments; it only stores what the coordinator sends. This keeps the FSM deterministic and free of external dependencies (like `DescribeTopic` lookups). The opcode allocation `0x0A/0x0B/0x0C` is a suggestion from the design doc; verify against the existing opcode table in `03-raft-fsm.md §4` before assigning — if those opcodes are taken, use the next available values and document the choice.
