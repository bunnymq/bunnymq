# T-028: Metadata FSM — Lookup paths

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** S
**Status:** TODO

## Goal

Implement `MetadataFSM.Lookup()` in `internal/metadata` for all eight query types, dispatching on `MetadataQuery.Type` and returning typed values that callers can type-assert.

## Context

`Lookup` is the read path for all topology data. It is called by `raft.Host.LookupMetadata`, which passes the result directly to callers (ClusterCoordinator, DataCoordinator, API servers). Per dragonboat's contract, `Lookup` on `IStateMachine` does NOT execute concurrently with `Update` — dragonboat serialises them. Therefore no locks are needed inside `MetadataFSM.Lookup`.

References:
- [03-raft-fsm.md §3.4 — Lookup queries](../../design/03-raft-fsm.md#34-lookup-queries)

## Scope

- Implement `(*MetadataFSM).Lookup(query interface{}) (interface{}, error)`:
  - Type-asserts `query` to `MetadataQuery`; returns error if wrong type.
  - Dispatches on `q.Type`:

  | Query | Returns | Behaviour |
  |-------|---------|-----------|
  | `QueryGetTopic` | `*TopicMeta, error` | Returns `ErrNotFound` if absent. |
  | `QueryListTopics` | `[]*TopicMeta` | Returns snapshot of all topics; sorted by name for determinism. |
  | `QueryGetPartition` | `*PartitionMeta, error` | Returns `ErrNotFound` if absent. |
  | `QueryGetPartitions` | `[]*PartitionMeta, error` | All partitions for `q.TopicName`; returns `ErrNotFound` if topic absent. Sorted by `PartitionID`. |
  | `QueryGetNode` | `*NodeInfo, error` | Returns `ErrNotFound` if absent. |
  | `QueryListNodes` | `[]*NodeInfo` | All nodes; sorted by `NodeID`. |
  | `QueryGetGroup` | `*ConsumerGroupMeta, error` | Returns `ErrNotFound` if absent. |
  | `QueryGetCommittedOffset` | `int64, error` | Returns offset for `q.PartKey` in `q.GroupID`; returns 0 and no error if no committed offset exists. |

- Define sentinel `var ErrNotFound = errors.New("not found")` in `internal/metadata`.
- Return shallow copies (not references into the FSM state map) to prevent callers from accidentally mutating FSM state.

## Out of scope

- Mutations via Update — T-026, T-027.
- PartitionFSM Lookup — T-031.

## Definition of done

- [ ] `go build ./internal/metadata/...` passes.
- [ ] `go test ./internal/metadata/...` passes.
- [ ] `QueryListTopics` returns sorted slice (reproducible for test assertions).
- [ ] `QueryGetPartition` returns `ErrNotFound` for unknown topic or partition.
- [ ] `QueryGetCommittedOffset` returns 0 (not error) when offset not committed.
- [ ] Results are copies, not pointers into FSM state (mutation test: caller modifies result; FSM state unchanged).

## Tests required

- `TestMetadataLookup_GetTopic` — create topic via Update; Lookup returns correct TopicMeta.
- `TestMetadataLookup_GetTopic_NotFound` — lookup absent topic returns ErrNotFound.
- `TestMetadataLookup_ListTopics_Sorted` — create 3 topics; ListTopics returns them sorted by name.
- `TestMetadataLookup_GetPartition` — create topic with 2 partitions; GetPartition(partitionID=1) returns correct PartitionMeta.
- `TestMetadataLookup_GetPartitions` — GetPartitions for topic returns all N partitions sorted by PartitionID.
- `TestMetadataLookup_GetGroup` — join a group; GetGroup returns ConsumerGroupMeta with member.
- `TestMetadataLookup_CommittedOffset_Zero` — group exists but no committed offsets; returns 0, no error.
- `TestMetadataLookup_ResultIsCopy` — get topic, modify returned pointer; call Lookup again; original state unchanged.

## Dependencies

T-025 (types, ErrNotFound sentinel).
T-026 (MetadataFSM struct and Update handlers populate state for lookup tests).

## Notes

Return value type-safety: callers do `result.(*TopicMeta)` after `LookupMetadata`. For `QueryListTopics` and `QueryListNodes` the return type is `[]*TopicMeta` and `[]*NodeInfo` — the slice itself is a copy even if the pointers inside point into FSM maps. Consider returning deep copies if the coordinator might hold references long enough for the FSM to mutate the underlying structs. For course-project simplicity, a shallow copy (new slice, same pointers) is acceptable if coordinators use the result synchronously before proposing new commands.
