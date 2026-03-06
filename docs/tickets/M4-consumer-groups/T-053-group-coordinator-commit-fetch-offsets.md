# T-053: Group Coordinator — CommitOffset and FetchCommittedOffsets

**Milestone:** M4 — Consumer groups
**Effort:** S
**Status:** TODO

## Goal

Add `CommitOffset` and `FetchCommittedOffsets` handlers to `GroupCoordinator`; `CommitOffset` validates membership + generation + assignment ownership before issuing a Raft command; `FetchCommittedOffsets` is a pure FSM read with no Raft write.

## Context

Offset commits must be durable (replicated via Raft) so they survive coordinator failover and broker restart. Offset fetches are read-only — no Raft round-trip — because the committed offsets already live in the Metadata FSM. Both operations enforce membership integrity to prevent stale clients from corrupting the group's offset state.

References:
- [08-consumer-groups.md §12 — Offset commit flow](../../design/08-consumer-groups.md#12-offset-commit-flow)
- [08-consumer-groups.md §13 — Offset fetch flow](../../design/08-consumer-groups.md#13-offset-fetch-flow)
- [08-consumer-groups.md §16 — Failure scenarios (idempotent commit)](../../design/08-consumer-groups.md#16-failure-scenarios)

## Scope

- Add to `internal/cluster/groupcoord.go` (extending T-051):
  - `CommitOffset(ctx context.Context, req CommitOffsetRequest) error`:
    1. `LookupMetadata(QueryGetGroup{GroupID})` — nil → `NOT_GROUP_MEMBER`.
    2. Validate `req.MemberID` in `group.Members` — not found → `NOT_GROUP_MEMBER`.
    3. Validate `req.GenerationID == group.GenerationID` — mismatch → return `STALE_GENERATION` error.
    4. Validate each `(topic, partitionID)` in `req.Offsets` is in `group.Assignments[MemberID]` — any missing → `INVALID_ARGUMENT`.
    5. `SyncProposeMetadata(CommitConsumerOffsetCmd{GroupID, Offsets: req.Offsets})`.
    6. Return nil on commit.
  - `FetchCommittedOffsets(ctx context.Context, req FetchCommittedOffsetsRequest) (map[TopicPartition]int64, error)`:
    1. `LookupMetadata(QueryGetGroupOffsets{GroupID, Partitions: req.Partitions})`.
    2. Return the map directly; missing partitions already return `-1` per T-049 query semantics.
  - Error sentinel types in `internal/cluster/errors.go` (or existing errors file):
    - `ErrStaleGeneration` — returned when `CommitOffset` generationID mismatches.
    - `ErrNotGroupMember` — returned when member not found in group.

## Out of scope

- JoinGroup / LeaveGroup — T-051.
- Heartbeat / sweep — T-052.
- gRPC handler wiring (converting `ErrStaleGeneration` → gRPC `FAILED_PRECONDITION`) — T-054.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] `CommitOffset` with matching generationID and valid partitions → `CommitConsumerOffsetCmd` proposed; offsets readable via `FetchCommittedOffsets`.
- [ ] `CommitOffset` with stale generationID → `ErrStaleGeneration`.
- [ ] `CommitOffset` with partition not in member's assignment → `INVALID_ARGUMENT` error.
- [ ] `CommitOffset` for unknown member → `ErrNotGroupMember`.
- [ ] `FetchCommittedOffsets` for committed partition → correct offset value.
- [ ] `FetchCommittedOffsets` for uncommitted partition → `-1`.
- [ ] `CommitOffset` is idempotent: committing the same offsets twice produces correct result (second commit overwrites with same values, no error).

## Tests required

- `TestCommitOffset_Success` — member present, correct generationID, assigned partitions; offsets committed.
- `TestCommitOffset_StaleGeneration` — generationID one below current; `ErrStaleGeneration` returned.
- `TestCommitOffset_NotMember` — unknown member_id; `ErrNotGroupMember` returned.
- `TestCommitOffset_UnassignedPartition` — partition not in member's assignment; `INVALID_ARGUMENT` returned.
- `TestCommitOffset_Idempotent` — two identical commit calls; second succeeds and offsets unchanged.
- `TestFetchCommittedOffsets_ReturnsCommitted` — commit offset 42 for partition 0; fetch returns 42.
- `TestFetchCommittedOffsets_MissingReturnsNegativeOne` — partition never committed; fetch returns -1.

## Dependencies

- T-051 (GroupCoordinator struct, nodeHostIface).
- T-049 (CommitConsumerOffsetCmd, QueryGetGroupOffsets).

## Notes

The `FetchCommittedOffsets` handler does not validate group membership — any caller can read committed offsets for any group. This matches the Kafka semantics used for consumer group lag monitoring. The `STALE_GENERATION` error maps to gRPC `FAILED_PRECONDITION` with `BunnyErrorDetail{code=STALE_GENERATION}` — that mapping is done in T-054. The `CommitOffset` idempotency guarantee ("same offsets committed twice is harmless — the FSM takes the max") from §16 is enforced by the FSM overwriting: if the design requires max semantics rather than overwrite, update the `CommitConsumerOffsetCmd` application in T-049 accordingly and add a test.
