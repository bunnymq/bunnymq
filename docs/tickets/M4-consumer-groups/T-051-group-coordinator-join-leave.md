# T-051: Group Coordinator — JoinGroup and LeaveGroup handlers

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Implement the `GroupCoordinator` struct and its `JoinGroup` and `LeaveGroup` handlers, including validation, member-ID UUID assignment, partition-assignment computation via `rangeAssign`, and `SyncProposeMetadata` calls.

## Context

The Group Coordinator is the server-side component that serialises group membership changes. It owns the `lastHeartbeat` table (used by T-052's sweep goroutine) and provides the five group-RPC handlers called by the DataServer (T-054). `JoinGroup` and `LeaveGroup` are the two write-heavy handlers that trigger rebalance: both validate inputs, compute the new assignment, and issue a Raft command. The coordinator always runs on the metadata shard leader; non-leaders return `NOT_LEADER`.

References:
- [08-consumer-groups.md §4 — Member ID assignment](../../design/08-consumer-groups.md#4-member-id-assignment)
- [08-consumer-groups.md §5 — Generation ID](../../design/08-consumer-groups.md#5-generation-id)
- [08-consumer-groups.md §7 — JoinGroup flow](../../design/08-consumer-groups.md#7-joingroup-flow)
- [08-consumer-groups.md §10 — LeaveGroup flow](../../design/08-consumer-groups.md#10-leavegroup-flow)
- [08-consumer-groups.md §15 — Concurrency model](../../design/08-consumer-groups.md#15-concurrency-model)

## Scope

- Create `internal/cluster/groupcoord.go`:
  - `GroupCoordinatorConfig` struct: `MetadataShardID uint64`, `ThisNodeID uint64`, `SessionTimeoutMinMs int32` (default 1000), `SessionTimeoutMaxMs int32` (default 300000), `SweepIntervalMs int64` (default 5000).
  - `GroupCoordinator` struct:
    - `config GroupCoordinatorConfig`
    - `nh nodeHostIface` (same interface as ClusterCoordinator uses for SyncProposeMetadata / LookupMetadata)
    - `heartbeatMu sync.RWMutex`
    - `lastHeartbeat map[string]map[string]time.Time` // group → member → last heartbeat time
  - `NewGroupCoordinator(config GroupCoordinatorConfig, nh nodeHostIface) *GroupCoordinator`.
  - `JoinGroup(ctx context.Context, req JoinGroupRequest) (JoinGroupResponse, error)`:
    1. Validate `req.SessionTimeoutMs` within `[SessionTimeoutMinMs, SessionTimeoutMaxMs]`.
    2. For each topic in `req.Topics`: `LookupMetadata(QueryGetTopic{topic})` — if not found, return `NOT_FOUND` error.
    3. Look up current group state: `LookupMetadata(QueryGetGroup{GroupID})`.
    4. If group exists and has members: validate all existing members subscribe to the same topic set as `req.Topics`; if not, return `INVALID_ARGUMENT` (mixed subscriptions).
    5. If `req.MemberID == ""`: assign `uuid.NewString()`.
    6. Collect current member IDs + new member ID → call `rangeAssign`.
    7. Build `MemberState{MemberID, SubscribedTopics: req.Topics, SessionTimeoutMs, HeartbeatIntervalMs, JoinedAt: time.Now()}`.
    8. `SyncProposeMetadata(JoinConsumerGroupCmd{...})`.
    9. On commit: `LookupMetadata(QueryGetGroup{GroupID})` to read new `GenerationID` and `Assignments`.
    10. Update `lastHeartbeat[GroupID][memberID] = time.Now()` (write lock).
    11. Return `JoinGroupResponse{MemberID, GenerationID, Assignments[memberID]}`.
  - `LeaveGroup(ctx context.Context, req LeaveGroupRequest) error`:
    1. `LookupMetadata(QueryGetGroup{GroupID})` — if nil, return `NOT_GROUP_MEMBER`.
    2. Validate `req.MemberID` is in `group.Members`; if not, return `NOT_GROUP_MEMBER`.
    3. Compute updated member list (current minus req.MemberID) → `rangeAssign` for new assignment.
    4. `SyncProposeMetadata(LeaveConsumerGroupCmd{GroupID, MemberID, Reason: "voluntary", NewAssignment: newAssignment})`.
    5. Remove `lastHeartbeat[GroupID][MemberID]` (write lock).
    6. Return nil on commit.
  - `GroupCoordinatorIface` interface for test doubles: `JoinGroup`, `LeaveGroup`, `Heartbeat`, `CommitOffset`, `FetchCommittedOffsets`.

## Out of scope

- Heartbeat and session timeout sweep — T-052.
- CommitOffset / FetchCommittedOffsets — T-053.
- gRPC handler wiring — T-054.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] `JoinGroup` with empty `MemberID` assigns a UUID.
- [ ] `JoinGroup` with known `MemberID` reuses it (re-join after rebalance).
- [ ] `JoinGroup` for unknown topic returns `NOT_FOUND` error.
- [ ] `JoinGroup` with mixed topic subscriptions (existing member has different topics) returns `INVALID_ARGUMENT`.
- [ ] `JoinGroup` increments `GenerationID` on each call.
- [ ] `LeaveGroup` for non-member returns `NOT_GROUP_MEMBER` error.
- [ ] `LeaveGroup` removes member from `lastHeartbeat` table.
- [ ] After `JoinGroup`, `lastHeartbeat[groupID][memberID]` is set.

## Tests required

- `TestGroupCoordinator_JoinGroup_NewMemberID` — empty member_id in request; response contains a non-empty UUID.
- `TestGroupCoordinator_JoinGroup_ReusesMemberID` — non-empty member_id; response echoes it.
- `TestGroupCoordinator_JoinGroup_UnknownTopic` — topic not in FSM; error returned.
- `TestGroupCoordinator_JoinGroup_MixedSubscriptions` — existing member has topic set A; new member requests topic set B; `INVALID_ARGUMENT` error.
- `TestGroupCoordinator_JoinGroup_GenerationIncrements` — two sequential JoinGroup calls; response GenerationID increments from 1 to 2.
- `TestGroupCoordinator_JoinGroup_AssignmentCoverage` — 4-partition topic, 2 members joining sequentially; second response shows 2 partitions each.
- `TestGroupCoordinator_LeaveGroup_NotMember` — member not in group; `NOT_GROUP_MEMBER` returned.
- `TestGroupCoordinator_LeaveGroup_RemovesFromHeartbeat` — after leave, `lastHeartbeat[g][m]` absent.

## Dependencies

- T-049 (FSM commands, GroupState type, QueryGetGroup).
- T-050 (rangeAssign function).
- T-039 (nodeHostIface — SyncProposeMetadata, LookupMetadata interface already defined there).

## Notes

Stub the `nodeHostIface` in tests using a simple in-memory struct that applies FSM commands directly — same pattern used in T-039 tests. `uuid.NewString()` requires `github.com/google/uuid` — check if it is already a project dependency before adding. The `GroupCoordinator` struct does not start any background goroutines itself; the sweep goroutine is added in T-052.
