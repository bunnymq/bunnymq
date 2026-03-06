# T-052: Group Coordinator — Heartbeat handler and session timeout sweep

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Add the `Heartbeat` handler (in-memory, no Raft write) and the `sessionTimeoutSweep` background goroutine to `GroupCoordinator`; the sweep evicts members whose heartbeats are overdue by proposing `LeaveConsumerGroupCmd` and runs only when this node is the metadata shard leader.

## Context

Heartbeats are the liveness signal for group members. The handler is deliberately not a Raft write — it only updates the in-memory `lastHeartbeat` table. The sweep goroutine is responsible for evicting timed-out members by issuing Raft commits, ensuring the eviction is durable. The sweep must guard against running on a non-leader node to avoid proposing commands that the leader will reject. On leader election (coordinator startup or failover), the `lastHeartbeat` table is seeded with `now` for all current members, giving them a full `session_timeout_ms` window before the first possible eviction.

References:
- [08-consumer-groups.md §8 — Heartbeat flow](../../design/08-consumer-groups.md#8-heartbeat-flow)
- [08-consumer-groups.md §9 — Session timeout enforcement](../../design/08-consumer-groups.md#9-session-timeout-enforcement)
- [08-consumer-groups.md §15 — Concurrency model](../../design/08-consumer-groups.md#15-concurrency-model)
- [08-consumer-groups.md §18 OQ5 — VERIFY GetLeaderID API](../../design/08-consumer-groups.md#18-open-questions)

## Scope

- Add to `internal/cluster/groupcoord.go` (extending T-051):
  - `Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error)`:
    1. `LookupMetadata(QueryGetGroup{GroupID})` — if nil or member not in group, return `NOT_GROUP_MEMBER`.
    2. Validate `req.GenerationID <= currentGroup.GenerationID` (stale client still gets a response, but `rebalance_required` will be true).
    3. Acquire `heartbeatMu.Lock()`; set `lastHeartbeat[GroupID][MemberID] = time.Now()`. Release lock.
    4. Return `HeartbeatResponse{RebalanceRequired: currentGroup.GenerationID > req.GenerationID}`.
  - `Start(ctx context.Context)` — starts the `sessionTimeoutSweep` goroutine; stops when ctx is cancelled.
  - `RebuildHeartbeatTable()` — called on coordinator startup or after leader election: `LookupMetadata(QueryGetGroup{""})` to enumerate all groups; for each member set `lastHeartbeat[g][m] = time.Now()`.
  - `sessionTimeoutSweep(ctx context.Context)` goroutine:
    ```
    ticker := time.NewTicker(sweepIntervalMs)
    for { select { case <-ticker.C: sweepOnce(ctx); case <-ctx.Done(): return } }
    ```
  - `sweepOnce(ctx context.Context)`:
    - **VERIFY:** Guard with `nh.GetLeaderID(MetadataShardID)` — run sweep only if `valid && leaderID == thisNodeID`. Mark as VERIFY until dragonboat v4 API confirmed.
    - Snapshot all groups from FSM via `LookupMetadata(QueryGetAllGroups{})` (add new query type to T-049 scope — see Notes).
    - Acquire `heartbeatMu.RLock()`; check each member's `lastHeartbeat`; release.
    - For each timed-out member (now − lastHeartbeat > member.SessionTimeoutMs): compute updated assignment (remove member, call `rangeAssign`); `SyncProposeMetadata(LeaveConsumerGroupCmd{…, Reason: "timeout", NewAssignment})`.
    - After proposing eviction: acquire `heartbeatMu.Lock()`; delete `lastHeartbeat[g][m]`; release.

## Out of scope

- JoinGroup / LeaveGroup — T-051.
- CommitOffset / FetchCommittedOffsets — T-053.
- gRPC handler wiring — T-054.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] `Heartbeat` for unknown member returns `NOT_GROUP_MEMBER`.
- [ ] `Heartbeat` updates `lastHeartbeat` table.
- [ ] `Heartbeat` returns `rebalance_required=false` when `req.GenerationID == current.GenerationID`.
- [ ] `Heartbeat` returns `rebalance_required=true` when `req.GenerationID < current.GenerationID`.
- [ ] `sweepOnce` evicts member whose heartbeat is overdue; `LeaveConsumerGroupCmd` proposed with correct `NewAssignment`.
- [ ] `sweepOnce` does NOT evict member whose heartbeat is current.
- [ ] `RebuildHeartbeatTable` seeds all current members with `now`.
- [ ] `sweepOnce` does not propose commands when this node is not leader (leader guard respected).

## Tests required

- `TestHeartbeat_UpdatesTable` — Heartbeat for known member; `lastHeartbeat` timestamp advances.
- `TestHeartbeat_NotMember` — member not in group; `NOT_GROUP_MEMBER` error.
- `TestHeartbeat_RebalanceRequired_True` — FSM has GenerationID=2; client sends GenerationID=1; response `rebalance_required=true`.
- `TestHeartbeat_RebalanceRequired_False` — generations match; `rebalance_required=false`.
- `TestSweepOnce_EjectsTimedOut` — member last heartbeat set to `now - 2*SessionTimeoutMs`; sweep proposes `LeaveConsumerGroupCmd`.
- `TestSweepOnce_SkipsFreshMember` — member last heartbeat set to `now`; sweep proposes nothing.
- `TestSweepOnce_NotLeader_NoProposal` — node is not leader (stub `GetLeaderID` returns different ID); sweep proposes nothing.
- `TestRebuildHeartbeatTable` — FSM has 2 groups × 2 members; after rebuild, all 4 entries present with recent timestamp.

## Dependencies

- T-051 (GroupCoordinator struct, lastHeartbeat table, nodeHostIface).
- T-049 (QueryGetGroup; also requires adding `QueryGetAllGroups{}` query — see Notes).
- T-050 (rangeAssign — needed for eviction new-assignment computation).

## Notes

The sweep needs to enumerate all groups from the FSM. Add `QueryGetAllGroups{}` → `map[string]*GroupState` to the query types in T-049's scope (small addition; add a test for it in T-049 if possible, or add it here as a coordinator-test-level query). The VERIFY marker on `GetLeaderID` is the same as the one in T-040 (ClusterCoordinator leaderSweep); both must be resolved before the sweep goroutine is implemented. If `nh.GetLeaderID` is confirmed working, the same pattern is reused here. The `sweepOnce` function acquires `RLock` for the read pass and `Lock` only at eviction time, per the concurrency model in §15.
