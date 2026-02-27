# Module 8: Consumer Groups

The Group Coordinator implements consumer group membership, partition assignment, heartbeat tracking, and offset storage. All group state lives in the Metadata FSM so it is replicated across the cluster and survives coordinator failover. In v1 there is exactly one group coordinator: the leader of the metadata shard. Clients discover it via `DescribeCluster` and connect directly.

---

## 1. Responsibilities

**Does:**
- Accept `JoinGroup`, `Heartbeat`, `LeaveGroup`, `CommitOffset`, `FetchCommittedOffsets` RPCs on the Data API (same port as Produce/Fetch, port `:9092`).
- Maintain group state (`members`, `generationID`, `assignments`, committed offsets) durably in the Metadata FSM via Raft commands.
- Compute range-based partition assignments on every membership change.
- Enforce session timeouts: remove members whose heartbeats have been absent for more than `session_timeout_ms` via a background sweep.
- Signal rebalance to live members by returning `rebalance_required=true` in the `Heartbeat` response.

**Does not:**
- Broker data reads or writes (those go through the Data Coordinator).
- Forward group RPCs to the metadata shard leader - it returns `NOT_LEADER` and the client connects directly to the leader.
- Implement sticky assignment, cooperative rebalance, static membership, or per-group configuration overrides (see §10 Limitations).

---

## 2. Group State Model

Group state is stored in the Metadata FSM under the key `group:<group_id>`. Each group entry holds:

```go
type GroupState struct {
    GroupID       string
    Members       map[string]MemberState // member_id → state
    GenerationID  int32
    Assignments   map[string][]TopicPartition // member_id → assigned partitions
    Offsets       map[TopicPartition]int64    // (topic, partition_id) → committed offset
}

type MemberState struct {
    MemberID            string
    SubscribedTopics    []string
    SessionTimeoutMs    int32
    HeartbeatIntervalMs int32
    // Last heartbeat time is NOT stored in the FSM - it is tracked in the
    // coordinator's in-memory sweep table and written to the FSM only on eviction.
    JoinedAt time.Time // stored in FSM for audit; not used for timeout logic
}

type TopicPartition struct {
    Topic       string
    PartitionID int32
}
```

The FSM stores `GroupState` values as part of its in-memory map (`groups map[string]*GroupState`), serialised to JSON in snapshots. See [03-raft-fsm.md](./03-raft-fsm.md) for the MetadataFSM snapshot format.

---

## 3. Coordinator Discovery

In v1, the metadata shard leader is the group coordinator for **all** groups. There is a single metadata shard (shard 0), and its Raft leader owns all group coordinator duties.

**Discovery flow:**
1. Client calls `ManagementService.DescribeCluster` on any bootstrap node.
2. Response includes `metadata_leader_node_id` and the full node address list.
3. Client resolves the coordinator address from the node list and connects.
4. If the client sends a group RPC to a non-leader node, the node returns `FAILED_PRECONDITION` with `BunnyErrorDetail{code=NOT_LEADER}` and the current leader's address. The client reconnects and retries.

Coordinator address is cached in `MetaCache` alongside partition leader addresses. It is refreshed on `NOT_LEADER` responses or on `DescribeCluster` TTL expiry.

---

## 4. Member ID Assignment

The server assigns a UUID (`uuid.NewString()`) at `JoinGroup` time. The UUID is returned in `JoinGroupResponse.member_id`. The client must present this `member_id` in all subsequent `Heartbeat`, `LeaveGroup`, and `CommitOffset` calls.

A client that has never joined (e.g., sending an empty `member_id` in `JoinGroupRequest`) always receives a new UUID. A client re-joining after a rebalance sends its existing `member_id`; the coordinator treats it as a known member re-registering and reuses the same ID.

---

## 5. Generation ID

`GenerationID` starts at 1 when a group is created and is incremented atomically (inside the Raft command application) on every membership change:
- A new member joins.
- A member leaves (voluntarily or by session timeout eviction).

Clients include their `generation_id` in `Heartbeat` and `CommitOffset` calls. The coordinator rejects offset commits from stale generation clients with `STALE_GENERATION`.

---

## 6. Range-Based Partition Assignment

### Algorithm

Inputs:
- `topics`: the union of subscribed topics across all current group members (v1 assumes all members subscribe to the same topic set; mixed subscriptions yield undefined assignment behaviour and are rejected with `INVALID_ARGUMENT`).
- `members`: the sorted list of current `member_id` strings (lexicographic order).
- `partitionCounts`: `map[topic]int32` - queried from the Metadata FSM at assignment time.

For each topic independently:

```
n_partitions = partitionCounts[topic]
n_members    = len(members)
base         = n_partitions / n_members       // integer division
remainder    = n_partitions % n_members

cursor = 0
for i, memberID := range members:
    count = base
    if i < remainder:
        count += 1
    assignments[memberID] = append(assignments[memberID],
        partitions[cursor : cursor+count]...)
    cursor += count
```

Example: 8 partitions, 3 members (sorted: `m-a`, `m-b`, `m-c`):
- base=2, remainder=2
- `m-a`: [0, 1, 2]  (base + 1 extra)
- `m-b`: [3, 4, 5]  (base + 1 extra)
- `m-c`: [6, 7]     (base only)

### When assignment is computed

Assignment is recomputed inside the coordinator (not inside the FSM) immediately after each Raft command that changes membership commits. The computed assignment is written back to the FSM as part of the same command's payload (i.e., the command carries the new membership *and* the new assignment). This keeps assignment state in the FSM deterministic across replicas.

---

## 7. JoinGroup Flow

1. Client sends `JoinGroupRequest{group_id, member_id, topics, session_timeout_ms, heartbeat_interval_ms}`.
2. Coordinator validates:
   - Topics exist in the Metadata FSM (`QueryGetTopic` for each).
   - All topics have the same subscription set if other members already exist.
   - `session_timeout_ms` is within server-configured bounds (`[1000, 300000]`).
3. Coordinator assigns `member_id` (UUID) if `member_id` in request is empty.
4. Coordinator computes the new assignment (including the new member).
5. Coordinator issues `SyncProposeMetadata(JoinConsumerGroupCmd{group_id, member, new_assignment})`.
   - This atomically updates `Members` and `Assignments` and increments `GenerationID` in the FSM.
6. On commit, coordinator reads the FSM result (new `GenerationID`, `Assignments`).
7. Returns `JoinGroupResponse{member_id, generation_id, assignments_for_this_member}`.

Concurrent `JoinGroup` calls from multiple clients are serialised by Raft: each proposes independently; each commit produces a new generation; the last committer's assignment is the current one. In practice, a group starts with one member and adds members one at a time, each triggering a rebalance.

See [sequence/group-join.md](./sequence/group-join.md).

---

## 8. Heartbeat Flow

Clients send `Heartbeat` every `heartbeat_interval_ms` (default 3 000 ms).

The coordinator handles heartbeats entirely in memory - **no Raft round-trip** for a healthy heartbeat:
1. Validate `member_id` is a known member of `group_id` (lookup from FSM via `QueryGetGroup`).
2. Validate `generation_id` matches the current generation.
3. Update the in-memory sweep table: `lastHeartbeat[group_id][member_id] = now`.
4. Return `HeartbeatResponse{rebalance_required: currentGenerationID > client_generation_id}`.

If `rebalance_required=true`, the client must re-issue `JoinGroup`. The heartbeat itself is not a Raft write; the coordinator does not commit heartbeat timestamps to the FSM.

See [sequence/group-heartbeat.md](./sequence/group-heartbeat.md).

---

## 9. Session Timeout Enforcement

A background goroutine on the coordinator runs a sweep every `sweepIntervalMs` (default 5 000 ms):

```
for each group in FSM:
    for each member in group.Members:
        if now - lastHeartbeat[group_id][member_id] > member.SessionTimeoutMs:
            SyncProposeMetadata(LeaveConsumerGroupCmd{group_id, member_id, reason=Timeout})
```

**VERIFY:** This goroutine must run only when this node is the metadata shard leader. The mechanism for detecting leadership is `// VERIFY: dragonboat v4 NodeHost or RaftHost API to query whether this node is the current leader of a given shard`. Candidate: `nh.GetLeaderID(shardID)` - returns `(leaderID, valid, err)`; goroutine runs the sweep only if `valid && leaderID == thisNodeID`. Mark as VERIFY until confirmed against dragonboat v4 docs.

When a member's session expires:
1. Sweep proposes `LeaveConsumerGroupCmd`.
2. On commit, `GenerationID` is incremented, member is removed, assignment is recomputed and stored.
3. Remaining members learn of the rebalance on their next `Heartbeat` response (`rebalance_required=true`).

The in-memory `lastHeartbeat` table is rebuilt on coordinator startup (or leader election) by assuming all current members are fresh: `lastHeartbeat[g][m] = now`. This gives existing members one full `session_timeout_ms` window to send their first heartbeat to the new coordinator before being evicted. This is a deliberate conservative choice to avoid spurious evictions after leader failover.

---

## 10. LeaveGroup Flow

1. Client sends `LeaveGroupRequest{group_id, member_id}`.
2. Coordinator validates membership.
3. Issues `SyncProposeMetadata(LeaveConsumerGroupCmd{group_id, member_id, reason=Voluntary})`.
4. On commit: `GenerationID++`, member removed, assignment recomputed.
5. Returns `LeaveGroupResponse{}` (empty on success).

See [sequence/group-leave.md](./sequence/group-leave.md).

---

## 11. Rebalance Flow

A rebalance is not a standalone action by the coordinator - it is the consequence of a `JoinConsumerGroupCmd` or `LeaveConsumerGroupCmd` commit. There is no separate "rebalance" Raft command.

The sequence:
1. A membership-changing Raft command commits.
2. FSM applies it: updates `Members`, increments `GenerationID`, stores new `Assignments`.
3. The coordinator does not proactively push to existing members.
4. Existing members discover the rebalance passively on their next `Heartbeat` call: the response returns `rebalance_required=true` because `currentGenerationID > client_generation_id`.
5. Members re-issue `JoinGroup`, receive the new assignment for their `member_id`, and resume fetching.

This is a "stop-the-world" rebalance: all members pause consumption and re-join before any member resumes with the new assignment. Cooperative rebalance (members can continue fetching unrevoked partitions during rebalance) is out of scope for v1.

See [sequence/group-rebalance.md](./sequence/group-rebalance.md).

---

## 12. Offset Commit Flow

1. Client sends `CommitOffsetRequest{group_id, member_id, generation_id, offsets: {TopicPartition→int64}}`.
2. Coordinator validates:
   - `member_id` is a current member (lookup from FSM).
   - `generation_id` == current `GenerationID`. If stale → `STALE_GENERATION`.
   - Each committed partition is in the member's current assignment. If not → `INVALID_ARGUMENT`.
3. Issues `SyncProposeMetadata(CommitConsumerOffsetCmd{group_id, offsets})`.
4. On commit: FSM updates `GroupState.Offsets`.
5. Returns `CommitOffsetResponse{}`.

See [sequence/offset-commit.md](./sequence/offset-commit.md).

---

## 13. Offset Fetch Flow

Offset fetches are **read-only** - no Raft round-trip:

1. Client sends `FetchCommittedOffsetsRequest{group_id, partitions: []TopicPartition}`.
2. Coordinator issues `LookupMetadata(QueryGetGroupOffsets{group_id, partitions})`.
3. FSM lookup returns the stored `int64` for each partition. Missing entries return `-1` (no committed offset, consumer should apply `AutoOffsetReset` policy).
4. Returns `FetchCommittedOffsetsResponse{offsets: map[TopicPartition]int64}`.

See [sequence/offset-fetch.md](./sequence/offset-fetch.md).

---

## 14. Metadata FSM Commands (consumer-group set)

These commands extend the MetadataFSM command set documented in [03-raft-fsm.md](./03-raft-fsm.md). If those commands are not already enumerated there, they must be added during implementation.

| Command name | Opcode (suggested) | Payload | FSM effect |
|---|---|---|---|
| `JoinConsumerGroupCmd` | `0x0A` | `group_id`, `member_id`, `member_state`, `new_assignment` | Upsert member; store assignment; `GenerationID++` |
| `LeaveConsumerGroupCmd` | `0x0B` | `group_id`, `member_id`, `reason` | Remove member; recompute + store assignment; `GenerationID++` |
| `CommitConsumerOffsetCmd` | `0x0C` | `group_id`, `offsets map[TP]int64` | Merge into `GroupState.Offsets` |

Query types (read-only, via `LookupMetadata`):

| Query | Returns |
|---|---|
| `QueryGetGroup{group_id}` | `*GroupState` or nil |
| `QueryGetGroupOffsets{group_id, partitions}` | `map[TopicPartition]int64` |

---

## 15. Concurrency Model

The coordinator uses the metadata shard's Raft single-leader guarantee as its primary serialisation mechanism for all writes. No additional mutex is needed to serialize concurrent `JoinGroup` or `CommitOffset` calls; dragonboat sequences them via the propose pipeline.

The in-memory sweep table (`lastHeartbeat map[string]map[string]time.Time`) is accessed by two goroutines:
- The gRPC handler goroutines (on each `Heartbeat` call).
- The sweep goroutine.

A single `sync.RWMutex` protects `lastHeartbeat`. Heartbeat handlers acquire `Lock` (write); the sweep goroutine acquires `RLock` for its read pass, then `Lock` only when it decides to evict a member and removes the entry.

---

## 16. Failure Scenarios

| Scenario | Behaviour |
|---|---|
| Metadata shard leader fails during `JoinGroup` propose | dragonboat elects a new leader. Client receives `UNAVAILABLE`. Client retries (with backoff); new leader accepts the join. Existing members' heartbeats return `NOT_LEADER`; clients discover new coordinator and reconnect. |
| Member crashes silently (no `LeaveGroup`) | Sweep goroutine detects missing heartbeat after `session_timeout_ms`. Proposes eviction. Remaining members learn on next heartbeat. |
| Network partition isolates a member | Member's heartbeats stop reaching coordinator; evicted after timeout. When partition heals, member's heartbeat returns `NOT_GROUP_MEMBER` (member no longer in FSM); member re-issues `JoinGroup`. |
| `CommitOffset` with stale `generation_id` | Coordinator returns `STALE_GENERATION`. Client re-issues `JoinGroup` to get the current generation, then retries the commit. |
| Coordinator failover during `CommitOffset` SyncPropose | The propose may or may not have been applied. Client receives `UNAVAILABLE`. Client may re-issue the commit (idempotent - same offsets committed twice is harmless; the FSM takes the max). |
| All replicas of metadata shard offline | All group RPCs return `UNAVAILABLE`. No group management possible until quorum recovers. |

---

## 17. Limitations (v1)

The following are explicitly **not implemented** in v1. Reference [REQUIREMENTS.md §9](../REQUIREMENTS.md):

- **Sticky assignment.** Partitions are not preserved for members across rebalances. Every rebalance computes a fresh range assignment from scratch.
- **Cooperative (incremental) rebalance.** All members stop consumption and re-join before anyone resumes. No partial revocation.
- **Static membership.** There is no `group.instance.id` equivalent. Every restart is treated as a new join.
- **Per-group configuration overrides.** Session timeout and heartbeat interval are per-member at `JoinGroup` time, but there is no server-side group-level policy.
- **Multiple coordinators.** All groups share the single metadata shard leader as coordinator. Horizontal scaling of group coordination is post-v1.
- **Consumer group metrics per-lag.** Lag per `(group, topic, partition)` is not exposed in v1 metrics (only offset is stored; lag = `latestOffset − committedOffset` can be computed externally). Future work.

---

## 18. Open Questions

1. **Opcode allocation for consumer group FSM commands.** The opcodes `0x0A`, `0x0B`, `0x0C` above are suggestions. The definitive opcode table lives in [03-raft-fsm.md](./03-raft-fsm.md) (immutable). The implementation must reconcile; if those slots are taken, assign the next available values.

2. **Mixed-subscription groups.** v1 rejects `JoinGroup` if the new member's topic list differs from existing members'. This is a strict simplification. Future work: allow mixed subscriptions and use range assignment per-member-per-topic based on that member's subscribed topics only.

3. **Rebuild of `lastHeartbeat` on leader election.** The current design gives every member a fresh `session_timeout_ms` window after a leader change, which is conservative. An alternative is to read the `JoinedAt` timestamp from the FSM and subtract elapsed time - but this is fragile if clocks drift. The conservative approach is recommended for v1.

4. **`QueryGetGroup` performance under many groups.** If the cluster has thousands of groups, a single `LookupMetadata` that returns a full `GroupState` per group is fine for v1. Pagination or projection queries are post-v1.

5. **VERIFY: `GetLeaderID` API in dragonboat v4.** The sweep goroutine needs to confirm this node is the metadata shard leader before writing eviction commands. Candidate: `nh.GetLeaderID(shardID) (leaderID uint64, valid bool, err error)`. Verify signature and behaviour (especially `valid=false` during election) against dragonboat v4 source before implementing the sweep guard.
