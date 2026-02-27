# Sequence: Consumer Group - JoinGroup

Full `JoinGroup` flow from the first member creating the group through a second member joining (triggering a rebalance). Both flows go through the metadata shard leader as the group coordinator.

---

## First member joins (group does not exist yet)

```mermaid
sequenceDiagram
    participant C1 as Consumer-1 (client)
    participant DAPI as DataService<br/>(coordinator node - metadata leader)
    participant GC as GroupCoordinator<br/>(in-process on coordinator node)
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    C1->>+DAPI: JoinGroup RPC {group_id="shipping-svc",<br/>member_id="" (empty),<br/>topics=["orders","payments"],<br/>session_timeout_ms=30000,<br/>heartbeat_interval_ms=3000}

    DAPI->>+GC: JoinGroup(ctx, req)

    Note over GC: Validate: topics exist?
    GC->>RH: LookupMetadata(QueryGetTopic{"orders"})
    RH->>MFSM: Lookup(QueryGetTopic{"orders"})
    MFSM-->>RH: *TopicInfo{partitionCount=8} ✓
    RH-->>GC: *TopicInfo

    GC->>RH: LookupMetadata(QueryGetTopic{"payments"})
    RH->>MFSM: Lookup(QueryGetTopic{"payments"})
    MFSM-->>RH: *TopicInfo{partitionCount=4} ✓
    RH-->>GC: *TopicInfo

    Note over GC: Group "shipping-svc" does not exist yet.<br/>Assign member_id = uuid.NewString() = "m-uuid-1".<br/>New group: generationID starts at 1.<br/>members = ["m-uuid-1"] (sorted, only one).<br/>Compute assignment (all partitions to single member):<br/>  orders/[0..7] → m-uuid-1<br/>  payments/[0..3] → m-uuid-1

    GC->>+RH: SyncProposeMetadata(JoinConsumerGroupCmd{<br/>group_id="shipping-svc",<br/>member={id="m-uuid-1", topics=["orders","payments"],<br/>session_timeout_ms=30000},<br/>new_assignment={m-uuid-1: [orders/0..7, payments/0..3]},<br/>new_generation_id=1})

    RH->>MFSM: Update([JoinConsumerGroupCmd]) - quorum committed
    Note over MFSM: Create GroupState{<br/>  GroupID: "shipping-svc",<br/>  Members: {"m-uuid-1": MemberState{...}},<br/>  GenerationID: 1,<br/>  Assignments: {"m-uuid-1": [orders/0..7, payments/0..3]},<br/>  Offsets: {}  (empty, no commits yet)<br/>}
    MFSM-->>RH: sm.Result.Value = generationID=1
    RH-->>-GC: result

    Note over GC: Update in-memory sweep table:<br/>lastHeartbeat["shipping-svc"]["m-uuid-1"] = now

    GC-->>-DAPI: JoinGroupResult{member_id="m-uuid-1", generation_id=1,<br/>assignments=[{topic="orders", partitions=[0,1,2,3,4,5,6,7]},<br/>{topic="payments", partitions=[0,1,2,3]}]}

    DAPI-->>-C1: JoinGroupResponse (OK) {member_id="m-uuid-1", generation_id=1,<br/>assignments=[{topic="orders", partitions=[0..7]},<br/>{topic="payments", partitions=[0..3]}]}

    Note over C1: Fetch committed offsets for all assigned partitions,<br/>init fetch positions, start heartbeat goroutine.<br/>(See client-consumer-poll.md for that flow.)
```

---

## Second member joins (rebalance)

```mermaid
sequenceDiagram
    participant C2 as Consumer-2 (client)
    participant C1 as Consumer-1 (running)
    participant DAPI as DataService<br/>(coordinator)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    C2->>+DAPI: JoinGroup RPC {group_id="shipping-svc",<br/>member_id="" (empty → new member),<br/>topics=["orders","payments"],<br/>session_timeout_ms=30000}

    DAPI->>+GC: JoinGroup(ctx, req)

    Note over GC: Topics still valid (skip re-lookup for brevity).<br/>Check existing group.

    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH->>MFSM: Lookup(QueryGetGroup{"shipping-svc"})
    MFSM-->>RH: GroupState{members=["m-uuid-1"], generationID=1}
    RH-->>GC: *GroupState

    Note over GC: Assign member_id = "m-uuid-2".<br/>New members sorted: ["m-uuid-1", "m-uuid-2"].<br/>Recompute assignment (8 partitions, 2 members):<br/>  orders/[0..3] → m-uuid-1<br/>  orders/[4..7] → m-uuid-2<br/>  payments/[0,1] → m-uuid-1<br/>  payments/[2,3] → m-uuid-2<br/>new_generation_id = 2

    GC->>+RH: SyncProposeMetadata(JoinConsumerGroupCmd{<br/>group_id="shipping-svc",<br/>member={id="m-uuid-2", topics=["orders","payments"], session_timeout_ms=30000},<br/>new_assignment={<br/>  "m-uuid-1": [orders/0..3, payments/0,1],<br/>  "m-uuid-2": [orders/4..7, payments/2,3]},<br/>new_generation_id=2})

    RH->>MFSM: Update([JoinConsumerGroupCmd]) - quorum committed
    Note over MFSM: Add m-uuid-2 to Members.<br/>Replace Assignments.<br/>GenerationID = 2.
    MFSM-->>RH: generationID=2
    RH-->>-GC: result

    Note over GC: Update sweep table: lastHeartbeat["shipping-svc"]["m-uuid-2"] = now.

    GC-->>-DAPI: JoinGroupResult{member_id="m-uuid-2", generation_id=2,<br/>assignments=[{topic="orders", partitions=[4,5,6,7]},<br/>{topic="payments", partitions=[2,3]}]}

    DAPI-->>-C2: JoinGroupResponse (OK) {member_id="m-uuid-2", generation_id=2,<br/>assignments=[{topic="orders", partitions=[4..7]},<br/>{topic="payments", partitions=[2,3]}]}

    Note over C1: C1 is polling. Its next Heartbeat will return<br/>rebalance_required=true (generationID=2 > client's 1).<br/>C1 will re-join to discover its new (reduced) assignment.
    Note over C2: C2 fetches committed offsets and begins polling<br/>its assigned partitions.
```

---

## NotLeader path

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant NonLeader as DataService<br/>(non-coordinator node)
    participant Coordinator as DataService<br/>(metadata leader)

    C->>+NonLeader: JoinGroup RPC {group_id="shipping-svc", ...}
    Note over NonLeader: This node is not the metadata shard leader.<br/>GroupCoordinator checks leadership before acting.
    NonLeader-->>-C: status=FAILED_PRECONDITION<br/>BunnyErrorDetail{code=NOT_LEADER,<br/>NotLeaderDetail{leader_node_id=1, leader_address="b1:9092"}}

    Note over C: Client updates coordinator address in MetaCache.<br/>Reconnects to "b1:9092".

    C->>+Coordinator: JoinGroup RPC {group_id="shipping-svc", ...}
    Coordinator-->>-C: JoinGroupResponse (OK) {member_id=..., generation_id=..., assignments=...}
```

---

## Notes

- **Concurrent JoinGroup calls.** If two consumers join simultaneously, their `SyncPropose` calls are serialised by dragonboat's propose pipeline. Each commit produces a new generation. The first committer produces generation N; the second committer computes its assignment based on the FSM state it read before proposing - which may now be stale. The implementation must re-read the group state after commit to build the response, not use the pre-propose snapshot.
- **Re-joining with existing member_id.** A client that already has a `member_id` (e.g. re-joining after a rebalance signal) sends it in the request. The coordinator treats this as an update to the existing member entry, reuses the same `member_id`, and recomputes the assignment. The `MemberState.JoinedAt` is not updated on re-join (it records the original join time).
- **Validation of session_timeout_ms bounds.** Server enforces `1000 ≤ session_timeout_ms ≤ 300000`. Values outside this range return `INVALID_ARGUMENT`.
