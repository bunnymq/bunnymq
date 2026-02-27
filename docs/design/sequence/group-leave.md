# Sequence: Consumer Group — LeaveGroup

Voluntary member departure. The coordinator removes the member, recomputes the assignment, and bumps the generation ID. Remaining members learn of the change on their next heartbeat.

---

## Voluntary leave

```mermaid
sequenceDiagram
    participant C1 as Consumer-1 (leaving)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM
    participant C2 as Consumer-2 (remaining)
    participant HB2 as Consumer-2 heartbeatLoop

    Note over C1: Application calls Consumer.Close().<br/>Consumer sends LeaveGroup before closing connections.

    C1->>+DAPI: LeaveGroup RPC {group_id="shipping-svc",<br/>member_id="m-uuid-1"}

    DAPI->>+GC: LeaveGroup(ctx, req)

    Note over GC: Validate: group exists, member_id is current member.
    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH->>MFSM: Lookup(QueryGetGroup{"shipping-svc"})
    MFSM-->>RH: GroupState{members=["m-uuid-1","m-uuid-2"], generationID=2}
    RH-->>GC: *GroupState

    Note over GC: m-uuid-1 is a valid current member. ✓<br/>Compute new assignment without m-uuid-1.<br/>Remaining members: ["m-uuid-2"].<br/>All partitions (orders/[0..7], payments/[0..3]) → m-uuid-2.<br/>new_generation_id = 3.

    GC->>+RH: SyncProposeMetadata(LeaveConsumerGroupCmd{<br/>group_id="shipping-svc",<br/>member_id="m-uuid-1",<br/>reason=Voluntary,<br/>new_assignment={"m-uuid-2": [orders/0..7, payments/0..3]},<br/>new_generation_id=3})

    RH->>MFSM: Update([LeaveConsumerGroupCmd]) — quorum committed
    Note over MFSM: Remove m-uuid-1 from Members.<br/>Replace Assignments with new_assignment.<br/>GenerationID = 3.
    MFSM-->>RH: ok
    RH-->>-GC: ok

    GC->>GC: lastHeartbeatMu.Lock()<br/>delete(lastHeartbeat["shipping-svc"]["m-uuid-1"])<br/>lastHeartbeatMu.Unlock()

    GC-->>-DAPI: LeaveGroupResult{}
    DAPI-->>-C1: LeaveGroupResponse (OK)

    Note over C1: Closes gRPC connections and exits.

    Note over C2,HB2: C2's heartbeat goroutine fires next (within 3s).
    HB2->>DAPI: Heartbeat {group_id, member_id="m-uuid-2", generation_id=2}
    DAPI->>GC: Heartbeat(ctx, req)
    GC->>GC: currentGenerationID=3 > client 2 → rebalance_required=true
    GC-->>DAPI: rebalance_required=true
    DAPI-->>HB2: HeartbeatResponse{rebalance_required=true}

    HB2->>HB2: rebalanceCh <- struct{}{}
    Note over C2: Consumer-2 re-joins with all partitions assigned to it.
```

---

## LeaveGroup with unknown member_id

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataService<br/>(coordinator)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    C->>+DAPI: LeaveGroup RPC {group_id="shipping-svc",<br/>member_id="m-uuid-99"}

    DAPI->>+GC: LeaveGroup(ctx, req)

    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH->>MFSM: Lookup
    MFSM-->>RH: GroupState{members=["m-uuid-1","m-uuid-2"], generationID=2}
    RH-->>GC: *GroupState

    Note over GC: m-uuid-99 not in Members. Return error.
    GC-->>-DAPI: NOT_GROUP_MEMBER
    DAPI-->>-C: status=FAILED_PRECONDITION<br/>BunnyErrorDetail{code=NOT_GROUP_MEMBER}

    Note over C: The client ignores the error during Close()<br/>— it is already not in the group.
```

---

## Last member leaves (group becomes empty)

```mermaid
sequenceDiagram
    participant C as Consumer-1 (last member)
    participant DAPI as DataService<br/>(coordinator)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    C->>+DAPI: LeaveGroup RPC {group_id="shipping-svc",<br/>member_id="m-uuid-1"}

    DAPI->>+GC: LeaveGroup(ctx, req)

    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH-->>GC: GroupState{members=["m-uuid-1"], generationID=5}

    Note over GC: Only member. New member list = [].<br/>new_assignment = {} (empty).<br/>new_generation_id = 6.

    GC->>+RH: SyncProposeMetadata(LeaveConsumerGroupCmd{<br/>group_id="shipping-svc",<br/>member_id="m-uuid-1",<br/>reason=Voluntary,<br/>new_assignment={},<br/>new_generation_id=6})

    RH->>MFSM: Update([LeaveConsumerGroupCmd])
    Note over MFSM: Members = {}. Assignments = {}.<br/>GenerationID = 6.<br/>GroupState remains in FSM (not deleted).<br/>Committed offsets are preserved.
    MFSM-->>RH: ok
    RH-->>-GC: ok

    GC-->>-DAPI: LeaveGroupResult{}
    DAPI-->>-C: LeaveGroupResponse (OK)

    Note over C: Group "shipping-svc" still exists in the FSM<br/>with its committed offsets intact.<br/>A new member can join it later and will pick up<br/>from the last committed offsets.
```

---

## Notes

- **Group is not deleted when the last member leaves.** The `GroupState` (with its committed offsets) remains in the FSM. This matches Kafka semantics: committed offsets survive until the group is explicitly deleted (not implemented in v1) or the FSM is wiped.
- **Assignment computed in coordinator, not FSM.** The `LeaveConsumerGroupCmd` carries the pre-computed `new_assignment` payload. The FSM applies it as-is; it does not re-run the assignment algorithm. This keeps the FSM `Update` method strictly deterministic (all inputs are in the command).
- **Consumer.Close() behaviour.** The client library calls `LeaveGroup` before closing connections. If the RPC fails (coordinator unreachable), the client still closes — the session timeout sweep will eventually evict the member. The application should handle this gracefully.
- **Concurrent leave + join.** If C1 leaves and C3 joins at the same time, both propose independently. The resulting generation depends on the Raft commit order. The final assignment is consistent because each committer uses the pre-propose group state to compute the assignment — the later committer must re-read FSM state after commit to produce the correct response. See the concurrent join note in [group-join.md](./group-join.md).
