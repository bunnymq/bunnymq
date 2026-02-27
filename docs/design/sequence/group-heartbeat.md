# Sequence: Consumer Group — Heartbeat

Heartbeat RPCs keep the member's session alive and deliver rebalance signals. In the normal (healthy) path there is no Raft round-trip — the coordinator responds from in-memory state. A Raft write occurs only when the session timeout sweep evicts a stale member.

---

## Normal heartbeat (no rebalance)

```mermaid
sequenceDiagram
    participant HB as heartbeatLoop<br/>(client goroutine)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator

    Note over HB: Timer fires (every heartbeat_interval_ms = 3000ms).

    HB->>+DAPI: Heartbeat RPC {group_id="shipping-svc",<br/>member_id="m-uuid-1",<br/>generation_id=2}

    DAPI->>+GC: Heartbeat(ctx, req)

    Note over GC: Fast path — no Raft I/O.
    GC->>GC: lastHeartbeatMu.Lock()<br/>lastHeartbeat["shipping-svc"]["m-uuid-1"] = now<br/>lastHeartbeatMu.Unlock()

    GC->>GC: currentGenerationID = 2 (read from cached FSM view)<br/>rebalance_required = (2 > 2) = false

    GC-->>-DAPI: HeartbeatResult{rebalance_required=false}
    DAPI-->>-HB: HeartbeatResponse (OK) {rebalance_required=false}

    Note over HB: No action. Sleep until next interval.
```

---

## Heartbeat with rebalance signal

```mermaid
sequenceDiagram
    participant HB as heartbeatLoop<br/>(client goroutine)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant Poll as Poll goroutine<br/>(same Consumer)

    Note over GC: A new member joined (or left).<br/>FSM committed JoinConsumerGroupCmd → generationID bumped to 3.

    HB->>+DAPI: Heartbeat RPC {group_id="shipping-svc",<br/>member_id="m-uuid-1",<br/>generation_id=2}

    DAPI->>+GC: Heartbeat(ctx, req)

    GC->>GC: Update lastHeartbeat (same as normal path).
    GC->>GC: currentGenerationID = 3<br/>rebalance_required = (3 > 2) = true

    GC-->>-DAPI: HeartbeatResult{rebalance_required=true}
    DAPI-->>-HB: HeartbeatResponse (OK) {rebalance_required=true}

    HB->>Poll: rebalanceCh <- struct{}{}

    Note over Poll: Poll loop notices rebalanceCh on its next iteration.<br/>Commits pending offsets, re-issues JoinGroup.<br/>(See group-rebalance.md for the full rebalance flow.)
```

---

## Heartbeat with unknown member_id (evicted or never joined)

```mermaid
sequenceDiagram
    participant HB as heartbeatLoop<br/>(client goroutine)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    Note over HB: Client was evicted by session timeout sweep<br/>while it was offline / partitioned.

    HB->>+DAPI: Heartbeat RPC {group_id="shipping-svc",<br/>member_id="m-uuid-1",<br/>generation_id=2}

    DAPI->>+GC: Heartbeat(ctx, req)

    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH->>MFSM: Lookup(QueryGetGroup{"shipping-svc"})
    MFSM-->>RH: GroupState{members={"m-uuid-2": ...}, generationID=4}
    Note over GC: m-uuid-1 not in Members map.
    RH-->>GC: GroupState (m-uuid-1 absent)

    GC-->>-DAPI: error NOT_GROUP_MEMBER
    DAPI-->>-HB: status=FAILED_PRECONDITION<br/>BunnyErrorDetail{code=NOT_GROUP_MEMBER}

    Note over HB: Signal rebalanceCh → Poll will call JoinGroup<br/>to re-enter the group (gets a new member_id or reuses old one).
```

---

## Session timeout sweep (server-side eviction)

```mermaid
sequenceDiagram
    participant Sweep as sweepGoroutine<br/>(coordinator background)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    Note over Sweep: Sweep interval fires (every 5000ms).<br/>VERIFY: confirm this node is metadata shard leader<br/>before writing. Candidate: nh.GetLeaderID(shardID).

    Sweep->>Sweep: leaderID, valid, _ = nh.GetLeaderID(metaShardID)<br/>if !valid || leaderID != thisNodeID: return

    Sweep->>+GC: checkSessions()

    GC->>GC: lastHeartbeatMu.RLock()<br/>Iterate: for each (groupID, memberID) in lastHeartbeat:<br/>  elapsed = now - lastHeartbeat[groupID][memberID]<br/>  sessionTimeout = members[memberID].SessionTimeoutMs

    Note over GC: m-uuid-1: elapsed = 45s > session_timeout_ms=30s → evict.

    GC->>GC: lastHeartbeatMu.RUnlock()

    GC->>+RH: SyncProposeMetadata(LeaveConsumerGroupCmd{<br/>group_id="shipping-svc",<br/>member_id="m-uuid-1",<br/>reason=SessionTimeout})

    RH->>MFSM: Update([LeaveConsumerGroupCmd]) — quorum committed
    Note over MFSM: Remove m-uuid-1 from Members.<br/>Recompute assignment for remaining members.<br/>GenerationID++ (3 → 4).<br/>Store new Assignments.
    MFSM-->>RH: generationID=4
    RH-->>-GC: ok

    GC->>GC: lastHeartbeatMu.Lock()<br/>delete(lastHeartbeat["shipping-svc"]["m-uuid-1"])<br/>lastHeartbeatMu.Unlock()

    GC-->>-Sweep: done

    Note over Sweep: Remaining members (m-uuid-2) learn of rebalance<br/>on next Heartbeat: generationID=4 > their cached 3.
```

---

## Notes

- **No Raft I/O on healthy heartbeat.** The coordinator reads `currentGenerationID` from a cached view of the FSM (updated whenever a metadata write commits). For v1 this cache is a field on the `GroupCoordinator` struct updated in the `SyncProposeMetadata` callback. VERIFY: dragonboat IStateMachine does not have an explicit update callback; the coordinator re-reads with a `LookupMetadata` only when the generation ID check would benefit from freshness. In practice the coordinator always has the current generation because it was the one who proposed the last membership change.
- **Heartbeat RPC does not include the committed offset.** Offset management is a separate concern (`CommitOffset` RPC). The heartbeat only signals liveness.
- **`generation_id` in heartbeat request.** The client sends its last-known `generation_id`. If `server_generation > client_generation`, `rebalance_required=true`. The server does not return the current generation in the heartbeat response — the client learns the new generation by re-issuing `JoinGroup`.
- **Concurrent sweep and heartbeat.** The sweep goroutine and heartbeat handlers both access `lastHeartbeat` — protected by `lastHeartbeatMu`. The sweep acquires `RLock` for iteration and upgrades to `Lock` only to delete an evicted entry (the upgrade is not atomic; the sweep re-checks the elapsed time under `Lock` before deleting to avoid TOCTOU).
