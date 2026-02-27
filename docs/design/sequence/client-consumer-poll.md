# Sequence: Consumer Poll Loop (group consumer with background heartbeat)

Full `Consumer.Poll` lifecycle for a group consumer from `Subscribe` through steady-state polling and a rebalance event. Shows the background heartbeat goroutine running concurrently with the poll loop.

## Subscribe + JoinGroup

```mermaid
sequenceDiagram
    participant App as Application
    participant C as Consumer
    participant HB as heartbeatLoop<br/>(goroutine)
    participant CP as ConnPool
    participant GC as DataService<br/>(Group Coordinator / metadata leader)

    App->>+C: Subscribe(["orders", "payments"])
    Note over C: Validate: GroupID set.<br/>Store topic list.

    C->>+CP: ConnFor(coordinatorAddr)
    CP-->>-C: conn

    C->>+GC: DataService.JoinGroup(JoinGroupRequest{<br/>group_id="shipping-svc",<br/>member_id="" (empty → server assigns),<br/>topics=["orders","payments"],<br/>session_timeout_ms=30000,<br/>heartbeat_interval_ms=3000})

    Note over GC: Validates topics exist.<br/>Assigns member_id (UUID).<br/>SyncProposeMetadata(JoinConsumerGroup).<br/>Computes range assignment for all current members.

    GC-->>-C: JoinGroupResponse{<br/>member_id="m-uuid-1",<br/>generation_id=5,<br/>assignments=[{topic="orders",partitions=[0,1,2]},<br/>{topic="payments",partitions=[0]}]}

    C->>C: Store memberID, generationID, assignments

    Note over C: Fetch committed offsets for assigned partitions,<br/>then init fetch positions (committed+1 or AutoOffsetReset).

    C->>+GC: DataService.FetchCommittedOffsets(FetchCommittedOffsetsRequest{<br/>group_id="shipping-svc",<br/>partitions=[orders/0,orders/1,orders/2,payments/0]})
    GC-->>-C: FetchCommittedOffsetsResponse{offsets={orders/0→100, orders/1→200, orders/2→50, payments/0→0}}

    C->>C: fetchPositions = {orders/0→101, orders/1→201, orders/2→51, payments/0→1}

    Note over C: Start heartbeat goroutine.
    C->>+HB: go heartbeatLoop(ctx, groupID, memberID, generationID, interval=3s)

    C-->>-App: nil
```

---

## Steady-state Poll loop

```mermaid
sequenceDiagram
    participant App as Application
    participant C as Consumer
    participant HB as heartbeatLoop<br/>(goroutine)
    participant CP as ConnPool
    participant DS as DataService<br/>(partition leader)
    participant GC as DataService<br/>(coordinator)

    App->>+C: Poll(ctx, maxWaitMs=500)

    Note over C: Distribute wait budget across assignments.<br/>budget_per_partition = maxWaitMs / len(assignments).<br/>Iterate: orders/0, orders/1, orders/2, payments/0.

    loop for each assigned partition
        C->>+CP: ConnFor(leaderAddr for partition)
        CP-->>-C: conn

        C->>+DS: DataService.Fetch(FetchRequest{<br/>topic, partition_id, offset=fetchPos[tp],<br/>max_bytes=1MiB, max_wait_ms=budget_per_partition})

        alt records available
            DS-->>-C: FetchResponse{records=<bytes>, next_offset=145}
            C->>C: Decode batches from records bytes.<br/>fetchPositions[tp] = 145.<br/>Append to pendingRecords.
        else no records yet (long-poll wait expired)
            DS-->>C: FetchResponse{records=nil, next_offset=0}
            Note over C: Empty response - no records in window.<br/>Keep fetchPositions[tp] unchanged.<br/>Move to next partition.
        end
    end

    Note over C: Check rebalanceCh from heartbeatLoop.
    alt rebalanceCh has signal
        C->>C: Pause poll. Drain pendingRecords to caller first.
        Note over C: Rebalance handling - see Rebalance flow below.
    end

    C-->>-App: []Record (from pendingRecords)

    Note over HB: Concurrently, every 3 seconds:
    HB->>+GC: DataService.Heartbeat(HeartbeatRequest{<br/>group_id, member_id, generation_id})
    GC-->>-HB: HeartbeatResponse{rebalance_required=false}
```

---

## Rebalance flow (member joined or left the group)

```mermaid
sequenceDiagram
    participant App as Application
    participant C as Consumer
    participant HB as heartbeatLoop<br/>(goroutine)
    participant CP as ConnPool
    participant GC as DataService<br/>(coordinator)

    Note over HB: Heartbeat fires. Coordinator has bumped<br/>generation_id because another member joined.

    HB->>+GC: DataService.Heartbeat(HeartbeatRequest{<br/>group_id, member_id, generation_id=5})
    GC-->>-HB: HeartbeatResponse{rebalance_required=true}

    HB->>HB: rebalanceCh <- struct{}{}

    Note over C: Next Poll() checks rebalanceCh.

    App->>+C: Poll(ctx, maxWaitMs=500)
    C->>C: Check rebalanceCh → signal present.

    Note over C: Commit any auto-commit pending offsets<br/>before re-joining.

    C->>+GC: DataService.CommitOffset(CommitOffsetRequest{<br/>group_id, member_id, generation_id=5,<br/>offsets={orders/0→145, orders/1→201, orders/2→51, payments/0→1}})
    GC-->>-C: CommitOffsetResponse{ok}

    C->>+GC: DataService.JoinGroup(JoinGroupRequest{<br/>group_id, member_id="m-uuid-1",<br/>topics=["orders","payments"],<br/>session_timeout_ms=30000,<br/>heartbeat_interval_ms=3000})
    Note over GC: Validates. SyncProposeMetadata(JoinConsumerGroup).<br/>Recomputes assignment for all current members<br/>(now includes new member).
    GC-->>-C: JoinGroupResponse{<br/>member_id="m-uuid-1",<br/>generation_id=6,<br/>assignments=[{topic="orders",partitions=[0,1]},<br/>{topic="payments",partitions=[0]}]}

    C->>C: Update assignments, generationID=6.<br/>Release revoked partitions from fetchPositions.

    C->>+GC: DataService.FetchCommittedOffsets({group_id, new partitions only if any gained})
    GC-->>-C: FetchCommittedOffsetsResponse{...}

    C->>C: Update fetchPositions for newly assigned partitions.

    Note over HB: heartbeatLoop is notified of new generationID<br/>via channel. Starts heartbeating with generation_id=6.

    C-->>-App: []Record (empty or partial - records from before rebalance)

    Note over App: Application calls Poll again.<br/>New assignment is orders/0,1 and payments/0<br/>(lost orders/2 to the new member).
```

---

## Auto-commit (background, within heartbeatLoop)

```mermaid
sequenceDiagram
    participant HB as heartbeatLoop<br/>(goroutine)
    participant C as Consumer (state)
    participant GC as DataService<br/>(coordinator)

    Note over HB: Auto-commit interval (default 5s) fires<br/>inside the heartbeat goroutine.

    HB->>+C: pendingCommits() → {orders/0→145, payments/0→1}
    C-->>-HB: offsets map

    HB->>+GC: DataService.CommitOffset(CommitOffsetRequest{<br/>group_id, member_id, generation_id=6,<br/>offsets={orders/0→145, payments/0→1}})
    GC-->>-HB: CommitOffsetResponse{ok}

    Note over HB: If response is STALE_GENERATION or NOT_GROUP_MEMBER,<br/>heartbeatLoop signals rebalanceCh - Poll will re-join.
```

---

## Consumer.Commit (explicit, from application)

```mermaid
sequenceDiagram
    participant App as Application
    participant C as Consumer
    participant GC as DataService<br/>(coordinator)

    App->>+C: Commit(ctx)
    C->>C: Snapshot fetchPositions → commit map<br/>(last returned position per partition)

    C->>+GC: DataService.CommitOffset(CommitOffsetRequest{<br/>group_id, member_id, generation_id, offsets})
    GC-->>-C: CommitOffsetResponse

    alt STALE_GENERATION or NOT_GROUP_MEMBER
        GC-->>C: error detail
        C-->>-App: ErrStaleGeneration / ErrNotGroupMember<br/>(caller should re-subscribe or check group status)
    else ok
        C-->>-App: nil
    end
```

---

## Notes

- **One goroutine per consumer.** The heartbeat goroutine is the only background goroutine. Poll runs in the caller's goroutine. Thread safety: `fetchPositions` and `assignments` are only written by the Poll goroutine (on rebalance or after Fetch); the heartbeat goroutine reads `generationID` and `memberID` via an internal protected getter.
- **`maxWaitMs` budget distribution.** Budget is divided equally across assigned partition count. If five partitions are assigned and `maxWaitMs=500`, each Fetch gets `max_wait_ms=100`. This is an approximation; the caller sets the wall-clock budget, and the consumer honors it on a best-effort basis.
- **Non-group consumer.** No JoinGroup or heartbeat goroutine is started. User calls `Seek(topic, partition, offset)` to set read positions and calls `Poll`. Offsets are kept only locally unless the user calls `CommitOffsets` explicitly with a group ID.
- **Leader-change during fetch.** If a Fetch returns `NOT_LEADER`, the consumer calls `SetLeader` on MetaCache (same as producer) and retries the Fetch for that partition before returning to the caller. This is handled within the per-partition fetch step, not visible to the application.
- **Session timeout.** If the application does not call Poll for longer than `session_timeout_ms`, heartbeats still fire (they are in a separate goroutine), keeping the session alive as long as the consumer object exists.
