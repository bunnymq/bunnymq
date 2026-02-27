# Sequence: Offset Commit

Commits the consumer's read position for one or more `(topic, partition)` pairs to the Metadata FSM. The coordinator validates group membership and generation before accepting the commit.

---

## Successful offset commit (group consumer)

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    C->>+DAPI: CommitOffset RPC {<br/>group_id="shipping-svc",<br/>member_id="m-uuid-1",<br/>generation_id=3,<br/>offsets={<br/>  {topic="orders",   partition_id=0} → 145,<br/>  {topic="orders",   partition_id=1} → 201,<br/>  {topic="payments", partition_id=0} → 88<br/>}}

    DAPI->>+GC: CommitOffset(ctx, req)

    Note over GC: Validate membership and generation.
    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH->>MFSM: Lookup(QueryGetGroup)
    MFSM-->>RH: GroupState{members=["m-uuid-1","m-uuid-2",...], generationID=3, assignments={...}}
    RH-->>GC: *GroupState

    Note over GC: m-uuid-1 ∈ Members ✓<br/>generation_id 3 == currentGenerationID 3 ✓<br/>Validate each committed partition is in m-uuid-1's assignment:<br/>  orders/0 ∈ m-uuid-1.assignment ✓<br/>  orders/1 ∈ m-uuid-1.assignment ✓<br/>  payments/0 ∈ m-uuid-1.assignment ✓

    GC->>+RH: SyncProposeMetadata(CommitConsumerOffsetCmd{<br/>group_id="shipping-svc",<br/>offsets={orders/0→145, orders/1→201, payments/0→88}})

    RH->>MFSM: Update([CommitConsumerOffsetCmd]) - quorum committed
    Note over MFSM: Merge into GroupState.Offsets:<br/>  orders/0 → 145<br/>  orders/1 → 201<br/>  payments/0 → 88<br/>(Existing offsets for unmentioned partitions are unchanged.)
    MFSM-->>RH: ok
    RH-->>-GC: ok

    GC-->>-DAPI: CommitOffsetResult{}
    DAPI-->>-C: CommitOffsetResponse (OK)
```

---

## Stale generation commit (member re-joined but using old generation_id)

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    Note over C: Member received rebalance_required=true but<br/>hasn't re-joined yet. Trying to commit with old generation_id.

    C->>+DAPI: CommitOffset RPC {group_id, member_id="m-uuid-1",<br/>generation_id=2, offsets={orders/0→120}}

    DAPI->>+GC: CommitOffset(ctx, req)

    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH->>MFSM: Lookup
    MFSM-->>RH: GroupState{members=[...], generationID=3}
    RH-->>GC: *GroupState

    Note over GC: generation_id 2 ≠ currentGenerationID 3 → reject.
    GC-->>-DAPI: error STALE_GENERATION
    DAPI-->>-C: status=FAILED_PRECONDITION<br/>BunnyErrorDetail{code=STALE_GENERATION}

    Note over C: Client re-issues JoinGroup to get current generation_id,<br/>then retries CommitOffset with the new generation_id.
```

---

## Commit for a partition not in the member's assignment

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    C->>+DAPI: CommitOffset RPC {group_id, member_id="m-uuid-1",<br/>generation_id=3,<br/>offsets={orders/7→999}}

    DAPI->>+GC: CommitOffset(ctx, req)

    GC->>RH: LookupMetadata(QueryGetGroup{"shipping-svc"})
    RH-->>GC: GroupState{assignments={"m-uuid-1": [orders/0..2, payments/0,1]}, generationID=3}

    Note over GC: orders/7 ∉ m-uuid-1.assignment → reject.
    GC-->>-DAPI: error INVALID_ARGUMENT ("partition not assigned to member")
    DAPI-->>-C: status=INVALID_ARGUMENT

    Note over C: Client should not commit partitions it is not assigned.<br/>This indicates a programming error in the consumer application.
```

---

## Auto-commit (from heartbeat goroutine)

```mermaid
sequenceDiagram
    participant HB as heartbeatLoop<br/>(goroutine)
    participant C as Consumer (state)
    participant DAPI as DataService<br/>(coordinator node)

    Note over HB: Auto-commit timer fires (default interval 5s).

    HB->>+C: getPendingCommits()
    C-->>-HB: {orders/0→145, orders/1→201, payments/0→88}

    Note over HB: If map is empty, skip the RPC.

    HB->>+DAPI: CommitOffset RPC {group_id, member_id, generation_id,<br/>offsets={orders/0→145, orders/1→201, payments/0→88}}
    DAPI-->>-HB: CommitOffsetResponse (OK)

    HB->>C: clearPendingCommits()

    Note over HB: On STALE_GENERATION or NOT_GROUP_MEMBER:<br/>signal rebalanceCh → Poll will re-join before next auto-commit.
```

---

## Notes

- **Idempotency.** Committing the same offset twice is safe - the FSM stores the last committed value. Committing a lower offset than the current stored value is not rejected in v1 (the FSM just overwrites with the lower value). The client library never commits lower offsets (it only commits `fetchPosition - 1`, which is monotonically increasing). A future hardening could add server-side monotonicity enforcement.
- **Partial commits.** The `CommitConsumerOffsetCmd` commits all listed partitions atomically (single Raft command). Either all succeed or none do. The client can commit a subset of its assigned partitions in each call.
- **Committed offset semantics.** Offset `N` committed means "record N-1 was the last processed record; next fetch should start at N." The client library stores `fetchPosition` (which is already `nextOffset` from the last successful Fetch) and commits it directly. On re-join, the coordinator returns the committed value; the client starts fetching from `committed + 1` - actually from `committed` directly, since `committed` == next offset to read.

  Correction: the offset stored is the **next offset to fetch**, not the last processed record's offset. This matches the convention: `fetchPosition` = `nextOffset` from last Fetch response. Stored value = what to pass as `offset` in the next Fetch.
- **Coordinator failover during SyncPropose.** The commit may or may not have been applied. The client retries; applying the same offset twice is safe (see idempotency note above).
