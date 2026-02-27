# Sequence: Offset Fetch

Retrieves committed offsets for a set of `(topic, partition)` pairs from the Metadata FSM. Read-only - no Raft round-trip.

---

## Successful offset fetch (group consumer at startup)

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    Note over C: After JoinGroup succeeds, Consumer fetches committed offsets<br/>for its assigned partitions to determine where to start polling.

    C->>+DAPI: FetchCommittedOffsets RPC {<br/>group_id="shipping-svc",<br/>partitions=[<br/>  {topic="orders",   partition_id=0},<br/>  {topic="orders",   partition_id=1},<br/>  {topic="orders",   partition_id=2},<br/>  {topic="payments", partition_id=0}<br/>]}

    DAPI->>+GC: FetchCommittedOffsets(ctx, req)

    GC->>+RH: LookupMetadata(QueryGetGroupOffsets{<br/>group_id="shipping-svc",<br/>partitions=[orders/0, orders/1, orders/2, payments/0]})

    RH->>+MFSM: Lookup(QueryGetGroupOffsets{...})
    Note over MFSM: Read GroupState.Offsets for "shipping-svc".<br/>No Raft round-trip - ReadLocalNode.
    MFSM-->>-RH: {orders/0→145, orders/1→201, orders/2→51, payments/0→88}
    RH-->>-GC: offset map

    GC-->>-DAPI: FetchCommittedOffsetsResult{offsets={orders/0→145, orders/1→201, orders/2→51, payments/0→88}}
    DAPI-->>-C: FetchCommittedOffsetsResponse (OK) {offsets={...}}

    Note over C: initFetchPositions:<br/>  orders/0  → 145  (start fetching from offset 145)<br/>  orders/1  → 201<br/>  orders/2  → 51<br/>  payments/0 → 88
```

---

## Partition with no committed offset (first-time consumer)

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataService<br/>(coordinator node)
    participant GC as GroupCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    C->>+DAPI: FetchCommittedOffsets RPC {group_id="shipping-svc",<br/>partitions=[{topic="orders", partition_id=5}]}

    DAPI->>+GC: FetchCommittedOffsets(ctx, req)

    GC->>+RH: LookupMetadata(QueryGetGroupOffsets{...})
    RH->>+MFSM: Lookup
    Note over MFSM: GroupState.Offsets has no entry for orders/5.
    MFSM-->>-RH: {orders/5 → -1}  (sentinel: no committed offset)
    RH-->>-GC: {orders/5 → -1}

    GC-->>-DAPI: FetchCommittedOffsetsResult{offsets={orders/5 → -1}}
    DAPI-->>-C: FetchCommittedOffsetsResponse (OK) {offsets={orders/5 → -1}}

    Note over C: offset == -1 → apply AutoOffsetReset policy:<br/>  "earliest" → call GetOffsets(EARLIEST) → fetch from start.<br/>  "latest"   → call GetOffsets(LATEST)   → skip existing records.
```

---

## AutoOffsetReset resolution (consumer startup, no committed offset)

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI_Data as DataService<br/>(partition leader node)

    Note over C: Received offset=-1 for orders/5.<br/>AutoOffsetReset = "latest".

    C->>+DAPI_Data: GetOffsets RPC {<br/>topic="orders", partition_id=5,<br/>requests=[{type=OFFSET_LATEST}]}
    DAPI_Data-->>-C: GetOffsetsResponse{offsets=[{type=OFFSET_LATEST, offset=2500}]}

    Note over C: fetchPositions[orders/5] = 2500.<br/>Consumer starts polling from offset 2500 (skips all prior records).
```

---

## AdminClient: inspect committed offsets for a group

```mermaid
sequenceDiagram
    participant Admin as AdminClient
    participant MAPI as ManagementService<br/>(any node)
    participant RH as RaftHost
    participant MFSM as MetadataFSM

    Note over Admin: Operator inspecting consumer lag for "shipping-svc".

    Admin->>+MAPI: ListPartitions RPC {topic="orders"}
    MAPI-->>-Admin: ListPartitionsResponse{partitions=[{id=0,...},{id=1,...},...]}

    Admin->>+MAPI: FetchCommittedOffsets RPC {<br/>group_id="shipping-svc",<br/>partitions=[orders/0..7, payments/0..3]}

    MAPI->>+RH: LookupMetadata(QueryGetGroupOffsets{"shipping-svc", all partitions})
    RH->>+MFSM: Lookup
    MFSM-->>-RH: full offset map
    RH-->>-MAPI: offset map

    MAPI-->>-Admin: FetchCommittedOffsetsResponse{offsets={orders/0→145, ..., payments/3→-1}}

    Note over Admin: For lag: call GetOffsets(LATEST) per partition<br/>then compute lag = latest_offset − committed_offset.<br/>(Not automated in v1 - operator does this externally.)
```

---

## Notes

- **`FetchCommittedOffsets` is on the DataService.** Group-related RPCs (JoinGroup, Heartbeat, LeaveGroup, CommitOffset, FetchCommittedOffsets) are all on `DataService` (port `:9092`), not `ManagementService`. The coordinator is reached via the Data API port.
- **Non-group consumers.** A consumer without a `GroupID` does not call `FetchCommittedOffsets`. It maintains fetch positions locally (via `Seek`) or relies on `AutoOffsetReset` when starting fresh. Committed offsets for non-group consumers are not stored server-side in v1.
- **Partial request.** The client may request only the partitions it is currently assigned. It does not need to fetch offsets for the entire topic. The FSM lookup returns `-1` for any partition not present in `GroupState.Offsets`.
- **No Raft round-trip.** `QueryGetGroupOffsets` uses `LookupMetadata`, which calls dragonboat's `ReadLocalNode` path (no consensus needed). The read is served from the local FSM state snapshot. See [03-raft-fsm.md](./03-raft-fsm.md) for the `ReadLocalNode` / `Lookup` distinction.
- **Lag computation.** Computing consumer lag (`latestOffset − committedOffset`) requires calling `GetOffsets(LATEST)` on each partition leader separately, then subtracting the committed offset. This is a two-step operation in v1 that the client or operator must perform. A dedicated `DescribeConsumerGroupLag` API is post-v1.
