# Sequence: Consumer Fetch (data available immediately)

Fetch request where records are available at the requested offset. No long-polling. Covers both the happy path (this node is the leader) and the NotLeader path.

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataAPI
    participant DC as DataCoordinator<br/>(this node)
    participant RH as RaftHost<br/>(this node)
    participant PFSM as PartitionFSM<br/>(this node)
    participant STR as Storage<br/>(this node)

    C->>+DAPI: Fetch RPC {topic, partitionID, offset=100,<br/>maxBytes=1MiB, maxWaitMs=0}
    DAPI->>+DC: Fetch(ctx, topic, partitionID, 100, 1MiB, 0)

    DC->>RH: LookupMetadata(QueryGetPartition{topic, partitionID})
    Note over RH: ReadLocalNode — no Raft round-trip
    RH-->>DC: *PartitionMeta{LeaderNodeID=this node, ShardID}

    alt this node is NOT the leader
        DC-->>DAPI: NotLeaderError{leaderNodeID, leaderAddress}
        DAPI-->>C: FetchResponse (FAILED_PRECONDITION)<br/>leader_node_id, leader_address in error detail
        Note over C: Client updates leader cache;<br/>retries Fetch against the leader node.
    else this node IS the leader
        DC->>DC: registryMu.RLock → shardID → released

        DC->>+RH: LookupPartition(ctx, shardID,<br/>PartitionQuery{QueryRead, offset=100, maxBytes=1MiB})
        RH->>+PFSM: Lookup(PartitionQuery{QueryRead, 100, 1MiB})
        Note over PFSM: No Raft round-trip. Lookup may run<br/>concurrently with Update (dragonboat guarantee<br/>for IOnDiskStateMachine).

        PFSM->>+STR: Read(offset=100, maxBytes=1MiB)
        Note over STR: 1. Binary search offset index (.index file)<br/>   for largest entry with relative_offset ≤ (100 − base_offset).<br/>2. Linear scan .log file forward from that position.<br/>3. Collect complete batches until maxBytes reached<br/>   or end of committed data (LatestOffset).
        STR-->>-PFSM: (records []byte, nextOffset=145, nil)

        PFSM-->>-RH: *ReadResult{Records, NextOffset=145}
        RH-->>-DC: *ReadResult

        DC-->>-DAPI: records ([]byte), nextOffset=145
        DAPI-->>-C: FetchResponse (OK)<br/>{records, next_offset=145}

        Note over C: Client parses batches from records bytes.<br/>Next Fetch call uses offset=145.
    end
```

## What Storage.Read returns

`Read(offset, maxBytes)` returns one or more **complete batches** (never partial). Each batch is in the on-disk/on-wire format ([REQUIREMENTS.md §4.4](../../REQUIREMENTS.md)). The client parses them by reading `batch_length` from the header.

`nextOffset` = `base_offset + record_count` of the last returned batch. The consumer uses this as the `offset` argument in the next `Fetch` call — it is the first offset not yet seen.

## Read path detail

1. **Segment routing.** `Storage` binary-searches its `[]SegmentStorage` slice by `base_offset` to find the segment containing the requested offset. (Acquires `segMu.RLock`; releases before file I/O.)
2. **Index lookup.** Binary search the segment's mmap'd `.index` file for the largest `relative_offset ≤ (offset − segment.base_offset)`. This gives a file position to start scanning.
3. **Log scan.** Read batches sequentially from that position, skipping any batch whose range does not include `offset`. Collect complete batches until `maxBytes` is consumed or data is exhausted.
4. **Boundary.** Reads are bounded by the current `LatestOffset()`, which equals `storage.nextOffset` — the byte frontier of all committed and applied entries. No incomplete batch bytes are ever returned.

## Error cases

| Condition | Storage return | DataCoordinator action |
|---|---|---|
| `offset < EarliestOffset()` | `ErrOffsetOutOfRange` | Returns `OffsetOutOfRange`; client must call `GetEarliestOffset` and seek. |
| `offset >= LatestOffset()` | `(nil, offset, nil)` (empty) | `maxWaitMs=0`: return empty. `maxWaitMs>0`: enter long-poll (see [consumer-fetch-empty-wait.md](./consumer-fetch-empty-wait.md)). |
| I/O error on sealed segment | `err != nil` | Returns retriable error; dragonboat propagates from `Lookup`. |
