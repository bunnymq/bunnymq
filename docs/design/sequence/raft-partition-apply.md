# raft-partition-apply: Applying a Produce Batch via the Partition FSM

This diagram shows the produce path for both `acks=all` (via `SyncProposePartition`) and `acks=0` (via `ProposePartition`). In the `acks=all` path the Data Coordinator blocks until a quorum of partition replicas has committed the batch; the assigned `base_offset` is returned to the producer. In the `acks=0` path the Data Coordinator calls dragonboat's async `Propose` and returns immediately with no offset — durability is not guaranteed. The batch bytes travel as the raw payload of the Raft entry: one byte command-type prefix (`0x01`) followed by the batch bytes verbatim. Inside `Update()`, the Partition FSM calls `Storage.Append`, which assigns `base_offset`, writes the batch to the active log segment, and returns the assigned offset in `sm.Result.Value`.

```mermaid
sequenceDiagram
    participant P as Producer
    participant DAPI as DataAPI
    participant DC as DataCoordinator
    participant RH as RaftHost
    participant DB as dragonboat
    participant FSM_L as PartitionFSM (leader)
    participant FSM_F1 as PartitionFSM (follower 1)
    participant STR as Storage (leader)

    P->>DAPI: Produce RPC {topic, partition_id, batch, acks}
    DAPI->>DC: Produce(topic, partitionID, batch, acks)

    DC->>RH: LookupMetadata(QueryGetPartition{topic, partitionID})
    RH-->>DC: PartitionMeta{ShardID: S, LeaderNodeID: L}

    DC->>DC: check LeaderNodeID == this node (or forward to leader node)

    DC->>DC: build cmd = [0x01] + batch bytes

    alt acks == all
        DC->>RH: SyncProposePartition(ctx, shardID=S, cmd)
        RH->>DB: SyncPropose(ctx, clientSession, cmd)

        Note over DB: Leader appends to WAL; replicates to followers
        DB->>FSM_F1: (Raft replication — AppendEntries RPC)
        FSM_F1-->>DB: ACK
        Note over DB: Quorum reached. Entry committed.

        DB->>FSM_L: Update([Entry{Index: N, Cmd: cmd}])
        FSM_L->>STR: Append(batch)
        STR->>STR: write nextOffset → batch[0:8]
        STR->>STR: write batch to active .log
        STR->>STR: conditionally update .index, .timeindex
        STR->>STR: advance nextOffset
        STR->>STR: close(newDataCh); newDataCh = make(chan struct{})
        STR-->>FSM_L: baseOffset, nil

        FSM_L->>FSM_L: persistApplied(index=N)
        Note over FSM_L: fsync .log; atomic rename applied.idx
        FSM_L-->>DB: []Entry{Result{Value: uint64(baseOffset)}}

        DB-->>RH: SyncPropose returns Result{Value: baseOffset}
        RH-->>DC: Result{Value: baseOffset}, nil
        DC-->>DAPI: baseOffset, nil
        DAPI-->>P: Produce response {partition_id, offset: baseOffset}

    else acks == 0
        DC->>RH: ProposePartition(ctx, shardID=S, cmd)
        RH->>DB: Propose(ctx, clientSession, cmd)
        Note over DB: Async; returns immediately after enqueueing
        DB-->>RH: RequestState (ignored)
        RH-->>DC: nil
        DC-->>DAPI: 0 (no offset), nil
        DAPI-->>P: Produce response {no offset}

        Note over DB,FSM_L: Batch is applied asynchronously after quorum commit
        DB->>FSM_L: Update([Entry{...}]) (async, not shown to producer)
        FSM_L->>STR: Append(batch)
        STR-->>FSM_L: baseOffset
        FSM_L->>FSM_L: persistApplied(index)
        FSM_L-->>DB: []Entry{Result{Value: baseOffset}}
    end
```

## Participants

| Participant | Role |
|-------------|------|
| `Producer` | External client; sets `acks` to control durability guarantee. |
| `DataAPI` | gRPC server; validates auth and batch size (max 4 MiB per [REQUIREMENTS.md §5](../REQUIREMENTS.md)). |
| `DataCoordinator` | Routes the produce request to the correct partition shard; dispatches Propose or SyncPropose. |
| `RaftHost` | Wraps dragonboat; serializes command (1-byte type prefix + raw batch). |
| `dragonboat` | Manages Raft replication; calls FSM.Update() after commit. |
| `PartitionFSM (leader)` | Calls Storage.Append; records `persistApplied` after each Update batch. |
| `PartitionFSM (follower)` | Also applies Update() (not shown in detail); their Storage.Append runs identically. |
| `Storage (leader)` | Assigns `base_offset`, writes batch, updates indexes, signals `newDataCh`. |

## Edge Cases

- **Leader forward:** If `LeaderNodeID != this node`, the Data Coordinator does NOT apply the produce locally. It either returns a `NOT_LEADER` error with the actual leader address (for client-side retry) or proxies the request to the leader node via an internal gRPC call. The proxy path is a coordinators concern (out of scope for this document).
- **Batch size enforcement:** Batches exceeding 4 MiB are rejected by the Data API layer before reaching the Coordinator. Storage.Append does not enforce a size limit.
- **Multiple entries in one Update():** dragonboat may batch several committed entries into one `Update(entries []sm.Entry)` call. The FSM applies them in order; `persistApplied` is called once at the end with the last entry's index. Each entry's `baseOffset` is returned individually in its `Result`.
- **acks=0 loss scenario:** If the leader crashes after returning from `ProposePartition` but before the Raft entry is replicated to a quorum, the entry is lost. No client notification is sent. This is by design and documented in [REQUIREMENTS.md §3.6.1](../REQUIREMENTS.md).
- **Fetch long-poll wakeup:** The `close(newDataCh)` inside `Storage.Append` (shown above) wakes any Data Coordinator goroutines waiting in a fetch long-poll for this partition. They will call `Storage.Read` on their next iteration and return the new data to their respective consumers.
