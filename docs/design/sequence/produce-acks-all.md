# Sequence: Produce (acks=all)

Full flow for a produce request with `acks=all` from gRPC entry to offset returned. Covers both the happy path (this node is the partition leader) and the `NotLeader` path (client must retry elsewhere).

```mermaid
sequenceDiagram
    participant P as Producer (client)
    participant DAPI as DataAPI
    participant DC as DataCoordinator<br/>(this node)
    participant RH as RaftHost<br/>(this node)
    participant DB as dragonboat<br/>(partition shard)
    participant PFSM as PartitionFSM<br/>(leader node — this node)
    participant STR as Storage<br/>(leader node)
    participant PFSMf as PartitionFSM<br/>(follower nodes)

    P->>+DAPI: Produce RPC {topic, partitionID, batch, acks=all}
    DAPI->>+DC: Produce(ctx, topic, partitionID, batch, AcksAll)

    DC->>RH: LookupMetadata(QueryGetPartition{topic, partitionID})
    Note over RH: ReadLocalNode — no Raft round-trip
    RH-->>DC: *PartitionMeta{LeaderNodeID, ShardID}

    alt this node is NOT the leader
        DC-->>DAPI: NotLeaderError{leaderNodeID, leaderAddress}
        DAPI-->>P: ProduceResponse (FAILED_PRECONDITION)<br/>leader_node_id, leader_address in error detail
        Note over P: Client updates leader cache;<br/>retries Produce RPC against leader node.
    else this node IS the leader
        DC->>DC: registryMu.RLock → shardID from shardRegistry → RLock released

        DC->>+RH: SyncProposePartition(ctx, shardID,<br/>PartitionCommand{CmdAppendBatch, batch})
        RH->>+DB: SyncPropose(shardID, entry)

        Note over DB: Raft replication begins.<br/>Entry written to leader WAL;<br/>AppendEntries RPCs sent to follower nodes.

        par Leader applies
            DB->>+PFSM: Update([entry]) on leader (this node)
            PFSM->>+STR: Append(batch)
            Note over STR: Overwrites batch[0:8] with base_offset;<br/>writes to active .log segment;<br/>writes index entry if sampling threshold met;<br/>closes newDataCh (wakes long-poll fetchers)
            STR-->>-PFSM: base_offset (int64)
            PFSM->>STR: Sync() — fsync active log
            PFSM->>PFSM: persistApplied(entry.Index)<br/>atomic sidecar write (applied.idx)
            PFSM-->>-DB: sm.Result{Value: uint64(base_offset)}
        and Followers apply
            DB->>PFSMf: Update([entry]) on each follower node
            Note over PFSMf: Same Append + fsync + sidecar sequence;<br/>followers' base_offset matches leader's.
        end

        Note over DB: Quorum (≥ RF/2 + 1 nodes including leader)<br/>have committed the entry.

        DB-->>-RH: committed; sm.Result{Value: base_offset}
        RH-->>-DC: sm.Result (success)

        DC-->>-DAPI: offset = int64(result.Value)
        DAPI-->>-P: ProduceResponse (OK)<br/>{partition_id, offset = base_offset}
    end
```

## Notes

- **Quorum, not all replicas.** `SyncProposePartition` returns after a quorum commits. Slow or crashed follower nodes do not block the response. dragonboat's Raft implementation guarantees that committed entries are durable as long as a quorum remains.
- **Offset assignment.** Storage assigns the `base_offset` by overwriting `batch[0:8]` before writing to disk. The CRC covers only `records[]` (bytes [38, batch_length)), so overwriting `base_offset` does not invalidate the CRC ([02-storage.md §2](../02-storage.md)).
- **`newDataCh` notification.** `Storage.Append` atomically closes the current `newDataCh` after a successful write. Any goroutine waiting in a long-poll fetch on that channel wakes immediately and retries its read, finding the new data.
- **Follower apply.** Followers apply the same `Update()` call deterministically. Their `base_offset` is derived identically because the batch bytes (with `base_offset` already overwritten by the leader's Storage) are replicated verbatim. VERIFY: confirm dragonboat replicates the FSM-mutated `sm.Entry.Cmd` bytes or the original proposal bytes. If the leader's Storage mutates `batch[0:8]` **before** dragonboat serialises the entry for replication, followers receive the correct `base_offset`. If replication uses the original bytes, Storage on followers will overwrite `batch[0:8]` with its own `nextOffset`, which should be identical (since all replicas start from the same log). Both paths are correct given Raft's serialisation guarantee. Add to Open Question 6 if confirmation is needed.
- **Batch validation.** The gRPC handler in `DataAPI` validates batch size limits (`≤ 4 MiB`, REQUIREMENTS.md §5) and CRC before calling `Produce`. The Data Coordinator trusts the batch is valid.
