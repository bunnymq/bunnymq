# Sequence: Produce (acks=0)

Fire-and-forget produce. The Data Coordinator enqueues the entry in dragonboat's propose pipeline and returns immediately — without waiting for replication, quorum commit, or FSM application. No offset is returned to the client.

```mermaid
sequenceDiagram
    participant P as Producer (client)
    participant DAPI as DataAPI
    participant DC as DataCoordinator<br/>(this node)
    participant RH as RaftHost<br/>(this node)
    participant DB as dragonboat<br/>(partition shard)
    participant PFSM as PartitionFSM<br/>(async, background)

    P->>+DAPI: Produce RPC {topic, partitionID, batch, acks=0}
    DAPI->>+DC: Produce(ctx, topic, partitionID, batch, AcksZero)

    DC->>RH: LookupMetadata(QueryGetPartition{topic, partitionID})
    Note over RH: ReadLocalNode — no Raft round-trip
    RH-->>DC: *PartitionMeta{LeaderNodeID, ShardID}

    alt this node is NOT the leader
        DC-->>DAPI: NotLeaderError{leaderNodeID, leaderAddress}
        DAPI-->>P: ProduceResponse (FAILED_PRECONDITION)<br/>leader_node_id, leader_address in error detail
        Note over P: Client retries against the leader.<br/>NotLeader is returned even for acks=0 because<br/>only the leader can submit entries to the shard.
    else this node IS the leader
        DC->>DC: registryMu.RLock → shardID → released

        DC->>+RH: ProposePartition(ctx, shardID,<br/>PartitionCommand{CmdAppendBatch, batch})
        RH->>DB: Propose(shardID, entry)
        Note over DB: Entry enqueued in dragonboat's<br/>in-memory propose pipeline.<br/>Returns immediately — does NOT wait for<br/>replication, commit, or FSM application.
        RH-->>-DC: nil (enqueued successfully)

        DC-->>-DAPI: offset = -1
        DAPI-->>-P: ProduceResponse (OK) {offset = -1}

        Note over P: Client receives no durable offset.<br/>If the leader crashes before the entry commits,<br/>the batch is silently lost.<br/>This is the defined semantics of acks=0<br/>(REQUIREMENTS.md §3.6.1).

        Note over DB,PFSM: Asynchronously (order not guaranteed<br/>relative to the response above):
        DB->>PFSM: Update([entry]) when committed
        Note over PFSM: PartitionFSM.Update → Storage.Append<br/>Followers apply identically.<br/>newDataCh closed — long-poll fetchers wake.
    end
```

## Comparison: acks=all vs acks=0

| Property | acks=all | acks=0 |
|---|---|---|
| Returns after | Quorum commit + FSM apply | Propose enqueue |
| Offset returned | Yes, `base_offset` of the batch | No, `-1` |
| Durability | Guaranteed to quorum | Not guaranteed; lost on leader crash before commit |
| Latency | ~Raft RTT × election overhead (~2–5 ms LAN) | Near-zero (~1 µs enqueue) |
| NotLeader check | Yes — only leader can propose | Yes — same: only leader can propose |
| dragonboat call | `SyncPropose` | `Propose` |

## Notes

- **LeaderCheck still required.** Even for acks=0, only the partition shard leader can accept proposals. A follower receiving a Propose call via `ProposePartition` would have dragonboat forward it to the leader internally — but this internal forwarding behaviour in dragonboat is VERIFY (not relied upon in v1). Instead, the Data Coordinator performs a leader check and returns `NotLeader` if it is not the leader, consistent with the acks=all path.

- **No offset.** REQUIREMENTS.md §3.6.1 explicitly states "No offset returned" for acks=0. The `-1` sentinel is the convention; the client library documents that `Send(..., AcksZero)` always returns `offset = -1`.

- **Pipeline full / NodeHost closing.** If dragonboat's propose pipeline is full or the NodeHost is shutting down, `Propose` returns an error. The Data Coordinator maps this to `Unavailable`. This is rare under normal operation. VERIFY the exact dragonboat v4 error type for this condition (Open Question 5 in [05-data-coordinator.md](../05-data-coordinator.md)).
