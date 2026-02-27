# raft-leader-election: Partition Shard Leader Failover

This diagram shows what happens from BunnyMQ's perspective when the current leader of a partition shard fails. The actual leader election is dragonboat's responsibility and is treated as a black box: dragonboat detects the missing leader via heartbeat timeout, holds an election among the remaining replicas, and elects a new leader. BunnyMQ observes the outcome through a dragonboat leader-change notification callback registered in the NodeHost wrapper. The NodeHost wrapper proposes an `AssignPartitionLeader` command to the metadata shard so that the new leader is recorded in the Metadata FSM and visible to the Data Coordinator for subsequent produce and fetch requests.

```mermaid
sequenceDiagram
    participant OL as OldLeader (failed)
    participant DB as dragonboat
    participant NL as NewLeader Node
    participant NHW as NodeHostWrapper
    participant CC as ClusterCoordinator
    participant RH as RaftHost
    participant MetaDB as dragonboat (shard 0)
    participant MetaFSM as MetadataFSM
    participant DC as DataCoordinator

    Note over OL: Node crashes or becomes unreachable

    DB->>DB: HeartbeatRTT timer expires; no heartbeat from OldLeader
    DB->>DB: ElectionRTT timeout (10 × RTT = 2 s); start election

    Note over DB,NL: dragonboat election - internal Raft protocol (black box)
    DB->>NL: RequestVote RPCs to all replicas
    NL-->>DB: vote granted
    Note over NL: NewLeader wins quorum; becomes leader for partition shard S

    DB->>NHW: LeaderUpdated callback {shardID: S, nodeID: NL.NodeID, term: T}

    NHW->>CC: OnLeaderChanged(shardID: S, newLeaderNodeID: NL.NodeID, term: T)

    CC->>CC: build AssignPartitionLeaderCmd{topic, partitionID, leaderNodeID: NL.NodeID, leaderEpoch: T, timestamp_ms: now()}
    CC->>RH: SyncProposeMetadata(ctx, AssignPartitionLeaderCmd)
    RH->>MetaDB: SyncPropose(ctx, cs, cmdBytes)

    Note over MetaDB: Replication to metadata shard quorum
    MetaDB->>MetaFSM: Update(Entry{Cmd: AssignPartitionLeaderCmd})
    MetaFSM->>MetaFSM: validate leaderEpoch > current epoch
    MetaFSM->>MetaFSM: Partitions[(topic, partID)].LeaderNodeID = NL.NodeID
    MetaFSM->>MetaFSM: Partitions[(topic, partID)].LeaderEpoch = T
    MetaFSM-->>MetaDB: Result{Value: 0}
    MetaDB-->>RH: Result{Value: 0}
    RH-->>CC: nil

    Note over DC: Next produce or fetch request for this partition

    DC->>RH: LookupMetadata(QueryGetPartition{topic, partitionID})
    RH->>MetaDB: ReadLocalNode(shard=0, query)
    MetaDB->>MetaFSM: Lookup(MetadataQuery{...})
    MetaFSM-->>MetaDB: PartitionMeta{LeaderNodeID: NL.NodeID, LeaderEpoch: T}
    MetaDB-->>RH: PartitionMeta
    RH-->>DC: PartitionMeta{LeaderNodeID: NL.NodeID}

    DC->>DC: route next request to NL.NodeID
```

## Participants

| Participant | Role |
|-------------|------|
| `OldLeader` | The previously leading replica that has crashed or become unreachable. |
| `dragonboat` | Manages the Raft election as a black box; delivers a `LeaderUpdated` callback when a new leader is elected. |
| `NewLeader Node` | The replica elected as the new leader by dragonboat's Raft protocol. |
| `NodeHostWrapper` | Receives the `LeaderUpdated` callback from dragonboat and dispatches it to the Cluster Coordinator. |
| `ClusterCoordinator` | Translates the leader-change event into a metadata command. Uses `time.Now()` here - this is the coordinator, not the FSM. |
| `RaftHost` | Proposes the `AssignPartitionLeader` command to the metadata shard. |
| `dragonboat (shard 0)` | Replicates the metadata update. |
| `MetadataFSM` | Applies the leader update deterministically (validates epoch to reject stale callbacks). |
| `DataCoordinator` | Reads the updated leader from metadata on the next request; resumes routing to the new leader. |

## Edge Cases

- **In-flight produce requests during failover:** Produce requests using `SyncProposePartition` on the old leader will fail with a dragonboat error (e.g., `ErrShardClosed` or timeout). The Data Coordinator returns a retriable error to the client. Clients with retry logic will retry after the metadata reflects the new leader.
- **Stale `LeaderUpdated` callback:** The `LeaderUpdated` callback may fire before the new leader has fully applied all committed entries from the old leader's term. This is safe: dragonboat guarantees the new leader has at least the last committed entry. The `AssignPartitionLeader` command's `leaderEpoch` (set to the new Raft term `T`) prevents a stale callback from overwriting a more recent leader assignment.
- **Split-brain impossible:** dragonboat's Raft guarantees that at most one leader exists per shard per term. The `leaderEpoch` guard in the Metadata FSM's `Update()` (validates `epoch > current`) provides an additional safeguard at the application level.
- **Old leader comes back:** When the old node recovers, dragonboat detects it is not the leader and switches it to follower mode for shard S. Its `PartitionFSM.Update()` begins receiving missed entries from the new leader. No BunnyMQ-level action is needed.
- **`LeaderUpdated` callback thread safety:** The callback is invoked from dragonboat's internal goroutine. The NodeHostWrapper must dispatch to the Cluster Coordinator via a channel or goroutine to avoid blocking dragonboat's internal event loop.
