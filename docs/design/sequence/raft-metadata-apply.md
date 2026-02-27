# raft-metadata-apply: Applying a Metadata Command (CreateTopic)

This diagram traces the full lifecycle of a `CreateTopic` admin RPC from the client to durable commit. The Admin Client calls the Management API gRPC server, which delegates to the Cluster Coordinator. The Coordinator validates the request, constructs a `CreateTopicCmd`, and calls `SyncProposeMetadata` on the Raft Host wrapper, which serializes the command and calls dragonboat's `SyncPropose` on the metadata shard (shard 0). dragonboat replicates the entry to a quorum of followers; once committed, it calls `Update()` on each replica's `MetadataFSM`. `SyncPropose` returns only after the quorum commit. The Coordinator then starts the partition shards and returns success to the client. A subsequent `LookupMetadata` verifies the new state is visible.

```mermaid
sequenceDiagram
    participant AC as AdminClient
    participant MAPI as ManagementAPI
    participant CC as ClusterCoordinator
    participant RH as RaftHost
    participant DB as dragonboat
    participant FSM_L as MetadataFSM (leader)
    participant FSM_F1 as MetadataFSM (follower 1)
    participant FSM_F2 as MetadataFSM (follower 2)

    AC->>MAPI: CreateTopic RPC {name, partition_count, replication_factor, ...}
    MAPI->>CC: CreateTopic(req)

    CC->>CC: validate: name regex, count >= 1, factor <= cluster_size
    CC->>CC: build CreateTopicCmd {name, partition_count, replica_assignments, created_at_ms}
    Note over CC: created_at_ms = time.Now() here (coordinator, not FSM)

    CC->>RH: SyncProposeMetadata(ctx, CreateTopicCmd)
    RH->>RH: json.Marshal(MetadataCommand{Type: CmdCreateTopic, CreateTopic: &cmd})
    RH->>DB: SyncPropose(ctx, clientSession, cmdBytes)

    Note over DB: Leader appends entry to its WAL; sends AppendEntries to followers
    DB->>FSM_F1: (Raft replication — AppendEntries RPC)
    DB->>FSM_F2: (Raft replication — AppendEntries RPC)
    FSM_F1-->>DB: AppendEntries ACK
    FSM_F2-->>DB: AppendEntries ACK

    Note over DB: Quorum reached (leader + 1 follower). Entry committed.

    DB->>FSM_L: Update(Entry{Index: N, Cmd: cmdBytes})
    FSM_L->>FSM_L: json.Unmarshal → CreateTopicCmd
    FSM_L->>FSM_L: check topic not in Topics map
    loop for each partition 0..partition_count-1
        FSM_L->>FSM_L: shardID = state.NextShardID++
        FSM_L->>FSM_L: Partitions[(name, i)] = PartitionMeta{ShardID: shardID, ...}
    end
    FSM_L->>FSM_L: Topics[name] = TopicMeta{...}
    FSM_L-->>DB: Result{Value: 0}

    DB->>FSM_F1: Update(Entry{Index: N, Cmd: cmdBytes})
    FSM_F1->>FSM_F1: same mutations applied
    DB->>FSM_F2: Update(Entry{Index: N, Cmd: cmdBytes})
    FSM_F2->>FSM_F2: same mutations applied

    DB-->>RH: SyncPropose returns Result{Value: 0}
    RH-->>CC: Result{Value: 0}, nil

    CC->>CC: extract assigned shard IDs from result
    loop for each partition
        CC->>RH: StartPartitionShard(shardID, initialMembers, join=false)
        RH->>DB: StartCluster(initialMembers, partitionFSMFactory, raftConfig)
    end

    CC->>RH: LookupMetadata(ctx, QueryGetTopic{name})
    RH->>DB: ReadLocalNode(shard=0, query)
    DB->>FSM_L: Lookup(MetadataQuery{Type: QueryGetTopic, TopicName: name})
    FSM_L-->>DB: *TopicMeta
    DB-->>RH: *TopicMeta
    RH-->>CC: *TopicMeta

    CC-->>MAPI: success
    MAPI-->>AC: CreateTopic response {shard_ids...}
```

## Participants

| Participant | Role |
|-------------|------|
| `AdminClient` | External caller; sends the CreateTopic gRPC request. |
| `ManagementAPI` | gRPC server; validates auth, delegates to Coordinator. |
| `ClusterCoordinator` | Sole proposer to the metadata shard for topic lifecycle. Adds `created_at_ms` (the only wall-clock read in this flow). |
| `RaftHost` | Serializes command and calls dragonboat. Hides all dragonboat types. |
| `dragonboat` | Handles Raft replication, WAL, and calls FSM.Update() after commit. |
| `MetadataFSM (leader)` | Applies the command; mutations are identical on all replicas. |
| `MetadataFSM (follower)` | Apply the same command after replication; shown in parallel. |

## Edge Cases

- **AlreadyExists:** If the topic already exists when `Update()` runs on the leader, the FSM returns `Result{Value: ErrAlreadyExists}` without modifying state. All replicas return the same result. The Coordinator propagates `AlreadyExists` to the client as per [REQUIREMENTS.md §3.1.1](../REQUIREMENTS.md).
- **Context timeout:** If `SyncPropose` times out (the node is not the leader or quorum is unavailable), the command is not committed. The Coordinator returns a retriable error to the client. The command bytes may have been sent to dragonboat but not yet committed; they are harmless — uncommitted entries are not applied.
- **Coordinator crash after SyncPropose returns but before StartPartitionShard:** On restart, the Coordinator reads the metadata FSM via `LookupMetadata` and detects that the topic exists but its partition shards have not been started. It retries `StartPartitionShard` for each missing shard. This is safe because `StartCluster` is idempotent for already-started shards.
- **Follower apply ordering:** Followers apply `Update()` asynchronously relative to the leader. Reads via `ReadLocalNode` on a follower may transiently see stale state. The Cluster Coordinator always reads from the leader's FSM via `ReadLocalNode` on shard 0 (which dragonboat routes to the local node only if it is the leader or uses stale-read semantics). This is consistent with at-least-once semantics.
