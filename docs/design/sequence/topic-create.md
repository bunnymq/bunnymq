# Sequence: Topic Create

Full flow from gRPC entry on the receiving broker node to partition shards becoming available for produce/fetch on all replica nodes.

```mermaid
sequenceDiagram
    participant AC as AdminClient
    participant MAPI as ManagementAPI
    participant CC as ClusterCoordinator<br/>(proposing node)
    participant RH as RaftHost
    participant DB as dragonboat<br/>(metadata shard)
    participant MFSM as MetadataFSM<br/>(all replica nodes)
    participant CCN as ClusterCoordinator<br/>(all nodes, background)

    AC->>+MAPI: CreateTopic RPC<br/>{name, partition_count, replication_factor, config}
    MAPI->>+CC: CreateTopic(ctx, name, partitions, rf, overrides)

    CC->>RH: LookupMetadata(QueryListNodes)
    Note over RH: ReadLocalNode — no Raft round-trip
    RH-->>CC: [node1, node2, node3] (sorted by node_id)

    Note over CC: Validate: name regex, partitions≥1,<br/>1≤rf≤len(nodes)<br/>Compute replicaNodeIDs[p] for p∈[0,P)<br/>  start = fnv32a(name) % M<br/>  replica r of partition p → nodes[(start+p+r)%M]<br/>Resolve retentionMs, retentionBytes from overrides/defaults

    CC->>+RH: SyncProposeMetadata(CreateTopicCmd{<br/>  name, partitionCount, rf,<br/>  retentionMs, retentionBytes,<br/>  createdAtMs=now, replicaNodeIDs})
    RH->>+DB: SyncPropose → Raft consensus

    Note over DB: Entry replicated to quorum of metadata<br/>shard replicas before commit returns

    DB->>+MFSM: Update(CreateTopicCmd) applied on each replica node
    Note over MFSM: Allocate shardIDs:<br/>  for p in [0,P): shardID = NextShardID+p<br/>  NextShardID += P<br/>Create TopicMeta{name, P, rf, retention, createdAt}<br/>Create PartitionMeta[p]{shardID, replicaNodeIDs, leaderNodeID=0}
    MFSM-->>-DB: sm.Result{Value:0} (success)

    DB-->>-RH: committed (quorum ack)
    RH-->>-CC: sm.Result (success)

    CC-->>-MAPI: TopicInfo{name, partitions=P, rf, ...}
    MAPI-->>-AC: CreateTopicResponse (OK)

    Note over AC: Topic is now visible in metadata.<br/>DescribeTopic returns it immediately.<br/>Partition shards start asynchronously below.

    Note over CCN: reconcileLoop fires on each node<br/>(within reconcile_interval_ms, default 3 s).<br/>If eager_reconcile_on_create=true,<br/>proposing node runs this synchronously<br/>before the response above.

    CCN->>+RH: LookupMetadata(QueryListAllPartitions)
    Note over RH: ReadLocalNode — no Raft round-trip
    RH-->>-CCN: all PartitionMeta[]

    Note over CCN: Detect new shards assigned to this node<br/>that are not in runningShards map

    loop for each new partition shard on this node
        CCN->>RH: StartPartitionShard(shardID, peers, join)
        Note over RH: dragonboat StartCluster:<br/>lowest-nodeID replica: join=false + initialMembers<br/>other replicas: join=true<br/>Raft leader election occurs across replicas
        RH-->>CCN: ok
        Note over CCN: runningShards[shardID] = shardInfo<br/>PartitionFSM.Open() called → Storage.Open()<br/>Crash recovery scan on existing files (empty dir)
    end

    Note over CCN: leaderSweepLoop (≤3 s) detects elected leader,<br/>proposes AssignPartitionLeader to metadata shard.<br/>After that, DescribeTopic returns leader_node_id.
```

## Notes

- **Concurrency:** Multiple concurrent `CreateTopic` RPCs for the same topic are safe. Raft serialises them; one gets `sm.Result{Value:0}`, the rest receive `AlreadyExists` from the FSM.
- **Visibility window:** The topic appears in `ListTopics` / `DescribeTopic` as soon as the metadata command commits (before the gRPC response returns to the client). Partition shards may not yet be running; `LeaderNodeID` in `PartitionMeta` is initially `0` until the first `AssignPartitionLeader` commit.
- **Failure before partition shard start:** If the broker crashes after the metadata command commits but before partition shards start, the reconciliation goroutine starts them on the next boot (step 4 of bootstrap in [04-cluster-coordinator.md §8](../04-cluster-coordinator.md)).
