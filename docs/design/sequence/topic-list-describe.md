# Sequence: Topic List and Describe

Read-only admin operations. Both are served from the local Metadata FSM via `ReadLocalNode` - no Raft consensus round-trip. Any broker node can serve them regardless of whether it is the metadata shard leader.

---

## ListTopics

```mermaid
sequenceDiagram
    participant AC as AdminClient
    participant MAPI as ManagementAPI
    participant CC as ClusterCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM<br/>(local replica)

    AC->>+MAPI: ListTopics RPC {}
    MAPI->>+CC: ListTopics(ctx)

    CC->>+RH: LookupMetadata(QueryListTopics)
    RH->>+MFSM: ReadLocalNode(QueryListTopics)
    Note over MFSM: Return snapshot of Topics map values.<br/>No locking needed: dragonboat does not<br/>call Lookup and Update concurrently on<br/>IStateMachine (03-raft-fsm.md §3.4).
    MFSM-->>-RH: []*TopicMeta
    RH-->>-CC: []*TopicMeta

    CC-->>-MAPI: []TopicInfo{name, partitionCount, rf, retention, createdAt}
    MAPI-->>-AC: ListTopicsResponse (OK, topics[])
```

---

## DescribeTopic

```mermaid
sequenceDiagram
    participant AC as AdminClient
    participant MAPI as ManagementAPI
    participant CC as ClusterCoordinator
    participant RH as RaftHost
    participant MFSM as MetadataFSM<br/>(local replica)

    AC->>+MAPI: DescribeTopic RPC {name}
    MAPI->>+CC: DescribeTopic(ctx, name)

    CC->>+RH: LookupMetadata(QueryGetTopic{name})
    RH->>+MFSM: ReadLocalNode(QueryGetTopic)
    MFSM-->>-RH: *TopicMeta or nil
    RH-->>-CC: *TopicMeta or nil

    alt topic not found
        CC-->>MAPI: TopicNotFound
        MAPI-->>AC: DescribeTopicResponse (NOT_FOUND)
    else topic found
        CC->>+RH: LookupMetadata(QueryGetPartitions{name})
        RH->>+MFSM: ReadLocalNode(QueryGetPartitions)
        Note over MFSM: Return all PartitionMeta entries<br/>with Topic == name, sorted by PartitionID.
        MFSM-->>-RH: []*PartitionMeta
        RH-->>-CC: []*PartitionMeta

        Note over CC: Assemble TopicDescription:<br/>TopicInfo from TopicMeta +<br/>[]PartitionInfo from []*PartitionMeta<br/>(PartitionID, ShardID, LeaderNodeID,<br/>LeaderEpoch, ReplicaNodeIDs per partition)

        CC-->>-MAPI: TopicDescription
        MAPI-->>-AC: DescribeTopicResponse (OK)
    end
```

---

## Notes

- **Staleness.** Both operations read from the local Metadata FSM replica. If this node is a follower and the leader has recently committed a metadata change (e.g. a new topic, a leader epoch update), there is a brief window during which this node's view is stale by up to one Raft RTT. This is acceptable: clients retry on `NotLeader` errors from the Data API, which embeds the current leader's address fetched from fresh metadata.
- **Concurrency.** Multiple concurrent `ListTopics` or `DescribeTopic` calls are served independently. `ReadLocalNode` on dragonboat's `IStateMachine` does not conflict with concurrent `Update` calls (dragonboat serialises them per [03-raft-fsm.md §3.4](../03-raft-fsm.md)).
- **DescribeCluster** follows the same pattern: `LookupMetadata(QueryListNodes)` returns `[]*NodeInfo`; the coordinator maps these to `ClusterDescription.Nodes`. Not shown separately because the flow is identical to `ListTopics`.
- **ListPartitions for a topic** (REQUIREMENTS.md §3.2.2) is a variant of `DescribeTopic` that additionally includes `current_offset_range` per partition. The `current_offset_range` is obtained from the Data Coordinator (which calls `LookupPartition(QueryEarliestOffset)` and `LookupPartition(QueryLatestOffset)` on each local partition shard). This cross-coordinator call is not shown here; it is detailed in [05-data-coordinator.md](../05-data-coordinator.md).
