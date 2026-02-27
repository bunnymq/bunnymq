# Sequence: Topic Delete

Covers the synchronous metadata removal followed by asynchronous physical teardown of partition Raft shards and storage directories.

```mermaid
sequenceDiagram
    participant AC as AdminClient
    participant MAPI as ManagementAPI
    participant CC as ClusterCoordinator<br/>(proposing node)
    participant RH as RaftHost
    participant DB as dragonboat<br/>(metadata shard)
    participant MFSM as MetadataFSM<br/>(all replica nodes)
    participant CCN as ClusterCoordinator<br/>(all nodes, background)
    participant DC as DataCoordinator<br/>(any node, in-flight)

    AC->>+MAPI: DeleteTopic RPC {name}
    MAPI->>+CC: DeleteTopic(ctx, name)

    CC->>RH: LookupMetadata(QueryGetTopic{name})
    Note over RH: ReadLocalNode — no Raft round-trip
    RH-->>CC: *TopicMeta (exists) or nil

    alt topic not found
        CC-->>MAPI: TopicNotFound
        MAPI-->>AC: DeleteTopicResponse (NOT_FOUND)
    else topic found
        CC->>+RH: SyncProposeMetadata(DeleteTopicCmd{name})
        RH->>+DB: SyncPropose → Raft consensus

        Note over DB: Entry replicated to quorum before commit

        DB->>+MFSM: Update(DeleteTopicCmd) on each replica node
        Note over MFSM: Remove TopicMeta[name]<br/>Remove PartitionMeta[(name,0)…(name,P-1)]<br/>Subsequent Lookup for these keys returns nil
        MFSM-->>-DB: sm.Result{Value:0}

        DB-->>-RH: committed
        RH-->>-CC: ok

        CC-->>-MAPI: nil (success)
        MAPI-->>-AC: DeleteTopicResponse (OK)

        Note over AC: Topic is removed from metadata.<br/>Physical cleanup proceeds asynchronously.

        Note over DC: Any in-flight Produce/Fetch RPCs that now<br/>call LookupMetadata(QueryGetPartition{name,p})<br/>receive nil → return TopicNotFound to client.
        Note over DC: In-flight SyncProposePartition calls<br/>complete normally (Raft shard still running);<br/>next routing attempt returns TopicNotFound.

        Note over CCN: reconcileLoop fires on each node<br/>(within reconcile_interval_ms, default 3 s)

        CCN->>+RH: LookupMetadata(QueryListAllPartitions)
        RH-->>-CCN: all PartitionMeta[] — topic's partitions absent

        Note over CCN: Detect shards in runningShards with no<br/>corresponding PartitionMeta (orphaned)

        loop for each orphaned partition shard on this node
            CCN->>RH: StopPartitionShard(shardID)
            Note over RH: dragonboat shuts down the Raft shard;<br/>PartitionFSM.Close() → Storage.Close()
            RH-->>CCN: ok
            Note over CCN: delete(runningShards, shardID)
            CCN->>CCN: os.RemoveAll(partitionDir)
            Note over CCN: Storage directory deleted:<br/>.log, .index, .timeindex files removed
        end

        Note over CCN: All partition shards and storage for<br/>the deleted topic are now cleaned up.
    end
```

## Notes

- **Async teardown window:** Between the `DeleteTopicResponse (OK)` and the completion of the reconciliation loop, partition shards are still running. During this window, produces routed to those shards succeed at the Raft level — but since the topic is absent from metadata, the routing layer returns `TopicNotFound` before reaching the shard, so no new data is actually written.
- **Concurrent operations during teardown:** A `CreateTopic` for the same name issued after `DeleteTopic` returns can proceed immediately (metadata is clean). If the new topic creation assigns the same shard IDs (impossible — `NextShardID` is monotonically increasing and never reuses IDs), there would be a conflict. Because shard IDs are never reused, old shard teardown and new shard startup for the same name cannot collide.
- **Storage directory deletion failure:** If `os.RemoveAll` fails (e.g. file descriptor still held), the reconcile goroutine removes the shard from `runningShards` regardless. Stale storage directories are not retried inline; a manual cleanup or restart is needed. This is acceptable for v1.
- **Multi-node consistency:** The metadata FSM update applies identically on all replica nodes. The reconcile goroutine on each node independently detects the orphaned shard and cleans up its local storage. No coordination between nodes is needed for teardown.
