# Sequence: Fresh Cluster Bootstrap (3 nodes, no existing state)

Three nodes start simultaneously on a brand-new cluster. No metadata shard exists yet; nodes must form it from scratch, elect a leader, and signal readiness before accepting client traffic.

---

## Phase 1 — All three nodes start and form the metadata shard

```mermaid
sequenceDiagram
    participant N1 as Node-1<br/>(process start)
    participant N2 as Node-2<br/>(process start)
    participant N3 as Node-3<br/>(process start)
    participant DB1 as dragonboat<br/>(Node-1)
    participant DB2 as dragonboat<br/>(Node-2)
    participant DB3 as dragonboat<br/>(Node-3)
    participant MFSM1 as MetadataFSM<br/>(Node-1)
    participant MFSM2 as MetadataFSM<br/>(Node-2)
    participant MFSM3 as MetadataFSM<br/>(Node-3)

    Note over N1,N3: All three nodes read their static config:<br/>node_id, listen addresses, initial_members map<br/>{1→"n1:9090", 2→"n2:9090", 3→"n3:9090"}.

    par Node-1 starts
        N1->>DB1: StartCluster(shardID=0, nodeID=1,<br/>initialMembers={1,2,3}, join=false, MetadataFSM{})
        Note over DB1: join=false → this is a fresh shard.<br/>Writes initial Raft log entry (configuration).<br/>Contacts peers to form quorum.
    and Node-2 starts
        N2->>DB2: StartCluster(shardID=0, nodeID=2,<br/>initialMembers={1,2,3}, join=false, MetadataFSM{})
    and Node-3 starts
        N3->>DB3: StartCluster(shardID=0, nodeID=3,<br/>initialMembers={1,2,3}, join=false, MetadataFSM{})
    end

    Note over DB1,DB3: Raft election runs among the three nodes.<br/>Assume Node-1 wins the election.

    DB1->>MFSM1: (leader) — no-op entry committed to confirm leadership
    DB2->>MFSM2: Update(no-op entry) — follower applies
    DB3->>MFSM3: Update(no-op entry) — follower applies

    Note over DB1: Node-1 is now the metadata shard leader.<br/>VERIFY: dragonboat surfaces leader election result via<br/>IStateMachine.Update or a separate notification callback.<br/>Exact mechanism TBD.
```

---

## Phase 2 — Each node initialises its ClusterCoordinator

```mermaid
sequenceDiagram
    participant N1 as Node-1 (leader)
    participant N2 as Node-2 (follower)
    participant N3 as Node-3 (follower)
    participant CC1 as ClusterCoordinator<br/>(Node-1)
    participant CC2 as ClusterCoordinator<br/>(Node-2)
    participant CC3 as ClusterCoordinator<br/>(Node-3)
    participant RH1 as RaftHost (Node-1)
    participant MFSM1 as MetadataFSM (Node-1)

    par All nodes start ClusterCoordinator
        N1->>CC1: Start()
        N2->>CC2: Start()
        N3->>CC3: Start()
    end

    Note over CC1,CC3: Each CC reads initial metadata state from its local FSM.

    CC1->>RH1: LookupMetadata(QueryListTopics)
    RH1->>MFSM1: Lookup(QueryListTopics)
    MFSM1-->>RH1: [] (empty — no topics yet)
    RH1-->>CC1: []

    Note over CC1: No topics → no partition shards to start.<br/>runningShards = {} (empty).

    Note over CC2,CC3: Same lookup on their local FSMs — also empty.

    par All nodes start DataCoordinator
        N1->>N1: DataCoordinator.Start()<br/>shardRegistry = {} (empty)
    and N2->>N2: DataCoordinator.Start()
    and N3->>N3: DataCoordinator.Start()
    end

    Note over CC1: Node-1 (metadata leader) starts background goroutines:<br/>  - reconcileLoop (partition shard lifecycle)<br/>  - leaderSweepLoop (partition leader reporting)<br/>  - sweepGoroutine (consumer group session timeouts)

    Note over CC2,CC3: Non-leader nodes start the same goroutines<br/>but their Raft-write paths are no-ops (leadership guard).
```

---

## Phase 3 — gRPC servers start, cluster signals ready

```mermaid
sequenceDiagram
    participant N1 as Node-1
    participant N2 as Node-2
    participant N3 as Node-3
    participant Client as Client (any)

    par All nodes start gRPC listeners
        N1->>N1: ManagementService.Listen(:9091)<br/>DataService.Listen(:9092)<br/>Ready.
    and N2->>N2: Listen(:9091, :9092). Ready.
    and N3->>N3: Listen(:9091, :9092). Ready.
    end

    Note over N1,N3: All nodes are ready.<br/>No topics, no partitions, no consumer groups.<br/>Cluster is live.

    Client->>N1: ManagementService.DescribeCluster()
    N1-->>Client: DescribeClusterResponse{<br/>nodes=[{id=1,...},{id=2,...},{id=3,...}],<br/>metadata_leader_node_id=1}

    Note over Client: Client can now call CreateTopic to begin using the cluster.
```

---

## Bootstrap readiness ordering

```
Node process start
  │
  ├─ 1. Load config (node_id, peers, data dir, token list)
  ├─ 2. Open / create dragonboat NodeHost
  ├─ 3. StartCluster(shardID=0, join=false) — metadata shard
  ├─ 4. Wait for metadata shard to have a leader
  │      (poll LookupMetadata or wait for first successful SyncPropose/Lookup)
  ├─ 5. ClusterCoordinator.Start() — reads initial FSM state
  ├─ 6. DataCoordinator.Start() — starts with empty shard registry
  ├─ 7. Start background goroutines (reconcile, sweep)
  └─ 8. Start gRPC listeners → signal ready
```

Step 4 blocks until quorum is established. If a node starts and cannot reach a quorum of peers within a configurable timeout (`startupTimeoutMs`, default 30s), it logs a fatal error and exits — this prevents a split-brain from forming with stale state.

---

## Notes

- **`join=false` vs `join=true`.** `join=false` tells dragonboat this is an initial cluster formation, not a node joining an existing cluster. All three nodes must use `join=false` on first boot. On subsequent restarts, nodes use `join=true` — see [startup-node-join.md](./startup-node-join.md).
- **No topic creation at bootstrap.** The cluster starts with zero topics. The first `CreateTopic` call from an admin client triggers partition shard creation across nodes. See [topic-create.md](./topic-create.md).
- **Static initial membership.** The `initialMembers` map is read from config and must be identical on all three nodes. Mis-matching configs produce split brains. Dynamic membership changes (add/remove nodes) are post-v1.
- **VERIFY: dragonboat leader notification.** The exact API by which a node learns it has become the metadata shard leader (to start write-eligible background goroutines) must be verified. Candidate: poll `nh.GetLeaderID(shardID)` on a short ticker after `StartCluster` returns. Alternatively, the first successful `SyncPropose` confirms leadership.
