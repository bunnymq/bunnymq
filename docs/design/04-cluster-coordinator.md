# Cluster Coordinator - Detailed Design

The Cluster Coordinator is the **sole writer** to the Metadata FSM for topic, partition, and node lifecycle operations. It handles all admin-plane operations - topic create, delete, and alter; cluster description - and manages the lifecycle of partition Raft shards on the local node: starting them when new partitions are assigned to this node and stopping them when partitions are deleted. The Data Coordinator and Group Coordinator read metadata via `LookupMetadata`; the Cluster Coordinator is the only module that writes it. It does not handle produce or fetch traffic.

See [00-overview.md §1](./00-overview.md), [01-modules.md §4](./01-modules.md), and [03-raft-fsm.md §2–3](./03-raft-fsm.md) for context on the Raft host, Metadata FSM state, and shard ID conventions.

---

## 1. Responsibilities

### What the Cluster Coordinator does

- **Topic lifecycle.** CreateTopic, DeleteTopic, AlterTopicPartitionCount, AlterTopicRetention. All mutations are serialised through `SyncProposeMetadata` to the metadata shard.
- **Partition assignment.** Computes the replica-to-node mapping for every new partition using the deterministic round-robin algorithm (§4). The assignment is included in the metadata command payload.
- **Partition shard lifecycle on this node.** A background reconciliation goroutine periodically compares the set of partition shards this node should be running (from metadata) against those it is actually running, and calls `StartPartitionShard` / `StopPartitionShard` accordingly (§7).
- **Leader epoch tracking.** Detects leader changes for partition shards on this node and commits `AssignPartitionLeader` commands to the metadata shard so clients can discover the current leader (§6).
- **Cluster description.** DescribeCluster, ListPartitions - read-only lookups against the local Metadata FSM with no Raft round-trip.
- **Bootstrap.** Starts the metadata shard replica, waits for leader election, registers this node, runs an initial reconciliation, and signals readiness (§8).

### What the Cluster Coordinator does NOT do

- **Does not route produce or fetch requests.** That is the Data Coordinator's responsibility ([05-data-coordinator.md](./05-data-coordinator.md)).
- **Does not manage consumer group state.** That is the Group Coordinator's responsibility ([08-consumer-groups.md](./08-consumer-groups.md)).
- **Does not call `internal/storage` directly.** All partition data access is mediated by the Partition FSM via dragonboat.
- **Does not initiate outbound gRPC calls.** The Cluster Coordinator is called by `internal/api/management`; it does not contact other broker nodes over gRPC.

---

## 2. Public Interface

```go
// ClusterCoordinator manages the topic and partition lifecycle for the cluster.
// All methods are safe to call concurrently from multiple gRPC handler goroutines.
// Methods that write metadata issue SyncProposeMetadata and block until quorum commit.
type ClusterCoordinator struct { /* unexported fields */ }

// CreateTopic creates a new topic with the given name, partition count,
// replication factor, and optional retention overrides. Returns TopicInfo on
// success. Returns AlreadyExists if a topic with this name already exists.
// Returns InvalidArgument if any input fails validation (name format, counts out
// of range, replicationFactor > cluster size). Returns Unavailable if the
// metadata shard has no leader.
func (cc *ClusterCoordinator) CreateTopic(
    ctx context.Context,
    name string,
    partitionCount int32,
    replicationFactor int32,
    configOverrides TopicConfigOverrides,
) (TopicInfo, error)

// DeleteTopic removes the topic from the Metadata FSM and returns immediately.
// Physical teardown of Raft partition shards and storage directories is
// asynchronous (background reconciliation goroutine). All subsequent produce
// and fetch calls against the deleted topic receive TopicNotFound.
// Returns TopicNotFound if the topic does not exist.
func (cc *ClusterCoordinator) DeleteTopic(ctx context.Context, name string) error

// ListTopics returns a summary of all topics in the cluster. Served from the
// local Metadata FSM via ReadLocalNode - no Raft consensus round-trip.
func (cc *ClusterCoordinator) ListTopics(ctx context.Context) ([]TopicInfo, error)

// DescribeTopic returns full metadata for a named topic, including per-partition
// leader node ID, leader epoch, replica node IDs, and shard ID. Served from the
// local Metadata FSM - no Raft consensus round-trip.
// Returns TopicNotFound if the topic does not exist.
func (cc *ClusterCoordinator) DescribeTopic(ctx context.Context, name string) (TopicDescription, error)

// AlterTopicPartitionCount increases the partition count of an existing topic.
// newCount must be strictly greater than the current count; decreasing is not
// supported (REQUIREMENTS.md §3.1.5). New partition shards are started
// asynchronously by the reconciliation goroutine.
// Returns InvalidArgument if newCount <= current count.
// Returns TopicNotFound if the topic does not exist.
func (cc *ClusterCoordinator) AlterTopicPartitionCount(
    ctx context.Context,
    name string,
    newCount int32,
) error

// AlterTopicRetention updates the retention configuration for an existing topic.
// The metadata change commits synchronously. RetentionConfig commands are then
// fired asynchronously (acks=0) to each partition shard; storage picks up the
// new config on the next retention tick (within retention_check_interval_ms).
// retentionMs = -1 leaves the existing time retention unchanged.
// retentionBytes = -1 means unlimited (REQUIREMENTS.md §4.1); 0 leaves unchanged.
// Returns TopicNotFound if the topic does not exist.
func (cc *ClusterCoordinator) AlterTopicRetention(
    ctx context.Context,
    name string,
    retentionMs int64,
    retentionBytes int64,
) error

// DescribeCluster returns the current cluster topology: all registered nodes
// with their node IDs and Raft addresses. Served from the local Metadata FSM -
// no Raft consensus round-trip.
func (cc *ClusterCoordinator) DescribeCluster(ctx context.Context) (ClusterDescription, error)
```

### Supporting types

```go
// TopicConfigOverrides carries optional per-topic retention overrides.
// A nil field means "use cluster default from config".
type TopicConfigOverrides struct {
    RetentionMs    *int64
    RetentionBytes *int64
}

type TopicInfo struct {
    Name              string
    PartitionCount    int32
    ReplicationFactor int32
    RetentionMs       int64
    RetentionBytes    int64
    CreatedAtMs       int64
}

type TopicDescription struct {
    TopicInfo
    Partitions []PartitionInfo
}

type PartitionInfo struct {
    PartitionID    int32
    ShardID        uint64
    LeaderNodeID   uint64
    LeaderEpoch    int64
    ReplicaNodeIDs []uint64
}

type ClusterDescription struct {
    Nodes []NodeDescriptor
}

type NodeDescriptor struct {
    NodeID  uint64
    Address string
}
```

---

## 3. Method Flows

### 3.1 CreateTopic

1. **Validate.** Check: `name` matches `[a-zA-Z0-9._-]{1,255}`, `partitionCount >= 1`, `1 <= replicationFactor <= len(cluster nodes)`. Return `InvalidArgument` on any violation.
2. **Read node list.** `LookupMetadata(QueryListNodes)` to get the current node list (nodes are registered at startup; the list is static during operation per REQUIREMENTS.md §3.2.3). Nodes are sorted by `node_id` ascending for deterministic assignment.
3. **Compute replica assignments.** Apply the round-robin algorithm (§4) to produce `replicaNodeIDs[p]` for each partition `p ∈ [0, partitionCount)`.
4. **Resolve retention.** Merge `configOverrides` with cluster defaults; produce concrete `retentionMs` and `retentionBytes` values.
5. **Propose.** `SyncProposeMetadata(CreateTopicCmd{name, partitionCount, replicationFactor, retentionMs, retentionBytes, createdAtMs: time.Now().UnixMilli(), replicaNodeIDs})`. Blocks until quorum commit. The `createdAtMs` is set by the proposer, not the FSM, to satisfy the determinism contract ([03-raft-fsm.md §3.6](./03-raft-fsm.md)).
6. **Handle FSM result.** `sm.Result.Value != 0` means an error; if the error code is `AlreadyExists`, return `AlreadyExists`. Other codes return `Unknown`.
7. **Return.** Return `TopicInfo` derived from the command payload on success. The reconciliation goroutine starts local partition shards asynchronously (within one reconcile interval, ≤ `reconcile_interval_ms`).

> **Partition shard startup latency.** To minimise the window during which new partition shards are unavailable, `CreateTopic` optionally triggers `reconcileOnce` synchronously before returning (Open Question 6). This is a configurable behaviour.

### 3.2 DeleteTopic

1. **Validate.** `LookupMetadata(QueryGetTopic{name})`. If not found, return `TopicNotFound`.
2. **Propose.** `SyncProposeMetadata(DeleteTopicCmd{name})`. Blocks until quorum commit. On commit, the FSM removes `TopicMeta` and all `PartitionMeta` entries for this topic; subsequent `LookupMetadata` for these partitions returns `nil`.
3. **Return.** Return `nil` immediately. Physical teardown is asynchronous - the reconciliation goroutine detects shards in `runningShards` with no corresponding `PartitionMeta` and stops them (§7.3).

### 3.3 ListTopics

1. `LookupMetadata(QueryListTopics)`.
2. Map `[]*TopicMeta` to `[]TopicInfo` and return. No Raft round-trip; reads from the local FSM state.

### 3.4 DescribeTopic

1. `LookupMetadata(QueryGetTopic{name})` - returns `*TopicMeta` or `nil`.
2. If nil, return `TopicNotFound`.
3. `LookupMetadata(QueryGetPartitions{name})` - returns `[]*PartitionMeta` for all partitions.
4. Assemble and return `TopicDescription`. No Raft round-trip.

### 3.5 AlterTopicPartitionCount

1. **Validate.** `LookupMetadata(QueryGetTopic{name})`. Return `TopicNotFound` if absent. Check `newCount > currentPartitionCount`; return `InvalidArgument` if not.
2. **Compute new replica assignments.** For partition IDs `[currentCount, newCount)`, apply the same round-robin formula used in CreateTopic (§4), continuing the cycle from where the initial assignment left off.
3. **Propose.** `SyncProposeMetadata(AlterTopicPartCountCmd{name, newCount, newReplicaAssignments})`. Blocks until quorum commit. The FSM appends new `PartitionMeta` entries and allocates their shard IDs.
4. **Return.** Return `nil` on success. Reconciliation goroutine starts the new partition shards.

### 3.6 AlterTopicRetention

1. **Validate.** `LookupMetadata(QueryGetTopic{name})`. Return `TopicNotFound` if absent.
2. **Metadata update.** `SyncProposeMetadata(AlterTopicRetentionCmd{name, retentionMs, retentionBytes})`. Blocks until quorum commit.
3. **Propagate to partition shards.** For each `PartitionMeta` of this topic, fire `ProposePartition(shardID, RetentionConfigCmd{retentionMs, retentionBytes})` (acks=0; dragonboat `Propose`, no wait). These proposals are issued concurrently via goroutines. Errors are logged at `warn`; they do not fail the RPC. Storage picks up the new config on its next retention tick.
4. **Return.** Return `nil` after the metadata commit.

### 3.7 DescribeCluster

1. `LookupMetadata(QueryListNodes)`.
2. Map `[]*NodeInfo` to `ClusterDescription` and return. No Raft round-trip.

---

## 4. Partition Assignment Algorithm

Given:
- `nodes []NodeInfo` - all cluster nodes sorted by `node_id` ascending (M nodes total).
- `topicName string`
- `partitionID int32` (0-indexed)
- `replicationFactor int32` (RF)

```go
// fnv32a returns the FNV-1a 32-bit hash of a UTF-8 string.
func fnv32a(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}

// assignReplicas returns the RF node IDs for one partition.
// The first entry is the initial leader candidate.
func assignReplicas(nodes []NodeInfo, topicName string, partitionID int32, rf int32) []uint64 {
    start := int(fnv32a(topicName)) % len(nodes)
    replicas := make([]uint64, rf)
    for r := int32(0); r < rf; r++ {
        replicas[r] = nodes[(start+int(partitionID)+int(r))%len(nodes)].NodeID
    }
    return replicas
}
```

**Properties:**
- *Deterministic.* Given the same node list, topic name, and partition ID, always produces the same assignment.
- *Spread leaders.* The first replica (initial leader candidate) for partition `p` is at position `(start + p) % M`, cycling through all nodes as `p` increases.
- *Even distribution.* With M nodes, P partitions, and RF replicas, each node hosts approximately `P × RF / M` replicas.

**For `AlterTopicPartitionCount`:** apply the same formula with `partitionID` ranging over `[currentCount, newCount)`. This extends the assignment without disturbing existing partition placements.

---

## 5. Shard ID Allocation

Shard IDs are assigned by the Metadata FSM during `Update(CreateTopicCmd)`. The FSM's `NextShardID` counter (persisted in `MetadataState`; initialized to 1; shard 0 is the metadata shard) is consumed atomically:

```
shardID for (topic, partitionID p) = NextShardID_before_apply + p
```

The FSM increments `NextShardID` by `partitionCount` in one command application. For `AlterTopicPartCount`, the same pattern extends the counter by `newCount − currentCount`.

The counter never resets and shard IDs from deleted topics are never reused. The Cluster Coordinator retrieves shard IDs from `PartitionMeta` via `LookupMetadata(QueryGetPartition{topic, partitionID})`. See [03-raft-fsm.md §2](./03-raft-fsm.md) for the full counter specification and snapshot semantics.

---

## 6. Leader Epoch Tracking

When a partition shard's Raft leader changes, the Cluster Coordinator must commit an `AssignPartitionLeader` command so that clients and the Data Coordinator can discover the new leader.

### Chosen mechanism - periodic sweep (v1)

A background goroutine sweeps all locally running partition shards every `leader_check_interval_ms` (default: 3 000 ms):

```go
func (cc *ClusterCoordinator) leaderSweepLoop(ctx context.Context) {
    ticker := time.NewTicker(time.Duration(cc.config.LeaderCheckIntervalMs) * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            cc.sweepLeaders(ctx)
        case <-ctx.Done():
            return
        }
    }
}

func (cc *ClusterCoordinator) sweepLeaders(ctx context.Context) {
    cc.shardMu.RLock()
    shards := maps.Clone(cc.runningShards) // snapshot under lock
    cc.shardMu.RUnlock()

    for shardID, info := range shards {
        // VERIFY: exact dragonboat v4 method for querying the current leader.
        // Proposed signature: NodeHost.GetLeaderID(clusterID uint64) (leaderID uint64, term uint64, valid bool)
        leaderID, term, valid := cc.raftHost.GetLeaderID(shardID)
        if !valid {
            continue // election in progress; skip
        }
        cc.leaderMu.Lock()
        last := cc.lastKnownLeader[shardID]
        cc.leaderMu.Unlock()
        if leaderID == last.nodeID && term == last.term {
            continue // no change
        }
        err := cc.raftHost.SyncProposeMetadata(ctx, MetadataCommand{
            Type: CmdAssignPartitionLeader,
            AssignPartitionLeader: &AssignPartitionLeaderCmd{
                Topic:        info.Topic,
                PartitionID:  info.PartitionID,
                LeaderNodeID: leaderID,
                LeaderEpoch:  int64(term),
            },
        })
        if err != nil {
            cc.logger.Warn("failed to update partition leader in metadata", ...)
            continue
        }
        cc.leaderMu.Lock()
        cc.lastKnownLeader[shardID] = leaderRecord{leaderID, term}
        cc.leaderMu.Unlock()
    }
}
```

The Metadata FSM validates that the incoming `LeaderEpoch` exceeds the stored epoch before applying the update - duplicate or stale proposals from multiple nodes are safely ignored ([03-raft-fsm.md §3.2](./03-raft-fsm.md)).

### Alternative - dragonboat event listener (future)

VERIFY: dragonboat v4 may expose a leader-change callback via `NodeHostConfig.RaftEventListener` (candidate type: `raft.IEventListener`, method: `LeaderUpdated(LeaderInfo)`). If confirmed available, the callback approach should replace the periodic sweep: it eliminates the staleness window (up to `leader_check_interval_ms`) and reduces unnecessary Raft proposals. The callback would enqueue a leader-change event on a buffered channel; a dedicated goroutine drains the channel and calls `SyncProposeMetadata`. The sweep can remain as a safety net with a longer interval (e.g. 30 s) to catch any missed events.

---

## 7. Partition Shard Lifecycle - Reconciliation Goroutine

One background goroutine per node reconciles the set of running partition shards against the set declared in the Metadata FSM.

### 7.1 Local shard registry

```go
type shardInfo struct {
    Topic       string
    PartitionID int32
    ShardID     uint64
    // nodeID → raftAddress for all shard members (from PartitionMeta + Nodes lookup).
    Peers map[uint64]string
}

// Protected by shardMu.
runningShards map[uint64]shardInfo // key: shardID
```

### 7.2 Reconciliation loop

```go
func (cc *ClusterCoordinator) reconcileLoop(ctx context.Context) {
    ticker := time.NewTicker(time.Duration(cc.config.ReconcileIntervalMs) * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            cc.reconcileOnce(ctx)
        case <-ctx.Done():
            return
        }
    }
}
```

### 7.3 reconcileOnce

```go
func (cc *ClusterCoordinator) reconcileOnce(ctx context.Context) {
    // 1. Fetch all PartitionMeta assigned to this node.
    allPartitions, err := cc.raftHost.LookupMetadata(ctx, MetadataQuery{Type: QueryListAllPartitions})
    if err != nil {
        cc.logger.Warn("reconcile: metadata lookup failed", zap.Error(err))
        return
    }
    nodes, _ := cc.raftHost.LookupMetadata(ctx, MetadataQuery{Type: QueryListNodes})

    expected := map[uint64]shardInfo{}
    for _, pm := range allPartitions {
        if slices.Contains(pm.ReplicaNodeIDs, cc.config.NodeID) {
            expected[pm.ShardID] = shardInfo{
                Topic:       pm.Topic,
                PartitionID: pm.PartitionID,
                ShardID:     pm.ShardID,
                Peers:       buildPeerMap(pm.ReplicaNodeIDs, nodes),
            }
        }
    }

    cc.shardMu.Lock()
    defer cc.shardMu.Unlock()

    // 2. Start shards that should be running but are not.
    for shardID, info := range expected {
        if _, running := cc.runningShards[shardID]; !running {
            go cc.startShard(shardID, info) // non-blocking
        }
    }

    // 3. Stop shards that are running but no longer expected (topic deleted).
    for shardID, info := range cc.runningShards {
        if _, wanted := expected[shardID]; !wanted {
            go cc.stopShard(shardID, info) // non-blocking
        }
    }
}
```

### 7.4 startShard

```go
func (cc *ClusterCoordinator) startShard(shardID uint64, info shardInfo) {
    // VERIFY: joining semantics in dragonboat v4 StartCluster:
    //   join=false + non-nil initialMembers: start a new shard (first time).
    //   join=true: join an existing shard already known to the cluster.
    // Proposed heuristic: if this node has the lowest nodeID in ReplicaNodeIDs,
    // it starts the shard (join=false, initialMembers=all replicas).
    // All other replicas join an existing shard (join=true, initialMembers=all).
    join := cc.config.NodeID != slices.Min(replicaNodeIDs(info.Peers))
    err := cc.raftHost.StartPartitionShard(info.ShardID, info.Peers, join)
    if err != nil {
        cc.logger.Warn("failed to start partition shard",
            zap.Uint64("shard_id", shardID), zap.Error(err))
        return
    }
    cc.shardMu.Lock()
    cc.runningShards[shardID] = info
    cc.shardMu.Unlock()
    cc.logger.Info("partition shard started",
        zap.String("topic", info.Topic), zap.Int32("partition_id", info.PartitionID))
}
```

### 7.5 stopShard

```go
func (cc *ClusterCoordinator) stopShard(shardID uint64, info shardInfo) {
    err := cc.raftHost.StopPartitionShard(shardID)
    if err != nil {
        cc.logger.Warn("failed to stop partition shard",
            zap.Uint64("shard_id", shardID), zap.Error(err))
        // Remove from runningShards anyway to avoid a retry storm.
    }
    cc.shardMu.Lock()
    delete(cc.runningShards, shardID)
    cc.shardMu.Unlock()

    dir := partitionDir(cc.config.DataDir, info.Topic, info.PartitionID)
    if err := os.RemoveAll(dir); err != nil {
        cc.logger.Warn("failed to delete partition storage directory",
            zap.String("dir", dir), zap.Error(err))
    }
    cc.logger.Info("partition shard stopped and storage removed",
        zap.String("topic", info.Topic), zap.Int32("partition_id", info.PartitionID))
}
```

**Reconcile interval:** default 3 000 ms. A shorter interval reduces partition startup latency after CreateTopic at the cost of more frequent Metadata FSM reads. See Open Question 6 for the option to trigger reconcileOnce eagerly from CreateTopic.

---

## 8. Bootstrap Behavior

On process start, performed synchronously before `cmd/bunnymq` declares the broker ready and starts gRPC servers:

```
Step 1 - Start metadata shard replica.
  raftHost.StartCluster(initialMembers, join=false, metadataFSMFactory, rcMetadata)
  initialMembers is populated from config.Peers (nodeID → raftAddress map).
  join=false for a fresh cluster; join=true when a node rejoins (VERIFY: exact semantics).

Step 2 - Wait for metadata shard leader.
  Poll LookupMetadata(QueryListNodes) every 200 ms.
  Success when a response returns without error (any leader is elected).
  Timeout: config.BootstrapTimeoutMs (default: 30 000 ms). Return fatal error on timeout.

Step 3 - Register this node.
  SyncProposeMetadata(RegisterNodeCmd{NodeID: config.NodeID, Address: config.RaftAddress})
  The FSM applies this idempotently (REQUIREMENTS.md §3.2.3; node list is static).

Step 4 - Run initial partition reconciliation.
  Call reconcileOnce(ctx) synchronously.
  Starts all partition shards assigned to this node before readiness is signalled.
  This ensures the broker can serve produce and fetch requests immediately on startup.

Step 5 - Start background goroutines.
  reconcileLoop goroutine
  leaderSweepLoop goroutine

Step 6 - Signal readiness.
  Close a done channel that cmd/bunnymq selects on before starting gRPC servers.
```

---

## 9. Routing Note

The Cluster Coordinator does **not** route produce or fetch requests. It has no knowledge of record offsets, consumer positions, or partition data. All partition-data routing is the Data Coordinator's responsibility ([05-data-coordinator.md](./05-data-coordinator.md)).

Clients discover the leader for a partition via `DescribeTopic` (which reads `PartitionMeta.LeaderNodeID`) or by receiving a `NotLeader` error response from the Data API, which includes the leader's address.

---

## 10. Concurrency Model

| Operation | Goroutine | Raft interaction | Local state touched |
|---|---|---|---|
| CreateTopic / DeleteTopic / AlterTopic* | gRPC handler goroutine (N concurrent) | SyncProposeMetadata (blocking) | Reads `runningShards` (shardMu.RLock for queries only) |
| ListTopics / DescribeTopic / DescribeCluster | gRPC handler goroutine (N concurrent) | LookupMetadata (non-blocking) | None |
| reconcileOnce | Single background ticker goroutine | LookupMetadata | shardMu.Lock for start/stop |
| startShard / stopShard | Goroutines spawned by reconcileOnce | StartPartitionShard / StopPartitionShard | shardMu.Lock for map update |
| leaderSweepLoop | Single background ticker goroutine | GetLeaderID (read), SyncProposeMetadata (write) | leaderMu for lastKnownLeader map |

**No additional serialisation is needed at the coordinator level for admin RPCs.** Raft is the serialiser: concurrent `CreateTopic` calls for the same topic result in one `sm.Result{Value:0}` (success) and one `sm.Result{Value:AlreadyExists}`. Concurrent `DeleteTopic` + `CreateTopic` for the same name are ordered by Raft commit sequence.

**Lock inventory:**

| Lock | Protects | Writers | Readers |
|---|---|---|---|
| `shardMu sync.RWMutex` | `runningShards map[uint64]shardInfo` | reconcileOnce, startShard, stopShard | leaderSweepLoop (via snapshot) |
| `leaderMu sync.Mutex` | `lastKnownLeader map[uint64]leaderRecord` | leaderSweepLoop | leaderSweepLoop |

---

## 11. Failure Modes

| Situation | Behavior |
|---|---|
| Metadata shard has no leader | `SyncProposeMetadata` returns `ErrNoLeader` (dragonboat). Coordinator returns `Unavailable`. |
| Raft propose timeout (ctx deadline) | `SyncProposeMetadata` returns timeout error. Coordinator returns mapped to `codes.DeadlineExceeded`. |
| Topic already exists (`CreateTopic`) | FSM returns `AlreadyExists` in `sm.Result`. Coordinator returns `AlreadyExists`. |
| Topic not found (`Delete`, `Alter*`, `Describe`) | Preliminary `LookupMetadata` returns `nil`. Coordinator returns `TopicNotFound` before proposing. |
| `newCount <= currentCount` (`AlterPartitionCount`) | Validated locally before proposing. Returns `InvalidArgument`. |
| `replicationFactor > cluster size` | Validated locally before proposing. Returns `InvalidArgument`. |
| Partition shard start failure (in reconcileOnce) | Logged at `warn`. Not added to `runningShards`. Retried on next reconcile tick (≤ `reconcile_interval_ms`). |
| Partition shard stop failure (in reconcileOnce) | Logged at `warn`. Removed from `runningShards` regardless to prevent retry loops. Storage directory deletion also attempted; failure is logged. |
| `ProposePartition` RetentionConfig fails | Logged at `warn`. Storage retains old retention config until next restart or re-apply. Not a fatal condition. |

---

## 12. Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `coordinator.reconcile_interval_ms` | 3 000 | Interval between partition shard reconciliation sweeps. |
| `coordinator.leader_check_interval_ms` | 3 000 | Interval between partition leader sweeps. |
| `coordinator.bootstrap_timeout_ms` | 30 000 | Maximum wait for metadata shard leader election at startup. |
| `coordinator.eager_reconcile_on_create` | true | If true, trigger reconcileOnce synchronously within CreateTopic before returning, to minimise partition shard startup latency. |

---

## 13. Open Questions

1. **VERIFY - dragonboat leader notification API.** Does dragonboat v4 expose an `IEventListener` interface (or equivalent, e.g. `config.NodeHostConfig.RaftEventListener`) that fires synchronous callbacks on leader change? Exact interface name and method signature needed. If confirmed, replace the periodic sweep for the leader-epoch update path and retain the sweep only as a reconciliation safety net at a longer interval (e.g. 30 s).

2. **VERIFY - `NodeHost.GetLeaderID` signature.** The periodic leader sweep calls `GetLeaderID(shardID)`. Verify the method exists in dragonboat v4, its exact signature, and whether the returned `term` corresponds to `LeaderEpoch` for use in `AssignPartitionLeaderCmd`.

3. **VERIFY - `StartCluster` join semantics.** When a partition shard already exists in the cluster and this node needs to join it, confirm the correct dragonboat v4 `StartCluster` call: `join=true` with a full `initialMembers` map, or `join=true` with `initialMembers=nil`. Also confirm the proposed heuristic (lowest NodeID in ReplicaNodeIDs starts with `join=false`; others use `join=true`) is correct for new shard creation, or if all nodes should simultaneously call `StartCluster` with `join=false` and full `initialMembers`.

4. **`QueryListAllPartitions` in the FSM.** The `reconcileOnce` procedure requires listing all partitions across all topics. A `QueryListAllPartitions` query type is proposed here; confirm whether it should be added to the Metadata FSM's query set in [03-raft-fsm.md](./03-raft-fsm.md) (immutable) or implemented here as two sequential lookups (QueryListTopics → per-topic QueryGetPartitions).

5. **`AlterTopicRetention` - sentinel values.** The interface uses `retentionMs = -1` for "no change" and `retentionBytes = -1` for "unlimited" (REQUIREMENTS.md §4.1), with `retentionBytes = 0` meaning "no change". This overloads `-1` with two different meanings across the two fields. An alternative is to use proto `optional` fields (oneof or google.protobuf.Int64Value) decided at API design time (Module 3). Flag for resolution there.

6. **Eager reconcile on CreateTopic.** With a 3-second reconcile interval, partition shards can take up to 3 seconds to start after CreateTopic returns, causing an `Unavailable` window for produces. If `eager_reconcile_on_create` is enabled, `CreateTopic` calls `reconcileOnce` synchronously before returning, eliminating this window. Confirm this is the desired behaviour given the added CreateTopic latency (partition shard startup, including dragonboat leader election, can take several RTTs).
