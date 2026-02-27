# Raft Host and FSMs - Detailed Design

BunnyMQ uses dragonboat v4 as its Raft consensus engine. Each broker node runs a single `NodeHost` instance that hosts all Raft shards co-located on that node: one metadata shard (shard ID 0) and one partition shard per partition replica the node owns. The `internal/raft` package wraps dragonboat behind typed helpers so that no other module ever touches the dragonboat API directly. Two FSM types implement the application logic: `MetadataFSM` (`IStateMachine`) manages in-memory cluster topology and consumer group state; `PartitionFSM` (`IOnDiskStateMachine`) wraps `internal/storage` and applies produce commands durably to the partition log. All non-deterministic inputs - timestamps, random values - must arrive in command payloads; the FSMs themselves never call `time.Now()` or `rand`.

See [overview §5.2](./00-overview.md) for rationale behind the dragonboat choice, and [modules §5](./01-modules.md) for the dragonboat integration boundary.

---

## 1. NodeHost Wrapper (`internal/raft`)

### 1.1 Configuration

The `NodeHost` is created with the following configuration:

```go
nhc := dragonboat.NodeHostConfig{
    DeploymentID:   1,                          // fixed; identifies the cluster
    WALDir:         config.DataDir + "/wal",    // dragonboat WAL
    NodeHostDir:    config.DataDir + "/raft",   // Raft log and snapshots
    RTTMillisecond: config.RaftRTTMs,           // default: 200 ms (LAN)
    RaftAddress:    config.RaftAddress,         // "host:port" for Raft RPC
    EnableMetrics:  true,                       // exposes internal dragonboat metrics
}
```

Per-shard `RaftConfig`:

```go
rc := config.Config{
    NodeID:             config.NodeID,
    ClusterID:          shardID,
    ElectionRTT:        10,   // election timeout = 10 × RTTMillisecond = 2 s
    HeartbeatRTT:       1,    // heartbeat = 1 × RTTMillisecond = 200 ms
    CheckQuorum:        true, // leader steps down if quorum unreachable
    MaxInMemLogSize:    32 << 20, // 32 MiB in-memory Raft log cap
    SnapshotEntries:    1 << 62,  // effectively disabled (Strategy A - §4.3)
    CompactionOverhead: 1 << 62,  // keep full Raft log; no compaction
}
```

`SnapshotEntries` is set to a very large value to disable automatic snapshot triggering in v1. This is Strategy A (see §4.3). See Open Question 2 for the exact dragonboat behaviour with large values.

### 1.2 Lifecycle

```text
Start:
  1. NewNodeHost(nhc)
  2. StartCluster(initialMembers, join, metadataFSMFactory, rcMetadata)  - shard 0
  3. For each partition shard on this node: StartCluster(..., partitionFSMFactory, rcPartition)

Graceful stop:
  1. Signal all coordinators to stop accepting requests
  2. NodeHost.Close()  - closes all shards and their FSMs
```

`initialMembers` is `map[uint64]string` mapping NodeID → Raft address, populated from `config.Peers`. When a node joins an existing cluster (`join = true`), `initialMembers` is `nil` and dragonboat fetches shard state from peers.

### 1.3 Public API of the Wrapper

The wrapper exposes typed helpers that hide all dragonboat types. Other modules import only `internal/raft` and use these functions; they never import dragonboat directly.

```go
// Metadata shard helpers
func (h *Host) SyncProposeMetadata(ctx context.Context, cmd MetadataCommand) (sm.Result, error)
func (h *Host) ProposeMetadata(ctx context.Context, cmd MetadataCommand) error
func (h *Host) LookupMetadata(ctx context.Context, q MetadataQuery) (interface{}, error)

// Partition shard helpers
func (h *Host) SyncProposePartition(ctx context.Context, shardID uint64, cmd PartitionCommand) (sm.Result, error)
func (h *Host) ProposePartition(ctx context.Context, shardID uint64, cmd PartitionCommand) error
func (h *Host) LookupPartition(ctx context.Context, shardID uint64, q PartitionQuery) (interface{}, error)

// Shard lifecycle (called by Cluster Coordinator)
func (h *Host) StartPartitionShard(shardID uint64, initialMembers map[uint64]string, join bool) error
func (h *Host) StopPartitionShard(shardID uint64) error
```

Internally, `SyncProposeMetadata` calls `NodeHost.SyncPropose`; `ProposeMetadata` calls `NodeHost.Propose`. Both serialize the `MetadataCommand` to `[]byte` (JSON) before handing to dragonboat. `LookupMetadata` calls `NodeHost.ReadLocalNode` on shard 0.

### 1.4 What the Wrapper Hides

- All `dragonboat.*` types: `NodeHostConfig`, `Config`, `Entry`, `Result`, `ISnapshotFileCollection`, etc.
- Raw `[]byte` serialization and deserialization of commands.
- Shard ID arithmetic (callers pass `shardID uint64` or use the metadata shard implicitly).
- dragonboat's `RequestState` and async propose machinery (the wrapper always blocks until completion or timeout for synchronous calls).

---

## 2. Shard ID Convention

| Shard ID | Role |
|----------|------|
| 0 | Metadata shard - hosts `MetadataFSM` |
| 1 … N | Partition shards - one per partition replica on this node |

**Mapping `(topic, partitionID) → shardID`:** assigned at partition creation time by the Metadata FSM. When `CreateTopic` is applied, the FSM consumes `N` values from its monotonically increasing `nextShardID` counter (one per partition) and stores `shardID` in each `PartitionMeta`. The mapping is therefore a metadata lookup:

```go
// shardID = MetadataFSM.partitions[(topic, partitionID)].shardID
shardID, err := raftHost.LookupMetadata(ctx, MetadataQuery{
    Type:        QueryGetPartition,
    TopicName:   topic,
    PartitionID: partID,
})
```

`nextShardID` starts at 1 and is part of the serialized snapshot, so it survives cluster restarts. The counter never resets; deleted topics' shard IDs are never reused. This ensures that a shard ID cannot be accidentally assigned to two different partitions even after topic deletion and recreation.

---

## 3. Metadata FSM (`IStateMachine`)

The Metadata FSM is the in-memory state machine for shard 0. It is the sole writer of cluster topology (topics, partitions, replica placement, consumer group state). All mutations arrive as committed Raft log entries routed through dragonboat; the FSM never initiates writes to itself.

### 3.1 In-Memory State

```go
type MetadataState struct {
    Topics      map[string]*TopicMeta        // key: topic name
    Partitions  map[PartitionKey]*PartitionMeta
    Nodes       map[uint64]*NodeInfo         // key: node_id
    Groups      map[string]*ConsumerGroupMeta // key: group_id
    NextShardID uint64
}

type PartitionKey struct {
    Topic       string
    PartitionID int32
}

type TopicMeta struct {
    Name              string
    PartitionCount    int32
    ReplicationFactor int32
    RetentionMs       int64
    RetentionBytes    int64
    CreatedAtMs       int64  // set from command payload; never time.Now()
}

type PartitionMeta struct {
    Topic          string
    PartitionID    int32
    ShardID        uint64
    ReplicaNodeIDs []uint64
    LeaderNodeID   uint64
    LeaderEpoch    int64
}

type NodeInfo struct {
    NodeID  uint64
    Address string
}

type ConsumerGroupMeta struct {
    GroupID          string
    GenerationID     int32
    Members          map[string]*MemberInfo  // key: member_id
    CommittedOffsets map[PartitionKey]int64
}

type MemberInfo struct {
    MemberID           string
    ClientHost         string
    SubscribedTopics   []string
    AssignedPartitions []PartitionKey
    LastHeartbeatMs    int64
}
```

`Nodes` is populated from configuration at startup via an initial `RegisterNode` command proposed by each node when it first joins. The node list does not change at runtime (§3.2.3 of [REQUIREMENTS.md](../REQUIREMENTS.md)).

### 3.2 Command Set

Commands are serialized as JSON. The envelope:

```go
type MetadataCommand struct {
    Type                   CommandType                  `json:"type"`
    CreateTopic            *CreateTopicCmd              `json:"ct,omitempty"`
    DeleteTopic            *DeleteTopicCmd              `json:"dt,omitempty"`
    AlterTopicPartCount    *AlterTopicPartCountCmd      `json:"atpc,omitempty"`
    AlterTopicRetention    *AlterTopicRetentionCmd      `json:"atr,omitempty"`
    RegisterNode           *RegisterNodeCmd             `json:"rn,omitempty"`
    AssignPartitionLeader  *AssignPartitionLeaderCmd    `json:"apl,omitempty"`
    JoinConsumerGroup      *JoinConsumerGroupCmd        `json:"jcg,omitempty"`
    LeaveConsumerGroup     *LeaveConsumerGroupCmd       `json:"lcg,omitempty"`
    HeartbeatConsumerGroup *HeartbeatConsumerGroupCmd   `json:"hcg,omitempty"`
    CommitConsumerOffset   *CommitConsumerOffsetCmd     `json:"cco,omitempty"`
    RebalanceConsumerGroup *RebalanceConsumerGroupCmd   `json:"rcg,omitempty"`
}
```

Short JSON field names reduce snapshot size. Only one inner struct is non-nil per command.

#### Command Details

| Command | Key fields | Apply result |
|---------|-----------|--------------|
| `CreateTopic` | name, partition_count, replication_factor, retention_ms, retention_bytes, created_at_ms, replica_node_ids[partition] | Creates TopicMeta + N PartitionMeta entries with assigned ShardIDs; increments NextShardID by N. Returns AlreadyExists in sm.Result if topic exists. |
| `DeleteTopic` | name | Removes TopicMeta and all PartitionMeta entries. No-op if absent. Physical shard teardown is a side-effect handled by Cluster Coordinator after the command commits, not inside Update(). |
| `AlterTopicPartCount` | name, new_partition_count, new_replica_assignments | Appends new PartitionMeta entries; increments NextShardID. Validates new_count > current. |
| `AlterTopicRetention` | name, retention_ms, retention_bytes | Updates TopicMeta.RetentionMs and RetentionBytes. |
| `RegisterNode` | node_id, address | Adds NodeInfo to Nodes map. Idempotent. |
| `AssignPartitionLeader` | topic, partition_id, leader_node_id, leader_epoch | Updates PartitionMeta.LeaderNodeID and LeaderEpoch. Validates epoch > current. |
| `JoinConsumerGroup` | group_id, member_id (empty = server-assigned), client_host, subscribed_topics, joined_at_ms | Adds/updates MemberInfo. Triggers range-based rebalance (see §3.4). Bumps GenerationID. Returns assigned member_id and partition assignment in sm.Result. |
| `LeaveConsumerGroup` | group_id, member_id | Removes MemberInfo. Triggers rebalance. Bumps GenerationID. |
| `HeartbeatConsumerGroup` | group_id, member_id, generation_id, timestamp_ms | Updates MemberInfo.LastHeartbeatMs. Returns "rebalance needed" in sm.Result if generation_id != group.GenerationID. |
| `CommitConsumerOffset` | group_id, offsets [{topic, partition_id, offset}] | Updates CommittedOffsets map. |
| `RebalanceConsumerGroup` | group_id, expired_member_ids, timestamp_ms | Removes expired members, recomputes assignment, bumps GenerationID. Proposed by the Group Coordinator's heartbeat checker (out of scope for this session). |

**Error encoding in sm.Result:** `sm.Result.Value` carries an error code (0 = success). A non-zero error code signals the caller to inspect `sm.Result.Data` for a JSON-encoded error string. `Update()` never returns an error to dragonboat; errors are always expressed through `sm.Result`.

### 3.3 Range-Based Rebalance Algorithm

Executed inside `Update()` for `JoinConsumerGroup`, `LeaveConsumerGroup`, and `RebalanceConsumerGroup`. All inputs come from the command payload or current FSM state - no external I/O.

```go
func rebalance(group *ConsumerGroupMeta, topics map[string]*TopicMeta, partitions map[PartitionKey]*PartitionMeta) {
    // 1. Collect all partitions subscribed by any member (sorted for determinism)
    eligible := collectEligiblePartitions(group.Members, topics, partitions)
    sort.Slice(eligible, func(i, j int) bool {
        return eligible[i].less(eligible[j]) // topic asc, partition_id asc
    })

    // 2. Collect active members (sorted for determinism)
    members := sortedMemberIDs(group.Members)

    // 3. Range-split: ceil(len(eligible) / len(members)) per member
    //    earlier members get the remainder partition
    for i, memberID := range members {
        lo := i * len(eligible) / len(members)
        hi := (i + 1) * len(eligible) / len(members)
        group.Members[memberID].AssignedPartitions = eligible[lo:hi]
    }
}
```

This algorithm is fully deterministic given the sorted inputs. It is identical to [REQUIREMENTS.md §3.5.6](../REQUIREMENTS.md).

### 3.4 Lookup Queries

dragonboat calls `IStateMachine.Lookup(query interface{}) (interface{}, error)`. BunnyMQ uses a typed query struct:

```go
type MetadataQuery struct {
    Type        QueryType
    TopicName   string
    PartitionID int32
    GroupID     string
    PartKey     PartitionKey
}
```

| Query type | Returns | Use |
|------------|---------|-----|
| `QueryGetTopic` | `*TopicMeta, error` | DescribeTopic RPC |
| `QueryListTopics` | `[]*TopicMeta` | ListTopics RPC |
| `QueryGetPartition` | `*PartitionMeta, error` | Data Coordinator leader lookup |
| `QueryGetPartitions` | `[]*PartitionMeta, error` | ListPartitions for a topic |
| `QueryGetNode` | `*NodeInfo, error` | DescribeCluster |
| `QueryListNodes` | `[]*NodeInfo` | DescribeCluster |
| `QueryGetGroup` | `*ConsumerGroupMeta, error` | Group Coordinator |
| `QueryGetCommittedOffset` | `int64, error` | FetchCommittedOffsets |

`Lookup` is read-only: it does not mutate FSM state and may be called concurrently with other `Lookup` calls. dragonboat does NOT call `Lookup` and `Update` concurrently on the same `IStateMachine` instance (unlike `IOnDiskStateMachine`), so no locking is needed inside the FSM itself.

### 3.5 Snapshot Strategy

**SaveSnapshot:**

```go
func (fsm *MetadataFSM) SaveSnapshot(w io.Writer, _ sm.ISnapshotFileCollection, done <-chan struct{}) error {
    return json.NewEncoder(w).Encode(fsm.state)
}
```

**RecoverFromSnapshot:**

```go
func (fsm *MetadataFSM) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile, done <-chan struct{}) error {
    fsm.state = &MetadataState{}
    return json.NewDecoder(r).Decode(fsm.state)
}
```

**Size estimate:** At maximum supported scale (1 000 topics × 100 partitions = 100 000 `PartitionMeta` entries, 100 consumer groups), JSON snapshot size ≈ 20–50 MiB. At course-project scale (tens of topics, few partitions each, handful of groups), ≈ 100–500 KiB. JSON serialization at 50 MiB takes ≈ 100 ms on a modern CPU - acceptable for a background snapshot operation.

**Snapshot frequency:** dragonboat's `SnapshotEntries` for the metadata shard is set to a moderate value (e.g., 10 000 entries) so that metadata shard restarts don't replay thousands of old commands. This is different from the partition shard where snapshots are effectively disabled (Strategy A). Exact value is Open Question 3.

### 3.6 Determinism Rules

1. **No `time.Now()`** inside `Update()`. All timestamps (e.g., `created_at_ms`, `joined_at_ms`, `timestamp_ms` in heartbeats) must be included in the command payload by the proposer (Cluster Coordinator, Group Coordinator).
2. **Deterministic map iteration.** Any operation that depends on ordering (rebalance assignment, snapshot serialization) must sort keys explicitly. Go's map iteration is randomized and must never drive application logic.
3. **Error codes in sm.Result, not error returns.** Returning a non-nil `error` from `Update()` causes dragonboat to halt the shard. Logical errors (topic already exists, invalid partition count) are returned as non-zero `sm.Result.Value`.
4. **No goroutine spawning inside Update().** No concurrent access to FSM state during `Update()`.
5. **Idempotent commands.** `CreateTopic` returns `AlreadyExists` if the topic exists; it does not modify state. `RegisterNode` is idempotent. These properties ensure that re-proposed commands (due to timeout retries at the coordinator level) produce deterministic, safe outcomes.

---

## 4. Partition FSM (`IOnDiskStateMachine`)

The Partition FSM is the on-disk state machine for a partition shard. It is a thin adapter: its state IS the `Storage` instance. All disk I/O goes through `internal/storage`. The FSM enforces the determinism contract and handles the durability bookkeeping required by dragonboat.

### 4.1 State

```go
type PartitionFSM struct {
    storage          storage.Storage  // the partition's segmented log
    lastAppliedIndex atomic.Uint64    // updated after each successful Update()
    sidecarPath      string           // path to applied.idx
}
```

No other state. Raft replication of the partition's batches arrives via `Update()` and is immediately written to Storage.

### 4.2 Partition Command Wire Format

Partition commands are serialized with a 1-byte type prefix followed by a type-specific payload:

```text
[0]      : uint8   command type
[1 ...]  : bytes   payload
```

| Type byte | Name | Payload |
|-----------|------|---------|
| `0x01` | `AppendBatch` | Raw batch bytes (as produced; `base_offset` field is ignored - Storage overwrites it) |
| `0x02` | `RetentionConfig` | JSON: `{"retention_ms": int64, "retention_bytes": int64}` |

Using a raw prefix byte avoids JSON overhead for the `AppendBatch` case, where the payload can be up to 4 MiB.

### 4.3 Open() - Partition Recovery

`Open(stopc <-chan struct{}) (uint64, error)` is called by dragonboat before any `Update()` or `Lookup()`. It returns the last applied Raft log index so dragonboat knows where to resume.

**Sidecar file (`applied.idx`) format:** 16 bytes, big-endian.

```text
[0:8]   uint64  last_applied_raft_index
[8:16]  int64   corresponding_storage_latest_offset
```

**Open() procedure:**

```go
func (fsm *PartitionFSM) Open(stopc <-chan struct{}) (uint64, error) {
    // 1. Open Storage (crash-recovery scan, index rebuild)
    if err := fsm.storage.Open(fsm.dir); err != nil {
        return 0, err
    }

    // 2. Read sidecar
    sidecar, err := readSidecar(fsm.sidecarPath)
    if os.IsNotExist(err) {
        return 0, nil  // fresh partition; dragonboat applies from index 1
    }
    if err != nil {
        return 0, err
    }

    // 3. Reconcile: if storage has more data than sidecar recorded,
    //    truncate the excess (re-applied by dragonboat)
    if fsm.storage.LatestOffset() > sidecar.LatestOffset {
        if err := fsm.storage.TruncateTo(sidecar.LatestOffset); err != nil {
            return 0, err
        }
    }

    fsm.lastAppliedIndex.Store(sidecar.LastAppliedIndex)
    return sidecar.LastAppliedIndex, nil
}
```

**Reconciliation rationale:** If a crash occurs after Storage.Append succeeds but before the sidecar is atomically renamed, `storage.LatestOffset()` is ahead of `sidecar.LatestOffset`. `TruncateTo` undoes those writes. dragonboat will re-apply the corresponding Raft entries. Since Storage.Append has already been truncated, the re-application produces the correct data with no duplication.

### 4.4 Update()

dragonboat calls `Update(entries []sm.Entry) ([]sm.Entry, error)` with a batch of committed entries.

```go
func (fsm *PartitionFSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
    for i := range entries {
        e := &entries[i]
        cmdType := e.Cmd[0]

        switch cmdType {
        case CmdAppendBatch:
            batch := e.Cmd[1:]
            baseOffset, err := fsm.storage.Append(batch)
            if err != nil {
                panic(fmt.Sprintf("storage.Append failed: %v", err))
            }
            e.Result = sm.Result{Value: uint64(baseOffset)}

        case CmdRetentionConfig:
            var rc RetentionConfigPayload
            if err := json.Unmarshal(e.Cmd[1:], &rc); err != nil {
                panic(fmt.Sprintf("bad retention config: %v", err))
            }
            fsm.storage.SetRetentionConfig(rc.RetentionMs, rc.RetentionBytes)
            e.Result = sm.Result{Value: 0}
        }
    }

    // Persist durably: fsync log + atomic sidecar update
    if err := fsm.persistApplied(entries[len(entries)-1].Index); err != nil {
        panic(fmt.Sprintf("persistApplied failed: %v", err))
    }

    fsm.lastAppliedIndex.Store(entries[len(entries)-1].Index)
    return entries, nil
}
```

**persistApplied:**

```go
func (fsm *PartitionFSM) persistApplied(index uint64) error {
    // 1. fsync active log (ensures batch bytes are durable)
    if err := fsm.storage.Sync(); err != nil {
        return err
    }
    // 2. Write sidecar atomically
    data := encodeSidecar(index, fsm.storage.LatestOffset())
    tmp := fsm.sidecarPath + ".tmp"
    if err := os.WriteFile(tmp, data, 0o644); err != nil {
        return err
    }
    if err := syncFile(tmp); err != nil {
        return err
    }
    return os.Rename(tmp, fsm.sidecarPath) // atomic on Linux
}
```

Panic on failure: see [§4.7 Failure Modes](#47-failure-modes).

### 4.5 Lookup()

`Lookup(query interface{}) (interface{}, error)` serves read requests from the Data Coordinator without a Raft round-trip:

```go
type PartitionQuery struct {
    Type        PartitionQueryType
    Offset      int64
    TimestampMs int64
    MaxBytes    int
}
```

| Query type | Delegates to |
|------------|-------------|
| `QueryRead` | `storage.Read(offset, maxBytes)` |
| `QueryReadByTime` | `storage.ReadByTime(timestampMs, maxBytes)` |
| `QueryEarliestOffset` | `storage.EarliestOffset()` |
| `QueryLatestOffset` | `storage.LatestOffset()` |

Reads are bounded by `storage.LatestOffset()`, which reflects only successfully committed and applied batches. No additional bounding against `lastAppliedIndex` is needed: every byte in Storage was written by a successful `Update()` call, and `Update()` is called only for committed Raft entries.

`Lookup` may be called concurrently with `Update` by dragonboat. Storage's concurrency model (§8 of [02-storage.md](./02-storage.md)) handles this safely.

### 4.6 Snapshot Strategy - Strategy A (v1)

**Rationale:** Strategy A (no-op snapshots) is chosen for v1 because it eliminates snapshot implementation complexity while remaining correct for course-project workloads. The trade-off is an ever-growing Raft log and slower fresh-node bootstrap (full log replay). Strategy B (segment manifest snapshot) is the production path.

**SaveSnapshot** (no-op):

```go
func (fsm *PartitionFSM) SaveSnapshot(_ interface{}, w io.Writer, _ <-chan struct{}) error {
    _, err := w.Write([]byte("strategy-a-noop"))
    return err
}
```

**RecoverFromSnapshot** (no-op):

```go
func (fsm *PartitionFSM) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
    io.Copy(io.Discard, r) // consume the marker
    return nil
}
```

**PrepareSnapshot** (no-op):

```go
func (fsm *PartitionFSM) PrepareSnapshot() (interface{}, error) {
    return nil, nil
}
```

**Sync:**

```go
func (fsm *PartitionFSM) Sync() error {
    return nil // durability handled in Update() via persistApplied
}
```

**Strategy B (production path - appendix):** When dragonboat requests a snapshot, the FSM writes a manifest listing the sealed segment file paths and the last applied index, then transfers the segment files via `ISnapshotFileCollection.AddFile`. On recovery, the manifest is read and the files are copied into the partition directory, followed by `Storage.Open`. This bounds Raft log replay to entries after the snapshot index and enables efficient fresh-node replication. Strategy B is documented here as future work; it is NOT implemented in v1.

### 4.7 Failure Modes

| Failure | Cause | Action |
|---------|-------|--------|
| `Storage.Append` I/O error | Disk full, kernel error | `panic`. Supervisor restarts node; Raft replays the entry from `lastAppliedIndex + 1`. |
| `persistApplied` fsync failure | Disk full or hardware fault | `panic`. Same recovery path. |
| `Lookup` I/O error on sealed segment | Corrupt or missing sealed file | Return error from `Lookup`. Data Coordinator propagates a retriable error to the client. |
| Bad command type byte | Bug in command encoder | `panic`. Indicates a programming error; all replicas will fail identically. |
| `RecoverFromSnapshot` called (Strategy A) | Should not occur if `SnapshotEntries` is large enough | Log `error`, return nil (no-op recovery). |

**Panic is correct here:** dragonboat's contract for `IOnDiskStateMachine.Update()` is that it either applies all entries and returns nil, or the node must restart. Returning a non-nil error from `Update()` halts the shard with no recovery. Panicking triggers the supervisor, which restarts the process; dragonboat then replays from `lastAppliedIndex + 1`.

---

## 5. Determinism Contract

The following rules apply to ALL code that runs inside `Update()` or `Lookup()` of either FSM:

| Rule | Anti-pattern | Correct approach |
|------|-------------|-----------------|
| No wall clock | `time.Now()` inside Update | Include timestamps in command payload |
| No `rand` without seed | `rand.Int()` inside Update | Include seed or pre-computed random value in payload |
| No network I/O | HTTP/gRPC calls inside Update | All inputs come from the committed entry |
| No goroutines | `go func()` inside Update | Spawn goroutines from coordinator, not FSM |
| Deterministic iteration | `for k, v := range map` affecting output order | Sort keys before range |
| Errors in Result, not return | `return err` from Update | `e.Result = errorResult(err); continue` |
| Single-source timestamps | `created_at_ms: time.Now()` in FSM | Proposer (coordinator) includes timestamp in command |

These rules ensure that all replicas apply the same sequence of mutations and arrive at identical state, regardless of the order in which dragonboat calls `Update()` across nodes (which is always the same, but must not be assumed to carry any implicit timing information).

---

## 6. Open Questions

1. **`SnapshotEntries` for partition shards:** `1 << 62` is used to effectively disable snapshots. Verify that dragonboat v4 accepts this value and does not treat it as an overflow or configuration error. If not, determine the actual maximum accepted value or a supported mechanism to disable automatic snapshots.

2. **`SnapshotEntries` for metadata shard:** The metadata shard benefits from periodic snapshots (bounded replay on restart). A value of 10 000 entries is proposed. Confirm or propose an alternative. This is a configuration knob, not a v1 correctness concern.

3. **`Sync()` vs `persistApplied` in `Update()`:** The current design calls `persistApplied` (fsync + sidecar) at the end of every `Update()` batch. dragonboat also provides a `Sync()` hook that it calls periodically. Should we defer the fsync to `Sync()` to reduce write amplification when dragonboat delivers many small batches? Trade-off: deferred fsync means a crash between `Update()` and `Sync()` loses progress, requiring re-apply; immediate fsync in `Update()` adds latency but ensures the sidecar is always consistent.

4. ~~**`Storage.Sync()` method:**~~ Resolved - `Sync() error` and `TruncateTo(offset int64) error` have been added to the `Storage` interface in [02-storage.md](./02-storage.md).
