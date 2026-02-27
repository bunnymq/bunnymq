# Data Coordinator — Detailed Design

The Data Coordinator is the data-plane routing layer on each broker node. It receives produce and fetch requests from the Data API, looks up the partition leader from the Metadata FSM, and either handles the request locally (if this node is the current leader) or returns a `NotLeader` error so the client can retry against the correct node. For produce, it dispatches `SyncProposePartition` (acks=all) or `ProposePartition` (acks=0) through the Raft host. For fetch, it calls `LookupPartition` against the local Partition FSM — no Raft round-trip — and implements long-polling via the storage `newDataCh` notification channel. The Data Coordinator does not forward requests to other nodes in v1; client-side retry is the routing mechanism.

See [01-modules.md §5](./01-modules.md) for the fetch long-polling design, [02-storage.md §8](./02-storage.md) for storage concurrency, [03-raft-fsm.md §4](./03-raft-fsm.md) for the Partition FSM interface, and [04-cluster-coordinator.md](./04-cluster-coordinator.md) for the shard lifecycle that feeds this module's registry.

---

## 1. Responsibilities

### What the Data Coordinator does

- **Produce routing.** Look up the partition leader; issue `SyncProposePartition` (acks=all) or `ProposePartition` (acks=0) if this node is the leader; return `NotLeader` otherwise.
- **Fetch routing.** Look up the partition leader; call `LookupPartition` for a zero-round-trip read from local storage; implement long-polling when no records are currently available.
- **Offset queries.** `GetEarliestOffset`, `GetLatestOffset`, `GetOffsetByTimestamp` — all via `LookupPartition` from the local leader.
- **Shard registry.** Maintain a local map of partition shards available for routing on this node, updated by the Cluster Coordinator via `StartPartitionReplica` and `StopPartitionReplica`.

### What the Data Coordinator does NOT do

- **Does not manage partition shard lifecycle.** `StartPartitionShard` / `StopPartitionShard` dragonboat calls are the Cluster Coordinator's responsibility ([04-cluster-coordinator.md §7](./04-cluster-coordinator.md)). The Data Coordinator is notified after the fact via `StartPartitionReplica` / `StopPartitionReplica`.
- **Does not forward requests within the cluster.** If this node is not the partition leader, it returns `NotLeader`; the client retries directly against the leader. See §4.2 for rationale.
- **Does not manage consumer group state.** Group Coordinator's responsibility ([08-consumer-groups.md](./08-consumer-groups.md)).
- **Does not drive retention enforcement.** Retention runs as a background goroutine inside `internal/storage` ([02-storage.md §7](./02-storage.md)).

---

## 2. Public Interface

```go
// DataCoordinator routes produce and fetch requests to the correct partition
// Raft shard on this node. All Produce/Fetch/Get* methods are safe to call
// concurrently from multiple gRPC handler goroutines.
type DataCoordinator struct { /* unexported */ }

// Produce appends batch to the partition identified by (topic, partitionID).
// batch must be a fully-encoded batch in the on-disk/on-wire format
// (REQUIREMENTS.md §4.4); the base_offset field is ignored — Storage overwrites
// it with the server-assigned offset.
//
// acks=AcksAll  — blocks until quorum commit; returns the assigned base_offset.
// acks=AcksZero — fires and forgets; returns offset -1 (REQUIREMENTS.md §3.6.1).
//
// Returns NotLeader if this node is not the current leader, including the
// leader's node ID and address so the caller can retry. Returns TopicNotFound
// or PartitionNotFound if metadata is absent. Returns Unavailable if the
// metadata shard has no leader.
func (dc *DataCoordinator) Produce(
    ctx context.Context,
    topic string,
    partitionID int32,
    batch []byte,
    acks AcksMode,
) (offset int64, err error)

// Fetch returns up to maxBytes of serialised batch data starting at the batch
// whose range contains offset. If no records are available and maxWaitMs > 0,
// the call blocks until records arrive, maxWaitMs elapses, a leader change is
// detected, or ctx is cancelled. Returns (nil, 0, nil) on timeout with no records.
// Returns NotLeader if this node is not the current leader.
// Returns OffsetOutOfRange if offset < EarliestOffset (deleted by retention).
func (dc *DataCoordinator) Fetch(
    ctx context.Context,
    topic string,
    partitionID int32,
    offset int64,
    maxBytes int,
    maxWaitMs int64,
) (records []byte, nextOffset int64, err error)

// GetEarliestOffset returns the base_offset of the oldest available batch.
// Returns 0 before any data is written. Returns NotLeader if not the leader.
func (dc *DataCoordinator) GetEarliestOffset(
    ctx context.Context, topic string, partitionID int32,
) (int64, error)

// GetLatestOffset returns the next offset to be assigned (one past the last
// written batch's final record). Returns 0 before any data is written.
// Returns NotLeader if not the leader.
func (dc *DataCoordinator) GetLatestOffset(
    ctx context.Context, topic string, partitionID int32,
) (int64, error)

// GetOffsetByTimestamp returns the base_offset of the first batch whose
// max_timestamp >= timestampMs. Returns OffsetNotFound if no such batch exists.
// Returns NotLeader if not the leader.
func (dc *DataCoordinator) GetOffsetByTimestamp(
    ctx context.Context, topic string, partitionID int32, timestampMs int64,
) (int64, error)

// StartPartitionReplica registers a partition shard as available for routing.
// Called by the Cluster Coordinator after raftHost.StartPartitionShard succeeds.
// Idempotent: calling twice for the same partition is a no-op.
func (dc *DataCoordinator) StartPartitionReplica(topic string, partitionID int32, shardID uint64)

// StopPartitionReplica removes a partition shard from the routing registry.
// Called by the Cluster Coordinator before raftHost.StopPartitionShard.
// In-flight requests against this partition receive Unavailable or NotLeader
// on their next metadata lookup after the shard stops.
func (dc *DataCoordinator) StopPartitionReplica(topic string, partitionID int32, shardID uint64)
```

### Supporting types

```go
type AcksMode int8

const (
    AcksAll  AcksMode = -1 // SyncPropose; returns assigned offset
    AcksZero AcksMode = 0  // Propose (async); returns -1
)

type NotLeaderError struct {
    LeaderNodeID  uint64
    LeaderAddress string // "host:port" of the Data API on the leader node
}

func (e *NotLeaderError) Error() string { /* ... */ }
```

---

## 3. Shard Registry and Partition Discovery

The Data Coordinator maintains a local shard registry (`shardRegistry`) keyed by `(topic, partitionID)`. This registry is populated exclusively via calls from the Cluster Coordinator:

```go
type partitionKey struct {
    Topic       string
    PartitionID int32
}

type shardEntry struct {
    ShardID     uint64
    Topic       string
    PartitionID int32
}

// Protected by registryMu.
shardRegistry map[partitionKey]shardEntry
```

The Cluster Coordinator's reconciliation goroutine calls `dc.StartPartitionReplica(topic, partitionID, shardID)` immediately after `raftHost.StartPartitionShard` returns successfully, and `dc.StopPartitionReplica(topic, partitionID, shardID)` before `raftHost.StopPartitionShard`. No polling of the Metadata FSM is performed by the Data Coordinator itself; the Cluster Coordinator is the sole driver of registry changes.

> **Cross-module dependency.** [04-cluster-coordinator.md §7.4](./04-cluster-coordinator.md) (startShard / stopShard) must include calls to `dc.StartPartitionReplica` and `dc.StopPartitionReplica`. This was not explicit in Module 1's written design; it is confirmed as required here and listed in Open Question 1.

---

## 4. Routing Logic

### 4.1 Leader check

Every produce and fetch request performs a leader check before any Raft call:

```go
func (dc *DataCoordinator) leaderCheck(
    ctx context.Context, topic string, partitionID int32,
) (shardID uint64, err error) {
    // 1. Fetch partition metadata from local Metadata FSM (no Raft round-trip).
    result, err := dc.raftHost.LookupMetadata(ctx, MetadataQuery{
        Type: QueryGetPartition, TopicName: topic, PartitionID: partitionID,
    })
    if err != nil {
        return 0, ErrUnavailable
    }
    if result == nil {
        return 0, ErrPartitionNotFound
    }
    pm := result.(*PartitionMeta)

    // 2. Check leadership.
    if pm.LeaderNodeID != dc.config.NodeID {
        addr := dc.nodeAddress(ctx, pm.LeaderNodeID) // cached lookup
        return 0, &NotLeaderError{LeaderNodeID: pm.LeaderNodeID, LeaderAddress: addr}
    }

    // 3. Confirm shard is locally registered (may not be yet if reconcile hasn't run).
    dc.registryMu.RLock()
    entry, ok := dc.shardRegistry[partitionKey{topic, partitionID}]
    dc.registryMu.RUnlock()
    if !ok {
        return 0, ErrUnavailable
    }
    return entry.ShardID, nil
}
```

`LookupMetadata` calls dragonboat's `ReadLocalNode` on the metadata shard — no Raft round-trip. The `LeaderNodeID` it reads reflects the most recent `AssignPartitionLeader` command committed to the metadata shard by the Cluster Coordinator's leader sweep. The maximum staleness is `leader_check_interval_ms` (default 3 s). Clients that hit a stale `NotLeader` response simply retry and receive a fresh leader reference.

`nodeAddress` caches `(nodeID → Data API address)` with a short TTL (default 5 s) to avoid a Metadata FSM lookup on every non-leader request.

### 4.2 No in-cluster forwarding (v1 decision)

The Data Coordinator returns `NotLeader` rather than forwarding the request to the actual leader.

**Rationale:** In-cluster forwarding adds one network hop per non-leader-routed request, requires the forwarding node to maintain a gRPC connection pool to every other broker, and doubles the request's latency for the common case after a leader change. Client-side retry with metadata cache refresh achieves the same result: after the first `NotLeader`, the client updates its leader cache and all subsequent requests go directly to the correct node. The per-request forwarding infrastructure cost is not justified given the client library's ability to cache leader addresses (see [07-client-library.md](./07-client-library.md)).

---

## 5. Produce Flow

### 5.1 acks=all

```go
shardID, err := dc.leaderCheck(ctx, topic, partitionID)
if err != nil {
    return -1, err
}

result, err := dc.raftHost.SyncProposePartition(ctx, shardID, PartitionCommand{
    Type:    CmdAppendBatch,
    Payload: batch, // Storage overwrites batch[0:8] with base_offset
})
if err != nil {
    return -1, mapRaftError(err)
}

// sm.Result.Value carries the base_offset returned by Storage.Append.
return int64(result.Value), nil
```

`SyncProposePartition` blocks until the Raft entry is replicated to a quorum of partition shard replicas and the leader's `PartitionFSM.Update()` has called `Storage.Append(batch)`. `Storage.Append` assigns the `base_offset`, writes the batch to the active log segment, optionally writes index entries, closes `newDataCh`, and returns the `base_offset`. This value propagates back via `sm.Result.Value`.

For a batch with `record_count = N`, the returned `base_offset` is the offset of the first record; subsequent records occupy `[base_offset + 1, base_offset + N − 1]`.

### 5.2 acks=0

```go
shardID, err := dc.leaderCheck(ctx, topic, partitionID)
if err != nil {
    return -1, err
}

if err := dc.raftHost.ProposePartition(ctx, shardID, PartitionCommand{
    Type:    CmdAppendBatch,
    Payload: batch,
}); err != nil {
    return -1, mapRaftError(err)
}

return -1, nil // no offset assigned — REQUIREMENTS.md §3.6.1
```

`ProposePartition` enqueues the entry in dragonboat's propose pipeline and returns immediately without waiting for replication or FSM application. The client receives `offset = -1`. If the leader crashes before the entry achieves quorum, the batch is silently lost — this is the defined semantics of `acks=0`.

---

## 6. Fetch Flow

### 6.1 Immediate fetch (data available)

```go
shardID, err := dc.leaderCheck(ctx, topic, partitionID)
if err != nil {
    return nil, 0, err
}

result, err := dc.raftHost.LookupPartition(ctx, shardID, PartitionQuery{
    Type: QueryRead, Offset: offset, MaxBytes: maxBytes,
})
if err != nil {
    return nil, 0, mapLookupError(err)
}
r := result.(*ReadResult)
return r.Records, r.NextOffset, nil
```

`LookupPartition` calls `PartitionFSM.Lookup` → `storage.Read(offset, maxBytes)`. Storage binary-searches the offset index, scans the log forward, and returns complete batches up to `maxBytes`. The result is bounded by `storage.LatestOffset()`, which reflects only committed and applied entries. No Raft round-trip.

### 6.2 Long-poll fetch (maxWaitMs > 0, no data available)

When `storage.Read` returns empty, the goroutine parks until new data arrives or the deadline fires. The channel snapshot must be taken **before** the read to eliminate a race where data arrives between the failed read and the channel snapshot:

```go
func (dc *DataCoordinator) fetchWithLongPoll(
    ctx context.Context,
    topic string, partitionID int32,
    shardID uint64,
    offset int64, maxBytes int,
    maxWaitMs int64,
) ([]byte, int64, error) {
    deadline := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)

    for {
        remaining := time.Until(deadline)
        if remaining <= 0 {
            return nil, 0, nil // timeout, return empty
        }

        // Snapshot newDataCh BEFORE reading.
        // If data arrives between this snapshot and the Read below, the channel
        // we hold will already be closed, waking the select on the next iteration.
        chResult, err := dc.raftHost.LookupPartition(ctx, shardID, PartitionQuery{
            Type: QueryGetNewDataCh,
        })
        if err != nil {
            return nil, 0, err
        }
        ch := chResult.(<-chan struct{})

        // Re-verify leadership on each iteration — leader may change during a long poll.
        pm, err := dc.raftHost.LookupMetadata(ctx, MetadataQuery{
            Type: QueryGetPartition, TopicName: topic, PartitionID: partitionID,
        })
        if err != nil || pm == nil {
            return nil, 0, ErrUnavailable
        }
        if pm.(*PartitionMeta).LeaderNodeID != dc.config.NodeID {
            addr := dc.nodeAddress(ctx, pm.(*PartitionMeta).LeaderNodeID)
            return nil, 0, &NotLeaderError{
                LeaderNodeID:  pm.(*PartitionMeta).LeaderNodeID,
                LeaderAddress: addr,
            }
        }

        // Read data.
        readResult, err := dc.raftHost.LookupPartition(ctx, shardID, PartitionQuery{
            Type: QueryRead, Offset: offset, MaxBytes: maxBytes,
        })
        if err != nil {
            return nil, 0, mapLookupError(err)
        }
        r := readResult.(*ReadResult)
        if len(r.Records) > 0 {
            return r.Records, r.NextOffset, nil
        }

        // No data yet — wait.
        select {
        case <-ch:
            // newDataCh closed by Storage.Append on a new record. Loop again.
            continue
        case <-time.After(remaining):
            return nil, 0, nil
        case <-ctx.Done():
            return nil, 0, ctx.Err()
        }
    }
}
```

**Race elimination:** If a batch is appended between the `LookupPartition(QueryGetNewDataCh)` call and the `LookupPartition(QueryRead)` call, `Storage.Append` closed the channel we hold. The subsequent `Read` finds the new data and returns immediately. If the `Read` still returns empty (offset not yet in this batch's range), the select on the already-closed channel exits immediately and loops again.

VERIFY: `QueryGetNewDataCh` requires a new `PartitionQueryType` value in [03-raft-fsm.md §4.5](./03-raft-fsm.md) (immutable). The FSM's `Lookup` implementation delegates to `storage.NewDataCh()` and returns the channel as `interface{}`. VERIFY that dragonboat v4's `IOnDiskStateMachine.Lookup()` threading model permits returning a `<-chan struct{}` via `interface{}`. `storage.NewDataCh()` holds `chanMu` while returning the channel reference, so the return is safe under concurrent `Append` calls. See Open Question 2.

---

## 7. Offset Queries

All three offset query methods follow the same leader-check → LookupPartition pattern. No long-polling.

| Method | `PartitionQuery.Type` | Storage delegation |
|---|---|---|
| `GetEarliestOffset` | `QueryEarliestOffset` | `storage.EarliestOffset()` |
| `GetLatestOffset` | `QueryLatestOffset` | `storage.LatestOffset()` |
| `GetOffsetByTimestamp` | `QueryReadByTime` | `storage.ReadByTime(timestampMs, smallMaxBytes)` |

For `GetOffsetByTimestamp`: `ReadByTime` returns `(records, nextOffset, err)`. The base_offset of the first batch in `records` is the answer (decoded from the batch header's `base_offset` field). Return `ErrOffsetNotFound` when `ReadByTime` returns `ErrTimestampNotFound`. See Open Question 4 for the precise return semantics.

---

## 8. Retention Enforcement

Storage manages its own retention lifecycle internally. `Storage.Open()` starts a background goroutine that ticks every `retention_check_interval_ms` and calls `storage.EnforceRetention(retentionMs, retentionBytes)` directly ([02-storage.md §7](./02-storage.md)). Retention configuration is updated at partition creation and propagated via the `RetentionConfig` Partition FSM command issued by `ClusterCoordinator.AlterTopicRetention` ([04-cluster-coordinator.md §3.6](./04-cluster-coordinator.md)).

**Retention runs independently on each replica.** All replicas hold identical data (Raft guarantees identical Apply sequences), so they delete the same segments within one `retention_check_interval_ms` window of each other. Minor temporary divergence between replicas is acceptable: a consumer that hits `OffsetOutOfRange` from one replica will encounter the same error on others shortly after.

> **Alternative: leader-driven Raft-commanded retention.** For strictly deterministic cross-replica deletion, the leader could call `EnforceRetention`, translate the result into a `DeleteSegmentsBefore(earliestValidOffset int64)` Partition FSM command, and have followers apply it at the same Raft index. This requires a new command type (`0x03`) in [03-raft-fsm.md §4.2](./03-raft-fsm.md) and a `DeleteSegmentsBefore(offset int64) error` Storage method — both in immutable documents. The decision is deferred to Open Question 3.

---

## 9. Concurrency Model

| Goroutine | Operations | Notes |
|---|---|---|
| gRPC handler (N concurrent) | Produce, Fetch, GetOffset*, leaderCheck | Each is independent; no shared mutable state beyond registryMu |
| Long-poll fetch goroutine | LookupPartition, select on newDataCh, leaderCheck | One goroutine per in-flight long-poll; lifespan ≤ maxWaitMs |
| Cluster Coordinator reconcile | StartPartitionReplica, StopPartitionReplica | Holds registryMu.Lock briefly |

**Locking:**

| Lock | Protects | Writers | Readers |
|---|---|---|---|
| `registryMu sync.RWMutex` | `shardRegistry map` | CC reconcile goroutine | gRPC handler goroutines (leaderCheck) |

No coordination is needed between concurrent produce and fetch goroutines. dragonboat serialises `PartitionFSM.Update()` calls; `Lookup()` may run concurrently with `Update()`. Storage's concurrency model ([02-storage.md §8](./02-storage.md)) handles the underlying file I/O safety.

Long-poll goroutines exit on `ctx.Done()` (client disconnect) with no resource leak. There is no goroutine pool; each gRPC handler runs its own select loop.

VERIFY: confirm that dragonboat's `IOnDiskStateMachine` allows `Lookup()` and `Update()` to execute concurrently on the same FSM instance. [03-raft-fsm.md §4.5](./03-raft-fsm.md) states "Lookup may be called concurrently with Update by dragonboat" and that Storage's concurrency model handles it safely. Mark as verified pending dragonboat v4 API documentation review.

---

## 10. Failure Modes

| Situation | Behavior |
|---|---|
| This node is not the partition leader | `leaderCheck` returns `NotLeader{leaderNodeID, leaderAddress}`. gRPC layer returns `FAILED_PRECONDITION`. Client retries against the leader. |
| Metadata shard has no leader | `LookupMetadata` returns error. Coordinator returns `Unavailable`. |
| Shard not in local registry | `leaderCheck` returns `Unavailable`. Occurs briefly after topic creation before the reconcile goroutine runs. Producer retries after a short back-off. |
| `SyncProposePartition` timeout | dragonboat returns deadline error. Coordinator returns `Timeout`. Batch not committed; client may safely retry (not yet applied, so no duplicate). |
| `SyncProposePartition` — no quorum | dragonboat returns error. Coordinator returns `Unavailable`. |
| `ProposePartition` enqueue error (acks=0) | Rare; dragonboat pipeline full or NodeHost closing. Coordinator returns `Unavailable`. |
| `OffsetOutOfRange` (fetch below EarliestOffset) | `storage.Read` returns `ErrOffsetOutOfRange`. Coordinator returns `OffsetOutOfRange`. Client must call `GetEarliestOffset` and reset its position. |
| Leader changes during long-poll | Leader re-check inside the poll loop detects the change. Returns `NotLeader`; client reconnects and re-issues Fetch against the new leader. |
| `ctx` cancelled during long-poll | `ctx.Done()` fires in the select. Goroutine exits; returns `ctx.Err()` to the gRPC layer. |

---

## 11. Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `data_coordinator.node_address_cache_ttl_ms` | 5 000 | TTL for caching `(nodeID → Data API address)` lookups in `leaderCheck`. Reduces Metadata FSM reads on the NotLeader hot path. |

---

## 12. Open Questions

1. **Cross-module dependency: ClusterCoordinator must call StartPartitionReplica/StopPartitionReplica.** [04-cluster-coordinator.md §7.4–7.5](./04-cluster-coordinator.md) (`startShard` / `stopShard`) must be extended to call `dc.StartPartitionReplica(topic, partitionID, shardID)` immediately after `raftHost.StartPartitionShard` succeeds, and `dc.StopPartitionReplica(topic, partitionID, shardID)` before `raftHost.StopPartitionShard`. This cross-module call is the binding between the cluster's shard lifecycle and the data-plane's routing table. Confirm this is the intended interface, and note it as a required change to Module 1's implementation.

2. **`QueryGetNewDataCh` in PartitionFSM.** Long-poll fetch requires a new `PartitionQueryType` value `QueryGetNewDataCh` that makes `PartitionFSM.Lookup()` return `storage.NewDataCh()` as `interface{}`. This is a minor addition to [03-raft-fsm.md §4.5](./03-raft-fsm.md) (immutable). VERIFY: (a) the addition to the query type enum is acceptable for the implementation phase; (b) dragonboat v4 permits `Lookup()` to return arbitrary Go types (channels) via `interface{}`; (c) no dragonboat-level restriction prevents a channel reference from being returned from an `IOnDiskStateMachine.Lookup()` call made concurrently with `Update()`.

3. **Retention enforcement: local vs. Raft-commanded.** Current design follows [02-storage.md §7](./02-storage.md): each replica enforces retention independently from a Storage-internal goroutine. The alternative (leader issues `DeleteSegmentsBefore` Raft commands so all replicas delete deterministically at the same log index) requires additions to immutable documents. User decision: accept eventual-consistency retention for v1 (recommended), or schedule amendments to `03-raft-fsm.md` and `02-storage.md` for the implementation phase.

4. **`GetOffsetByTimestamp` return semantics.** `storage.ReadByTime(timestampMs, n)` returns records starting at the first batch whose `max_timestamp >= timestampMs`. The `base_offset` of that batch is the intended return value. Confirm: (a) the semantics match Kafka's `offsetsForTimes` (return the offset of the first message at or after the given timestamp, not the batch base offset); (b) a batch's `base_offset` is acceptable even if only the batch's last record has `timestamp >= timestampMs`, or whether a record-level scan is required.

5. **VERIFY — `ProposePartition` error semantics.** dragonboat's `NodeHost.Propose` returns an error when the pipeline is full or NodeHost is shutting down. Verify the exact error type(s) in dragonboat v4 and confirm the correct client-visible error code (`Unavailable` is proposed as retriable; `Unknown` if the type is unexpected).
