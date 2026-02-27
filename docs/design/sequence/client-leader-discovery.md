# Sequence: Client Leader Discovery

How the client library learns the current partition leader - at startup (cold cache) and after a leader change (stale cache). Applies to both Producer and Consumer. The AdminClient uses the same bootstrap mechanism but never touches the per-partition leader cache.

---

## Cold start: metadata fetch from bootstrap server

```mermaid
sequenceDiagram
    participant Client as Producer / Consumer
    participant CP as ConnPool
    participant BS as Bootstrap Broker<br/>(any node)
    participant MC as MetaCache

    Note over Client: First operation on topic "orders".<br/>MetaCache has no entry for this topic.

    Client->>+CP: ConnFor(bootstrapAddrs[0])
    CP-->>-Client: grpc.ClientConn (lazy dial)

    Client->>+BS: ManagementService.DescribeCluster(DescribeClusterRequest{})
    BS-->>-Client: DescribeClusterResponse{<br/>nodes=[<br/>  {node_id=1, address="b1:9092"},<br/>  {node_id=2, address="b2:9092"},<br/>  {node_id=3, address="b3:9092"}<br/>],<br/>metadata_leader_node_id=1}

    Client->>+BS: ManagementService.DescribeTopic(DescribeTopicRequest{topic_name="orders"})
    BS-->>-Client: DescribeTopicResponse{<br/>topic_name="orders",<br/>partitions=[<br/>  {partition_id=0, leader_node_id=2, replica_node_ids=[2,3,1]},<br/>  {partition_id=1, leader_node_id=3, replica_node_ids=[3,1,2]},<br/>  {partition_id=2, leader_node_id=1, replica_node_ids=[1,2,3]},<br/>  ...<br/>]}

    Client->>+MC: SetTopicMeta("orders", {<br/>  partitionCount: 8,<br/>  leaders: {0→"b2:9092", 1→"b3:9092", 2→"b1:9092", ...},<br/>  cachedAt: now, TTL: 60s<br/>})
    MC-->>-Client: ok

    Note over Client: Continue with operation (Produce / Fetch)<br/>using leader address from MetaCache.
```

---

## Leader-change discovery via NOT_LEADER response

```mermaid
sequenceDiagram
    participant Client as Producer / Consumer
    participant MC as MetaCache
    participant CP as ConnPool
    participant OldLeader as Broker-2<br/>(was leader, now follower)
    participant NewLeader as Broker-3<br/>(new leader)

    Note over Client: MetaCache: partition 0 leader = "b2:9092" (stale).

    Client->>+CP: ConnFor("b2:9092")
    CP-->>-Client: conn

    Client->>+OldLeader: DataService.Produce / Fetch (partition 0)
    Note over OldLeader: Raft elected a new leader for this partition.<br/>DataCoordinator returns NOT_LEADER.
    OldLeader-->>-Client: status=FAILED_PRECONDITION<br/>BunnyErrorDetail{code=NOT_LEADER,<br/>NotLeaderDetail{leader_node_id=3, leader_address="b3:9092"}}

    Note over Client: Extract leader_address from NotLeaderDetail.<br/>Update MetaCache immediately (no full DescribeTopic needed).

    Client->>+MC: SetLeader("orders", partition_id=0, "b3:9092")
    MC-->>-Client: ok

    Client->>+CP: ConnFor("b3:9092")
    CP-->>-Client: conn (lazy dial if new address)

    Client->>+NewLeader: DataService.Produce / Fetch (partition 0) - retry
    NewLeader-->>-Client: OK response
```

---

## TTL expiry: proactive metadata refresh

```mermaid
sequenceDiagram
    participant Client as Producer / Consumer
    participant MC as MetaCache
    participant CP as ConnPool
    participant AnyBroker as Any Broker<br/>(bootstrap list)

    Note over Client: MetaCache entry for "orders" has expired (TTL=60s).

    Client->>+MC: GetPartitionMeta("orders", partition_id=0)
    MC-->>-Client: (miss - TTL expired)

    Note over Client: Fall back to full metadata fetch,<br/>same as cold-start path.

    Client->>+CP: ConnFor(bootstrapAddrs[0])
    CP-->>-Client: conn

    Client->>+AnyBroker: ManagementService.DescribeTopic("orders")
    AnyBroker-->>-Client: DescribeTopicResponse{partitions=[{leader_node_id=3, ...}, ...]}

    Client->>+MC: SetTopicMeta("orders", ...) - refresh
    MC-->>-Client: ok

    Note over Client: Continue with Produce / Fetch using refreshed leaders.
```

---

## Bootstrap failover: first broker unreachable

```mermaid
sequenceDiagram
    participant Client as Producer / Consumer
    participant CP as ConnPool
    participant BS1 as Bootstrap Broker-1<br/>(unreachable)
    participant BS2 as Bootstrap Broker-2<br/>(reachable)
    participant MC as MetaCache

    Note over Client: Cold start. bootstrapAddrs=["b1:9092","b2:9092","b3:9092"].

    Client->>+CP: ConnFor("b1:9092")
    CP-->>-Client: conn

    Client->>+BS1: ManagementService.DescribeTopic(...)
    Note over BS1: Connection refused / deadline exceeded.
    BS1-->>-Client: UNAVAILABLE

    Note over Client: Try next bootstrap address.

    Client->>+CP: ConnFor("b2:9092")
    CP-->>-Client: conn

    Client->>+BS2: ManagementService.DescribeTopic("orders")
    BS2-->>-Client: DescribeTopicResponse{partitions=[...]}

    Client->>+MC: SetTopicMeta("orders", ...)
    MC-->>-Client: ok

    Note over Client: Operation proceeds normally from "b2:9092" metadata.
```

---

## MetaCache data structure

```go
type topicMeta struct {
    partitionCount int32
    leaders        map[int32]string // partition_id → "host:port"
    cachedAt       time.Time
}

type MetaCache struct {
    mu     sync.RWMutex
    topics map[string]*topicMeta // topic_name → meta
    ttl    time.Duration         // default 60s
}

// GetPartitionMeta returns ("", false) on miss (cold or TTL expired).
func (mc *MetaCache) GetPartitionMeta(topic string, partitionID int32) (leaderAddr string, ok bool)

// SetTopicMeta replaces the full topic entry and resets TTL.
func (mc *MetaCache) SetTopicMeta(topic string, partitionCount int32, leaders map[int32]string)

// SetLeader updates a single partition's leader without touching TTL or other partitions.
func (mc *MetaCache) SetLeader(topic string, partitionID int32, addr string)

// EvictTopic removes the topic entry entirely (called on TOPIC_NOT_FOUND after refresh).
func (mc *MetaCache) EvictTopic(topic string)
```

---

## Notes

- **No in-cluster forwarding.** The client always contacts the partition leader directly. There is no proxy or forward-to-leader within the broker cluster in v1.
- **SetLeader vs SetTopicMeta.** On a `NOT_LEADER` response the client calls `SetLeader` (single-partition point update using the address already in the error detail). A full `DescribeTopic` is only fetched on cold start or TTL expiry, to avoid a management RPC on every leader change.
- **Concurrent access.** `MetaCache` uses `sync.RWMutex`. `GetPartitionMeta` acquires `RLock`; `SetLeader` / `SetTopicMeta` / `EvictTopic` acquire `Lock`. Multiple goroutines (e.g. parallel per-partition sends) can safely read concurrently.
- **Bootstrap round-robin.** The client tries bootstrap addresses in order. A failed address is skipped for that attempt; all addresses are tried again on the next cold-start trigger (TTL expiry or first-use).
- **Coordinator discovery.** The Consumer uses the same `DescribeCluster` response to find `metadata_leader_node_id` and the corresponding address, which is used as the Group Coordinator address for `JoinGroup`, `Heartbeat`, `LeaveGroup`, `CommitOffset`, and `FetchCommittedOffsets`.
