# Sequence: Producer Send - Full Path (cache miss, NotLeader retry)

Full `Producer.Send` flow including metadata cache population, leader resolution, and a NotLeader retry. The second call in a session to the same topic hits the warm cache and skips metadata fetch.

```mermaid
sequenceDiagram
    participant App as Application
    participant P as Producer
    participant MC as MetaCache
    participant CP as ConnPool
    participant B1 as Broker-1<br/>(bootstrap / wrong leader)
    participant B2 as Broker-2<br/>(partition leader)
    participant DAPI as DataService<br/>(Broker-2)

    App->>+P: Send(ctx, "orders", key="k1", value, AcksAll)

    Note over P: 1. Partition selection.<br/>key != nil → FNV-1a(key) % partitionCount.<br/>Requires partitionCount → check MetaCache.

    P->>+MC: GetPartitionMeta("orders", partitionID=0)
    MC-->>-P: (miss) - topic not in cache

    Note over P: 2. Metadata fetch from bootstrap server.

    P->>+CP: ConnFor("broker-1:9092")
    CP-->>-P: grpc.ClientConn (lazy dial)
    P->>+B1: ManagementService.DescribeCluster(DescribeClusterRequest{})
    B1-->>-P: DescribeClusterResponse{nodes=[{id=1,addr="broker-1:9092"},{id=2,addr="broker-2:9092"},{id=3,addr="broker-3:9092"}]}

    P->>+B1: ManagementService.DescribeTopic(DescribeTopicRequest{topic_name="orders"})
    B1-->>-P: DescribeTopicResponse{partitions=[{id=0,leader_node_id=2},{id=1,leader_node_id=3},...]}

    P->>+MC: SetTopicMeta("orders", partitionCount=8, leaderMap={0→"broker-2:9092", 1→"broker-3:9092", ...})
    MC-->>-P: ok (TTL=60s)

    Note over P: 3. Partition selection (now have count).<br/>FNV-1a("k1") % 8 = 0.<br/>Leader for partition 0 = "broker-2:9092".

    Note over P: 4. Build batch.<br/>Encode record: key, value, headers → RecordBytes.<br/>Wrap in BatchHeader (base_offset placeholder,<br/>batch_length, record_count=1, crc32c, attributes,<br/>base_timestamp, max_timestamp).

    P->>+CP: ConnFor("broker-2:9092")
    CP-->>-P: grpc.ClientConn

    P->>+DAPI: DataService.Produce(ProduceRequest{topic="orders", partition_id=0, batch_data=..., acks=ACKS_ALL})
    DAPI-->>-P: ProduceResponse{base_offset=4200}

    P->>+MC: SetLeader("orders", 0, "broker-2:9092")
    MC-->>-P: ok (confirm / reset TTL)

    P-->>-App: (offset=4200, nil)
```

---

## Warm-cache path (second call, same topic/partition)

```mermaid
sequenceDiagram
    participant App as Application
    participant P as Producer
    participant MC as MetaCache
    participant CP as ConnPool
    participant DAPI as DataService<br/>(Broker-2)

    App->>+P: Send(ctx, "orders", key="k9", value, AcksAll)

    P->>+MC: GetPartitionMeta("orders", partitionID=?)
    Note over MC: Cache hit. partitionCount=8, leader for<br/>computed partition = "broker-2:9092". TTL not expired.
    MC-->>-P: {leaderAddr="broker-2:9092"} (hit)

    Note over P: FNV-1a("k9") % 8 = 2.<br/>Leader for partition 2 = "broker-2:9092" (cached).

    P->>+CP: ConnFor("broker-2:9092")
    CP-->>-P: existing conn (already open)

    P->>+DAPI: DataService.Produce(ProduceRequest{topic="orders", partition_id=2, batch_data=..., acks=ACKS_ALL})
    DAPI-->>-P: ProduceResponse{base_offset=7800}

    P-->>-App: (offset=7800, nil)
```

---

## NotLeader retry path

```mermaid
sequenceDiagram
    participant App as Application
    participant P as Producer
    participant MC as MetaCache
    participant CP as ConnPool
    participant B2 as Broker-2<br/>(stale - no longer leader)
    participant B3 as Broker-3<br/>(new leader)
    participant DAPI3 as DataService<br/>(Broker-3)

    App->>+P: Send(ctx, "orders", key="k1", value, AcksAll)

    P->>+MC: GetPartitionMeta("orders", 0)
    MC-->>-P: {leaderAddr="broker-2:9092"} (stale cache entry)

    P->>+CP: ConnFor("broker-2:9092")
    CP-->>-P: conn

    P->>+B2: DataService.Produce(ProduceRequest{topic="orders", partition_id=0, ...})
    Note over B2: Partition shard has a new leader.<br/>DataCoordinator returns NOT_LEADER.
    B2-->>-P: status=FAILED_PRECONDITION, BunnyErrorDetail{code=NOT_LEADER,<br/>NotLeaderDetail{leader_node_id=3, leader_address="broker-3:9092"}}

    Note over P: Retry 1 (NOT_LEADER - immediate, no backoff).<br/>Update MetaCache from error detail. Retry on new leader.

    P->>+MC: SetLeader("orders", 0, "broker-3:9092")
    MC-->>-P: ok

    P->>+CP: ConnFor("broker-3:9092")
    CP-->>-P: conn (new lazy dial)

    P->>+DAPI3: DataService.Produce(ProduceRequest{topic="orders", partition_id=0, ...})
    DAPI3-->>-P: ProduceResponse{base_offset=4201}

    P-->>-App: (offset=4201, nil)
```

---

## Retry policy summary

| Error code | Retry? | Backoff | Max retries | Cache action |
|---|---|---|---|---|
| `NOT_LEADER` | Yes (immediate) | None - new leader address is in the error | `maxRetries` (default 3) | `SetLeader(topic, partition, leaderAddr)` from `NotLeaderDetail` |
| `UNAVAILABLE` | Yes | Exponential: 50ms → 100ms → 200ms → … cap 5s | `maxRetries` | None |
| `TIMEOUT` | Yes | Same as UNAVAILABLE | `maxRetries` | None |
| `INVALID_ARGUMENT` | No | - | - | None |
| `MESSAGE_TOO_LARGE` | No | - | - | None |
| `BATCH_TOO_LARGE` | No | - | - | None |
| `UNAUTHENTICATED` | No | - | - | None |
| `TOPIC_NOT_FOUND` | Refresh meta, then no | One metadata refresh, then surface to caller | 1 refresh | Full topic cache eviction |
| `INVALID_MESSAGE_FORMAT` | No | - | - | None |

## Notes

- **Metadata refresh on `TOPIC_NOT_FOUND`**: after cache eviction the producer re-fetches metadata once. If the topic still does not exist after refresh, the error is returned to the application.
- **acks=0 path**: identical routing and retry-on-NotLeader, but the Produce RPC returns `base_offset = -1` always. See [produce-acks-0.md](./produce-acks-0.md).
- **ConnPool dial**: `grpc.NewClient` is non-blocking. The TCP dial happens on the first RPC. Reconnects on transport failure are handled by gRPC's built-in retry backoff, below the producer retry loop.
- **Key=nil path**: round-robin counter (`atomic.Int64`) is used instead of FNV-1a hash. Counter is per-`Producer` instance, shared across all `Send` calls.
