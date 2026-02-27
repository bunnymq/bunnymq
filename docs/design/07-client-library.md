# Client Library — Detailed Design

The client library (`pkg/client`) provides the public Go API for interacting with a BunnyMQ cluster: a `Producer` for sending message batches, a `Consumer` for polling and committing offsets, and an `AdminClient` for topic and cluster management. Each type wraps a pool of gRPC connections, handles leader discovery and caching, and implements retry logic for transient errors. Internal helpers (batch encoding/decoding, connection pool, metadata cache) live in `internal/client` and are not part of the public API. No broker-side logic lives here: the library is a pure client.

See [06-api-protocol.md](./06-api-protocol.md) for the gRPC service definitions and error model, [05-data-coordinator.md §4.2](./05-data-coordinator.md) for the `NotLeader` response the library must handle, and [08-consumer-groups.md](./08-consumer-groups.md) for the group protocol the Consumer implements.

---

## 1. Package Layout

```text
pkg/client/
├── producer.go         — Producer, ProducerConfig, NewProducer
├── consumer.go         — Consumer, ConsumerConfig, NewConsumer
├── admin.go            — AdminClient, NewAdminClient
├── record.go           — Record type (decoded message), TP (TopicPartition alias)
└── config.go           — Config (common fields shared by all three client types)

internal/client/
├── connpool.go         — gRPC connection pool (one conn per broker address)
├── metacache.go        — topic metadata cache (partition count, per-partition leader)
├── batch_encoder.go    — encode []Record → batch_data bytes (REQUIREMENTS.md §4.4)
├── batch_decoder.go    — decode batch_data bytes → []Record
└── retry.go            — retry loop with exponential backoff
```

All types in `pkg/client` are safe for concurrent use by multiple goroutines unless stated otherwise. `internal/client` types are not exported.

---

## 2. Common Configuration

```go
// Config holds fields shared by Producer, Consumer, and AdminClient.
type Config struct {
    // BootstrapServers is the list of broker Data API addresses to attempt
    // on startup. At least one must be reachable. The client connects to the
    // first that responds and fetches full cluster metadata from it.
    BootstrapServers []string

    // AuthToken is sent as the bunnymq-auth-token gRPC metadata value on
    // every RPC. Empty string = PLAINTEXT mode (no token sent).
    AuthToken string

    // TLS optionally enables TLS for all gRPC connections.
    // nil = plaintext gRPC (default for demo deployments).
    TLS *tls.Config

    // RequestTimeout is the per-RPC deadline. Applies to each individual
    // gRPC call, not to the overall retry sequence.
    // Default: 5 s.
    RequestTimeout time.Duration

    // RetryPolicy controls automatic retry for retryable errors.
    RetryPolicy RetryPolicy
}

// RetryPolicy configures exponential backoff for retryable errors.
type RetryPolicy struct {
    // MaxRetries is the maximum number of retries (not counting the first attempt).
    // Default: 3.
    MaxRetries int
    // InitialBackoff is the delay after the first failure.
    // Default: 50 ms.
    InitialBackoff time.Duration
    // MaxBackoff caps the exponential growth.
    // Default: 2 s.
    MaxBackoff time.Duration
    // BackoffFactor multiplies the delay after each retry.
    // Default: 2.0.
    BackoffFactor float64
}
```

**Retryable vs. non-retryable errors:**

| BunnyErrorCode | Retryable | Action |
|---|---|---|
| `NOT_LEADER` | Yes, immediately | Update leader cache from `NotLeaderDetail`; retry against new leader; no backoff |
| `UNAVAILABLE` | Yes, with backoff | Refresh metadata after N consecutive `UNAVAILABLE`; back off |
| `TIMEOUT` / `DEADLINE_EXCEEDED` | Yes, with backoff | Back off; per-RPC deadline is applied independently |
| `INVALID_ARGUMENT` | No | Programming error; do not retry |
| `MESSAGE_TOO_LARGE` / `BATCH_TOO_LARGE` | No | Client must reduce payload |
| `UNAUTHENTICATED` | No | Token misconfiguration |
| `TOPIC_NOT_FOUND` | No (after meta refresh) | Refresh metadata; if topic still absent, return error |
| `TOPIC_ALREADY_EXISTS` | Treat as success | Idempotent create; return `OK` |
| `INVALID_MESSAGE_FORMAT` | No | Encoding bug |
| `OFFSET_OUT_OF_RANGE` | No | Consumer must seek to `EARLIEST` |

---

## 3. Connection Management (`internal/client.ConnPool`)

One gRPC connection is maintained per distinct broker address. Connections are established lazily on first use and cached for the lifetime of the client.

```go
type ConnPool struct {
    mu     sync.RWMutex
    conns  map[string]*grpc.ClientConn  // address → connection
    opts   []grpc.DialOption            // credentials, keepalive, etc.
}

func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error) {
    p.mu.RLock()
    conn, ok := p.conns[addr]
    p.mu.RUnlock()
    if ok {
        return conn, nil
    }

    p.mu.Lock()
    defer p.mu.Unlock()
    if conn, ok := p.conns[addr]; ok { // double-checked
        return conn, nil
    }
    conn, err := grpc.NewClient(addr, p.opts...)
    if err != nil {
        return nil, err
    }
    p.conns[addr] = conn
    return conn, nil
}

func (p *ConnPool) Close() error {
    p.mu.Lock()
    defer p.mu.Unlock()
    var errs []error
    for _, conn := range p.conns {
        if err := conn.Close(); err != nil {
            errs = append(errs, err)
        }
    }
    p.conns = nil
    return errors.Join(errs...)
}
```

gRPC manages reconnection internally (channel state machine with exponential backoff). The `ConnPool` does not implement explicit reconnect logic. `grpc.NewClient` is non-blocking; the actual TCP connection is established on the first RPC call.

**Dial options applied to every connection:**
- Auth interceptor: injects `bunnymq-auth-token` metadata on every call (if token is non-empty).
- Keepalive: `keepalive.ClientParameters{Time: 30s, Timeout: 10s}` to detect dead connections.
- TLS credentials (if `config.TLS != nil`).
- `grpc.WithBlock()` is NOT used; connections are non-blocking.

---

## 4. Metadata Cache (`internal/client.MetaCache`)

The metadata cache holds topic-level information needed for partition selection and leader routing.

```go
type TopicMeta struct {
    PartitionCount int32
    Leaders        map[int32]string  // partitionID → Data API address of leader
    FetchedAt      time.Time
}

type MetaCache struct {
    mu    sync.RWMutex
    cache map[string]*TopicMeta  // topic → metadata
    ttl   time.Duration          // default: 60 s
}

// Get returns cached metadata if fresh, or nil if absent/expired.
func (mc *MetaCache) Get(topic string) *TopicMeta

// Put stores or replaces metadata for a topic.
func (mc *MetaCache) Put(topic string, meta *TopicMeta)

// SetLeader updates the leader address for one partition without invalidating
// the rest of the cached metadata. Called on NotLeader with the new address.
func (mc *MetaCache) SetLeader(topic string, partitionID int32, addr string)

// Invalidate removes a topic's cache entry, forcing a refresh on next access.
func (mc *MetaCache) Invalidate(topic string)
```

**Cache population.** The client calls `ManagementService.DescribeTopic` (on any bootstrap server or any known broker) to populate the cache. The response provides `PartitionCount` and, for each partition, `LeaderNodeID`. The client then looks up `NodeInfo.address` from `DescribeCluster` (or inlines the address from a cached node list) to resolve the Data API address.

**Simplified path.** To avoid the two-RPC roundtrip (DescribeTopic + DescribeCluster), `DescribeTopic` could be extended to return node addresses directly, or the client could call `ListPartitions` which returns `PartitionInfoWithOffsets` but not addresses. For v1, the two-RPC path is used. See Open Question 1.

---

## 5. Batch Encoder / Decoder (`internal/client`)

### Encoder

`BatchEncoder.Encode(records []Record, nowMs int64) ([]byte, error)` builds a complete batch from a slice of records:

1. Encodes each record into its variable-length wire form (key, value, headers, deltas).
2. Writes the batch header: `base_offset=0`, `batch_length`, `record_count`, `base_timestamp=records[0].TimestampMs`, `max_timestamp=records[last].TimestampMs`, `attributes=0`.
3. Computes CRC-32C over `records[]` (bytes [38, batch_length)) and writes to header offset 16.
4. Returns the complete batch bytes.

`base_offset` is set to 0; Storage overwrites it with the server-assigned offset.

### Decoder

`BatchDecoder.Decode(data []byte) ([]Record, error)` parses one or more consecutive batches from `FetchResponse.records`:

1. Reads batch header (38 bytes): extracts `base_offset`, `batch_length`, `record_count`, `base_timestamp`.
2. Verifies CRC-32C.
3. Decodes each record: `offset = base_offset + offset_delta`, `timestamp_ms = base_timestamp + timestamp_delta`.
4. Returns decoded `[]Record`.
5. Advances by `batch_length` bytes; repeats until the buffer is exhausted.

On CRC mismatch, returns an error for the affected batch; records from earlier batches in the same response are returned as-is. This is a defensive measure; in practice, gRPC's transport integrity should prevent corruption.

---

## 6. Producer

### 6.1 Configuration

```go
type ProducerConfig struct {
    Config                         // embedded common config
    DefaultAcks      AcksMode      // default: AcksAll
    MetadataCacheTTL time.Duration // TTL for topic metadata. Default: 60 s.
}
```

### 6.2 Public interface

```go
// NewProducer creates a Producer, verifies connectivity to at least one
// bootstrap server, and fetches initial cluster metadata.
// Returns an error if no bootstrap server is reachable.
func NewProducer(config ProducerConfig) (*Producer, error)

// Send encodes key+value+headers as a single-record batch and produces it to
// the given topic. Partition selection: FNV-1a 32-bit hash of key bytes modulo
// partition count, or round-robin if key is nil or empty.
// Returns the assigned base_offset on success, -1 for AcksZero.
// Applies the retry policy for retryable errors.
func (p *Producer) Send(
    ctx context.Context,
    topic string,
    key, value []byte,
    headers map[string][]byte,
    acks AcksMode,
) (offset int64, err error)

// SendBatch produces a pre-encoded batch to a specific partition.
// The caller is responsible for encoding the batch in the on-wire format
// (REQUIREMENTS.md §4.4). The base_offset field in the batch header is
// ignored — Storage assigns it. Returns the assigned base_offset, or -1
// for AcksZero.
func (p *Producer) SendBatch(
    ctx context.Context,
    topic string,
    partitionID int32,
    batchData []byte,
    acks AcksMode,
) (offset int64, err error)

// Flush is a no-op in v1 (there is no internal batching buffer).
// It exists so that code written for a future batching Producer compiles
// without changes.
func (p *Producer) Flush(ctx context.Context) error

// Close releases all gRPC connections. No in-flight RPCs are cancelled;
// callers should drain before calling Close.
func (p *Producer) Close() error
```

### 6.3 Partition selection

```go
func selectPartition(key []byte, partitionCount int32, counter *atomic.Int64) int32 {
    if len(key) == 0 {
        // Round-robin: per-Producer atomic counter, wraps at partition count.
        n := counter.Add(1) - 1
        return int32(n % int64(partitionCount))
    }
    h := fnv.New32a()
    h.Write(key)
    return int32(h.Sum32() % uint32(partitionCount))
}
```

The round-robin counter is per-`Producer` instance (not global) to avoid cross-producer interference. Key-based routing is sticky: the same key always maps to the same partition for a given `partitionCount`. If partition count changes (AlterTopicPartitions), the mapping shifts. This matches Kafka's behaviour and is documented as a known limitation.

### 6.4 Send flow

```go
func (p *Producer) Send(ctx context.Context, topic string, key, value []byte,
    headers map[string][]byte, acks AcksMode) (int64, error) {

    // 1. Ensure metadata is available for the topic.
    meta, err := p.metaFor(ctx, topic)
    if err != nil {
        return -1, err
    }

    // 2. Select partition.
    partID := selectPartition(key, meta.PartitionCount, &p.roundRobinCounter)

    // 3. Encode single-record batch.
    nowMs := time.Now().UnixMilli()
    batchData, err := p.encoder.Encode([]Record{{
        Key: key, Value: value, Headers: headers, TimestampMs: nowMs,
    }}, nowMs)
    if err != nil {
        return -1, err
    }

    return p.sendToPartition(ctx, topic, partID, batchData, acks)
}

func (p *Producer) sendToPartition(ctx context.Context,
    topic string, partID int32, batchData []byte, acks AcksMode) (int64, error) {

    for attempt := 0; ; attempt++ {
        // Get leader address from cache (or fetch if absent/expired).
        addr, err := p.leaderFor(ctx, topic, partID)
        if err != nil {
            return -1, err
        }

        // Send RPC.
        conn, err := p.pool.Get(addr)
        if err != nil {
            return -1, backoffOrFail(attempt, err, p.config.RetryPolicy)
        }
        client := pb.NewDataServiceClient(conn)
        callCtx, cancel := context.WithTimeout(ctx, p.config.RequestTimeout)
        resp, err := client.Produce(callCtx, &pb.ProduceRequest{
            Topic:       topic,
            PartitionId: partID,
            Acks:        pb.AcksMode(acks),
            BatchData:   batchData,
        })
        cancel()

        if err == nil {
            return resp.Offset, nil
        }

        // Classify error and decide whether to retry.
        bunnyErr := extractBunnyError(err)
        switch bunnyErr.Code {
        case pb.BunnyErrorCode_NOT_LEADER:
            // Update leader cache; retry immediately (no backoff).
            newAddr := extractLeaderAddress(err)
            p.meta.SetLeader(topic, partID, newAddr)
            if attempt >= p.config.RetryPolicy.MaxRetries {
                return -1, err
            }
            continue

        case pb.BunnyErrorCode_UNAVAILABLE, pb.BunnyErrorCode_TIMEOUT:
            if attempt >= p.config.RetryPolicy.MaxRetries {
                return -1, err
            }
            time.Sleep(backoffDuration(attempt, p.config.RetryPolicy))
            continue

        default:
            return -1, err // non-retryable
        }
    }
}
```

### 6.5 Metadata fetch (`metaFor`)

```go
func (p *Producer) metaFor(ctx context.Context, topic string) (*TopicMeta, error) {
    if meta := p.meta.Get(topic); meta != nil {
        return meta, nil
    }
    return p.refreshMeta(ctx, topic)
}

func (p *Producer) refreshMeta(ctx context.Context, topic string) (*TopicMeta, error) {
    // Try each known broker address (bootstrap servers + any known leaders).
    for _, addr := range p.knownAddresses() {
        conn, _ := p.pool.Get(addr)
        mgmt := pb.NewManagementServiceClient(conn)
        callCtx, cancel := context.WithTimeout(ctx, p.config.RequestTimeout)
        resp, err := mgmt.DescribeTopic(callCtx, &pb.DescribeTopicRequest{Name: topic})
        cancel()
        if err != nil {
            continue
        }
        meta := buildTopicMeta(resp, p.nodeAddressCache)
        p.meta.Put(topic, meta)
        return meta, nil
    }
    return nil, ErrNoReachableServer
}
```

---

## 7. Consumer

### 7.1 Configuration

```go
type ConsumerConfig struct {
    Config                          // embedded common config
    GroupID            string       // empty = manual assignment (no-group) mode
    SessionTimeout     time.Duration // server-side member expiry. Default: 30 s.
    HeartbeatInterval  time.Duration // how often to heartbeat. Default: 3 s.
    MaxFetchBytes      int           // max bytes per Fetch RPC. Default: 1 MiB.
    MaxFetchWaitMs     int64         // max_wait_ms per Fetch RPC. Default: 500 ms.
    AutoCommit         bool          // auto-commit on heartbeat tick. Default: true.
    AutoCommitInterval time.Duration // auto-commit interval. Default: 5 s.
    AutoOffsetReset    OffsetResetPolicy // where to start when no committed offset exists.
}

type OffsetResetPolicy int
const (
    OffsetResetLatest   OffsetResetPolicy = 0 // start from latest (default)
    OffsetResetEarliest OffsetResetPolicy = 1 // start from earliest
)

type TP struct { Topic string; PartitionID int32 } // TopicPartition shorthand
```

### 7.2 Public interface

```go
func NewConsumer(config ConsumerConfig) (*Consumer, error)

// Subscribe sets the topics to consume from. For group consumers, triggers
// JoinGroup and starts the background heartbeat goroutine. Blocks until
// JoinGroup completes and assignments are received.
// For manual consumers (no GroupID), records which topics to poll from;
// the caller must also call Seek to set the starting offset on each partition.
func (c *Consumer) Subscribe(topics []string) error

// Poll fetches records from assigned (group consumer) or sought (manual consumer)
// partitions. Calls Fetch for each partition with max_wait_ms derived from the
// remaining poll budget divided by the number of unfetched partitions. Returns
// decoded records. Triggers rejoin if a rebalance is pending.
// maxWaitMs is the total budget across all partitions.
func (c *Consumer) Poll(ctx context.Context, maxWaitMs int64) ([]Record, error)

// Commit commits the highest fetched offset + 1 for each assigned partition to
// the group coordinator. No-op for manual consumers with no GroupID.
func (c *Consumer) Commit(ctx context.Context) error

// CommitOffsets commits the caller-specified offsets. Useful for fine-grained
// control over exactly which offset is acknowledged.
func (c *Consumer) CommitOffsets(ctx context.Context, offsets map[TP]int64) error

// Seek overrides the next fetch offset for a specific partition. For manual
// consumers, this is the primary way to set the read position. For group
// consumers, Seek affects only in-memory state; it does not commit to the server.
func (c *Consumer) Seek(topic string, partitionID int32, offset int64)

// Close sends LeaveGroup (for group consumers), cancels the heartbeat goroutine,
// and closes all gRPC connections.
func (c *Consumer) Close() error
```

### 7.3 Record type

```go
// Record is a decoded message from a BunnyMQ partition.
type Record struct {
    Topic       string
    PartitionID int32
    Offset      int64
    Key         []byte            // nil if no key
    Value       []byte
    Headers     map[string][]byte // nil if no headers
    TimestampMs int64
}
```

### 7.4 Group consumer lifecycle

#### Subscribe → JoinGroup

```go
func (c *Consumer) Subscribe(topics []string) error {
    c.subscribedTopics = topics

    if c.config.GroupID == "" {
        return nil // manual mode; no JoinGroup
    }

    return c.joinGroup(context.Background())
}

func (c *Consumer) joinGroup(ctx context.Context) error {
    addr := c.groupCoordinatorAddr(ctx) // DescribeCluster → metadata shard leader address

    conn, _ := c.pool.Get(addr)
    svc := pb.NewDataServiceClient(conn)

    callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
    resp, err := svc.JoinGroup(callCtx, &pb.JoinGroupRequest{
        GroupId:          c.config.GroupID,
        MemberId:         c.memberID,     // empty on first join; re-used on rejoin
        SubscribedTopics: c.subscribedTopics,
        ClientHost:       hostname(),
    })
    cancel()
    if err != nil {
        return err
    }

    c.memberID    = resp.MemberId
    c.generationID= resp.GenerationId
    c.assignments = protoToTP(resp.Assignments)

    // Initialise fetch offsets from committed positions (or AutoOffsetReset).
    if err := c.initFetchOffsets(ctx); err != nil {
        return err
    }

    // Start (or restart) heartbeat goroutine.
    c.startHeartbeat()
    return nil
}
```

#### Heartbeat goroutine

```go
func (c *Consumer) heartbeatLoop() {
    ticker := time.NewTicker(c.config.HeartbeatInterval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            resp, err := c.sendHeartbeat()
            if err != nil {
                c.logger.Warn("heartbeat error", zap.Error(err))
                continue
            }
            if resp.Status == pb.HeartbeatStatus_HEARTBEAT_REBALANCE_REQUIRED {
                // Signal the Poll goroutine to rejoin.
                select {
                case c.rebalanceCh <- struct{}{}:
                default: // channel already signalled; don't block
                }
            }
            if c.config.AutoCommit {
                c.maybeAutoCommit()
            }

        case <-c.heartbeatStopCh:
            return
        }
    }
}
```

The heartbeat goroutine is the sole writer to `lastHeartbeatSent`. It does not read or modify `fetchOffsets`; those are owned by the Poll goroutine. The only shared state between the heartbeat goroutine and the Poll goroutine is `rebalanceCh` (channel; safe) and `uncommittedOffsets` (protected by a mutex, accessed by both auto-commit and Poll).

#### Poll — rebalance handling

```go
func (c *Consumer) Poll(ctx context.Context, maxWaitMs int64) ([]Record, error) {
    // Check for pending rebalance before fetching.
    select {
    case <-c.rebalanceCh:
        if err := c.rejoin(ctx); err != nil {
            return nil, err
        }
    default:
    }

    deadline := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)
    var records []Record

    for i, tp := range c.assignments {
        remaining := time.Until(deadline)
        if remaining <= 0 {
            break
        }
        // Distribute remaining budget evenly over the rest of the partitions.
        perPartitionWaitMs := remaining.Milliseconds() / int64(len(c.assignments)-i)
        if perPartitionWaitMs > c.config.MaxFetchWaitMs {
            perPartitionWaitMs = c.config.MaxFetchWaitMs
        }

        recs, err := c.fetchPartition(ctx, tp, perPartitionWaitMs)
        if err != nil {
            if isNotLeader(err) {
                c.meta.Invalidate(tp.Topic)
                continue // skip partition; will retry on next Poll
            }
            return nil, err
        }
        records = append(records, recs...)
    }

    return records, nil
}

func (c *Consumer) rejoin(ctx context.Context) error {
    c.stopHeartbeat()        // pause heartbeat during rejoin
    c.commitUncommitted(ctx) // best-effort commit before reassignment
    return c.joinGroup(ctx)  // blocks until new assignment received
}
```

#### initFetchOffsets

```go
func (c *Consumer) initFetchOffsets(ctx context.Context) error {
    // Fetch committed offsets for all assigned partitions.
    committed, err := c.fetchCommittedOffsets(ctx)
    if err != nil {
        return err
    }
    for _, tp := range c.assignments {
        if off, ok := committed[tp]; ok {
            c.fetchOffsets[tp] = off + 1 // resume from next-after-committed
        } else {
            // No committed offset: apply AutoOffsetReset policy.
            switch c.config.AutoOffsetReset {
            case OffsetResetEarliest:
                earliest, _ := c.getOffset(ctx, tp, pb.OffsetQueryType_EARLIEST)
                c.fetchOffsets[tp] = earliest
            case OffsetResetLatest:
                latest, _ := c.getOffset(ctx, tp, pb.OffsetQueryType_LATEST)
                c.fetchOffsets[tp] = latest
            }
        }
    }
    return nil
}
```

### 7.5 Manual consumer (no GroupID)

When `ConsumerConfig.GroupID` is empty:
- `Subscribe(topics)` stores the topic list but does not call `JoinGroup`.
- No heartbeat goroutine is started.
- The caller must call `Seek(topic, partitionID, offset)` before the first `Poll`. Without a Seek, the initial offset is 0 (the earliest possible offset; safe default for manual mode).
- `Poll` fetches from partitions that have been `Seek`'d to.
- `Commit` and `CommitOffsets` are no-ops if no `GroupID` is configured. Offsets are tracked only in memory and lost on consumer restart.
- There is no rebalance; the caller manages partition assignment explicitly.

---

## 8. AdminClient

A thin wrapper over `ManagementServiceClient`. No caching, no retry beyond the common policy. One gRPC connection to a configured target or a bootstrap server.

```go
func NewAdminClient(config Config) (*AdminClient, error)

func (a *AdminClient) CreateTopic(ctx context.Context, req CreateTopicRequest) (TopicInfo, error)
func (a *AdminClient) DeleteTopic(ctx context.Context, name string) error
func (a *AdminClient) ListTopics(ctx context.Context) ([]TopicInfo, error)
func (a *AdminClient) DescribeTopic(ctx context.Context, name string) (TopicDescription, error)
func (a *AdminClient) AlterTopicPartitions(ctx context.Context, name string, newCount int32) error
func (a *AdminClient) AlterTopicRetention(ctx context.Context, name string, retentionMs, retentionBytes int64) error
func (a *AdminClient) DescribeCluster(ctx context.Context) (ClusterDescription, error)
func (a *AdminClient) ListPartitions(ctx context.Context, topic string) ([]PartitionInfoWithOffsets, error)
func (a *AdminClient) Close() error
```

`AdminClient` targets the `ManagementService` port (`:9091`). `Producer` and `Consumer` target the `DataService` port (`:9092`). The `ConnPool` in each client type holds connections keyed by `"host:port"` tuples; Management API and Data API connections to the same broker are distinct entries.

---

## 9. Concurrency Model

### Producer

| Goroutine | Shared state | Protection |
|---|---|---|
| Caller goroutines (N concurrent `Send` calls) | `MetaCache`, `ConnPool`, `roundRobinCounter` | `MetaCache` uses its own `RWMutex`; `ConnPool` uses its own `RWMutex`; counter is `atomic.Int64` |
| No background goroutines | — | — |

Multiple goroutines may call `Send` concurrently. Each call is independent: it reads from the metadata cache (shared), selects a partition (atomic increment), encodes a batch (local), and sends one gRPC RPC (gRPC stubs are safe for concurrent use).

### Consumer

| Goroutine | Owned state | Shared state |
|---|---|---|
| Caller goroutine (`Poll`, `Commit`, etc.) | `fetchOffsets`, `assignments` | `uncommittedOffsets` (offsetMu), `rebalanceCh` |
| Heartbeat goroutine | `lastHeartbeatSent`, `lastCommit` | `uncommittedOffsets` (offsetMu), `rebalanceCh` |

The caller goroutine and the heartbeat goroutine must not be used concurrently. That is: only one goroutine should call `Poll` / `Commit` at a time; the heartbeat goroutine runs internally. This is the standard single-threaded consumer loop pattern (same as Kafka's Java client).

`uncommittedOffsets` (the highest fetched offset per partition, used for auto-commit) is protected by `offsetMu sync.Mutex`.

### AdminClient

Goroutine-safe. Each method makes one gRPC call independently.

---

## 10. Configuration Parameters (summary)

| Parameter | Default | Description |
|---|---|---|
| `BootstrapServers` | required | Broker Data API addresses for initial connection |
| `AuthToken` | `""` | Auth token; empty = PLAINTEXT |
| `RequestTimeout` | 5 s | Per-RPC deadline |
| `RetryPolicy.MaxRetries` | 3 | Max retries for retryable errors |
| `RetryPolicy.InitialBackoff` | 50 ms | First retry delay |
| `RetryPolicy.MaxBackoff` | 2 s | Maximum retry delay |
| `RetryPolicy.BackoffFactor` | 2.0 | Exponential multiplier |
| `ProducerConfig.DefaultAcks` | `AcksAll` | Default acks mode for `Send` |
| `ProducerConfig.MetadataCacheTTL` | 60 s | Topic metadata cache TTL |
| `ConsumerConfig.GroupID` | `""` | Consumer group; empty = manual mode |
| `ConsumerConfig.SessionTimeout` | 30 s | Server-side member expiry |
| `ConsumerConfig.HeartbeatInterval` | 3 s | Heartbeat frequency |
| `ConsumerConfig.MaxFetchBytes` | 1 MiB | Max bytes per Fetch RPC |
| `ConsumerConfig.MaxFetchWaitMs` | 500 | max_wait_ms per Fetch RPC |
| `ConsumerConfig.AutoCommit` | `true` | Auto-commit on heartbeat tick |
| `ConsumerConfig.AutoCommitInterval` | 5 s | Auto-commit frequency |
| `ConsumerConfig.AutoOffsetReset` | `OffsetResetLatest` | Where to start with no committed offset |

---

## 11. Open Questions

1. **Metadata fetch: two-RPC round-trip.** Populating the leader cache requires `DescribeTopic` (partition metadata including `LeaderNodeID`) followed by resolving `LeaderNodeID → Data API address` (requiring `DescribeCluster` or a node-address lookup). To reduce this to one RPC, consider extending `DescribeTopicResponse.PartitionInfo` to include `leader_address string` directly. This is an API change deferred to the API Protocol design (Module 3 Open Question) but has impact on how the client library is implemented.

2. **Group coordinator discovery.** `JoinGroup` must be sent to the metadata shard leader (which acts as the group coordinator in v1). The client discovers this address via `DescribeCluster` on any bootstrap server and then sending `JoinGroup` — the server returns `NotLeader` if it is not the metadata shard leader. The client then retries against the `NotLeaderDetail.leader_address`. Confirm this is the correct discovery path, or whether `DescribeCluster` should expose a `coordinator_address` field explicitly.

3. **`Poll` for manual consumers with multiple partitions.** The current design is sequential: fetch from each sought partition in order. For a manual consumer with many partitions, this may be slow. An alternative: fan-out goroutines per partition, collect results within the poll budget. Recommend keeping sequential for v1 (simpler, no goroutine pool overhead); fan-out as optional optimisation.

4. **`Seek` during active group consumption.** If a group consumer calls `Seek` between two `Poll` calls, the seek offset overrides the server-committed offset for that partition. On the next rebalance (rejoin), `initFetchOffsets` re-reads the committed offset and discards the seek. This may surprise callers. Confirm: Seek is intended for manual consumers only; group consumers should use `CommitOffsets` to control their position persistently.

5. **Auto-commit and `STALE_GENERATION`.** If a rebalance occurs between two heartbeats, the auto-commit in the heartbeat goroutine may fire with a stale `generationID` and receive `STALE_GENERATION`. The goroutine should log this at `warn` and skip the commit; the rejoin path triggered by the next heartbeat response (`REBALANCE_REQUIRED`) will reset the generation. Confirm this error handling is acceptable.

6. **`Flush` future compatibility.** `Flush` is a no-op in v1. If future versions add internal batching (accumulating records before sending), `Flush` will block until the buffer drains. The no-op implementation is a compatible placeholder. Callers should call `Flush` before `Close` to be future-safe.
