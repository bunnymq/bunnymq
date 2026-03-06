# T-044: Client library — Producer

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement `pkg/client.Producer` with `Send` (single-record batch) and `SendBatch` (pre-encoded batch), including partition selection (FNV-1a hash or round-robin), metadata fetch (`metaFor`), and the retry loop for `NOT_LEADER` and `UNAVAILABLE` errors.

## Context

`Producer` is the primary write interface for BunnyMQ clients. It encodes a single record into a batch, selects the target partition, looks up the leader from the metadata cache, and sends one `DataService.Produce` RPC. On `NOT_LEADER`, it updates the leader cache from `NotLeaderDetail` and retries immediately. On `UNAVAILABLE`/`TIMEOUT`, it backs off and retries up to `MaxRetries`.

References:
- [07-client-library.md §6 — Producer](../../design/07-client-library.md#6-producer)
- [07-client-library.md §6.3 — Partition selection](../../design/07-client-library.md#63-partition-selection)
- [07-client-library.md §6.4 — Send flow](../../design/07-client-library.md#64-send-flow)

## Scope

- Create `pkg/client/producer.go`:
  - `ProducerConfig` struct (embeds `Config`): `DefaultAcks AcksMode`, `MetadataCacheTTL time.Duration`.
  - `Producer` struct: `config ProducerConfig`, `pool *iclient.ConnPool`, `meta *iclient.MetaCache`, `encoder *iclient.BatchEncoder`, `roundRobinCounter atomic.Int64`, `knownAddrs []string`.
  - `NewProducer(config ProducerConfig) (*Producer, error)` — dials bootstrap servers; calls `DescribeTopic` on at least one to verify connectivity (optional pre-warm); does NOT fail if topic doesn't exist yet.
  - `Send(ctx, topic, key, value, headers, acks) (int64, error)`:
    - `metaFor(ctx, topic)` — cache hit or `refreshMeta`.
    - `selectPartition(key, partitionCount, &counter)` — FNV-1a hash of key or round-robin.
    - `encoder.Encode([]Record{{Key, Value, Headers, TimestampMs: time.Now().UnixMilli()}})`.
    - `sendToPartition(ctx, topic, partID, batchData, acks)`.
  - `SendBatch(ctx, topic, partitionID, batchData, acks) (int64, error)` — skip encode step; call `sendToPartition`.
  - `sendToPartition` retry loop (from §6.4): `NOT_LEADER` → `SetLeader` + retry; `UNAVAILABLE`/`TIMEOUT` → backoff + retry; others → return error.
  - `Flush(ctx) error` — returns nil (no-op in v1).
  - `Close() error` — `pool.Close()`.
  - `selectPartition(key, count, counter)` — exact algorithm from §6.3.
  - `metaFor(ctx, topic)` — returns cached or calls `refreshMeta(ctx, topic)`.
  - `refreshMeta(ctx, topic)` — tries each `knownAddrs`; calls `ManagementService.DescribeTopic`; populates cache.

## Out of scope

- Consumer — T-046.
- AdminClient — T-045.
- BatchEncoder implementation — delegate to `internal/storage.EncodeBatch` (T-015) or its client-side equivalent.

## Definition of done

- [ ] `go build ./pkg/client/...` passes.
- [ ] `go test ./pkg/client/...` passes.
- [ ] `Send` with nil key: round-robin selects partitions sequentially on repeated calls.
- [ ] `Send` with non-nil key: same key → same partition across multiple calls.
- [ ] `Send` `NOT_LEADER`: leader cache updated from `NotLeaderDetail`; retry goes to new leader address.
- [ ] `Send` `NOT_LEADER` after `MaxRetries`: returns error.
- [ ] `Send` `UNAVAILABLE`: exponential backoff between retries.
- [ ] `SendBatch` calls `DataService.Produce` with the provided batch bytes.

## Tests required

- `TestProducer_Send_RoundRobin` — nil key; 6 sends to 3-partition topic; partition distribution is 2-2-2.
- `TestProducer_Send_KeyHash` — key="foo"; 3 sends; all go to same partition.
- `TestProducer_Send_NotLeader_Retry` — server stub returns `NOT_LEADER` first then success; verify two RPC calls; second call goes to leader address from detail.
- `TestProducer_Send_MaxRetriesExceeded` — server always returns `NOT_LEADER`; after `MaxRetries+1` attempts returns error.
- `TestProducer_Send_Unavailable_Backoff` — server returns `UNAVAILABLE` twice then success; verify delay between retries > `InitialBackoff`.
- `TestProducer_SendBatch_Success` — pre-encoded batch; RPC called with exact bytes.
- `TestProducer_RefreshMeta_TriesAllBootstrap` — first bootstrap fails; second succeeds; `metaFor` returns populated cache.

## Dependencies

T-034 (proto stubs).
T-043 (ConnPool, MetaCache, retry helpers).
T-015 (EncodeBatch — reused for batch encoding; client-side encoder mirrors the same format).

## Notes

The `BatchEncoder` in `internal/client/batch_encoder.go` can be a thin wrapper around the same encoding logic as `internal/storage.EncodeBatch` (T-015). The key difference is that the client sets `base_offset = 0` (Storage overwrites it) and populates `base_timestamp`/`max_timestamp` from the records' `TimestampMs` values. To avoid duplicating the encoding logic, extract it into a shared internal package if the implementer finds both the server-side T-015 codec and the client-side encoder are identical in logic. `NewProducer` should NOT fail if `DescribeTopic` returns `NOT_FOUND` — the topic may not exist yet when the producer is constructed; `metaFor` will fail at `Send` time.
