# T-043: Client library — ConnPool, MetaCache, and retry helper

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement `internal/client` helpers: `ConnPool` (lazy gRPC connection pool), `MetaCache` (per-topic leader cache), and the `retry` loop with exponential backoff — all shared by `Producer`, `Consumer`, and `AdminClient`.

## Context

All three client types share the same infrastructure: one connection pool keyed by broker address, a topic metadata cache (partition count + per-partition leader address) with TTL, and a retry loop that handles `NOT_LEADER` (immediate retry with cache update), `UNAVAILABLE`/`TIMEOUT` (backoff retry), and non-retryable errors (immediate fail). These helpers live in `internal/client` and are not exported.

References:
- [07-client-library.md §2 — Common config and retry policy](../../design/07-client-library.md#2-common-configuration)
- [07-client-library.md §3 — Connection pool](../../design/07-client-library.md#3-connection-management)
- [07-client-library.md §4 — Metadata cache](../../design/07-client-library.md#4-metadata-cache)

## Scope

- Create `internal/client/connpool.go`:
  - `ConnPool` struct: `mu sync.RWMutex`, `conns map[string]*grpc.ClientConn`, `opts []grpc.DialOption`.
  - `NewConnPool(opts ...grpc.DialOption) *ConnPool`.
  - `Get(addr string) (*grpc.ClientConn, error)` — double-checked locking; lazy `grpc.NewClient`.
  - `Close() error` — close all connections; nil-out map.
- Create `internal/client/metacache.go`:
  - `TopicMeta` struct: `PartitionCount int32`, `Leaders map[int32]string`, `FetchedAt time.Time`.
  - `MetaCache` struct: `mu sync.RWMutex`, `cache map[string]*TopicMeta`, `ttl time.Duration`.
  - `Get(topic string) *TopicMeta` — returns nil if absent or `time.Since(FetchedAt) > ttl`.
  - `Put(topic string, meta *TopicMeta)`.
  - `SetLeader(topic string, partitionID int32, addr string)` — updates one partition leader without full invalidation.
  - `Invalidate(topic string)` — deletes the entry.
- Create `internal/client/retry.go`:
  - `RetryPolicy` struct (mirrors `pkg/client.RetryPolicy`).
  - `extractBunnyError(err error) (*pb.BunnyErrorDetail, *pb.NotLeaderDetail)` — reads `Status.Details()`.
  - `backoffDuration(attempt int, policy RetryPolicy) time.Duration` — `min(InitialBackoff * BackoffFactor^attempt, MaxBackoff)`.
  - `isRetryable(code pb.BunnyErrorCode) bool` — true for `NOT_LEADER`, `UNAVAILABLE`, `TIMEOUT`.
- Create `pkg/client/config.go`: export `Config` and `RetryPolicy` types.

## Out of scope

- Producer, Consumer, AdminClient — T-044, T-045, T-046.
- Batch encoder/decoder — part of T-015 (server-side); re-use the same package from `internal/` for client-side.

## Definition of done

- [ ] `go build ./internal/client/...` and `go build ./pkg/client/...` pass.
- [ ] `go test ./internal/client/...` passes.
- [ ] `ConnPool.Get` returns the same `*grpc.ClientConn` on second call to same addr.
- [ ] `ConnPool.Close` closes all connections; subsequent `Get` after Close returns error.
- [ ] `MetaCache.Get` returns nil after TTL expires.
- [ ] `MetaCache.SetLeader` updates one partition's leader without resetting `FetchedAt`.
- [ ] `backoffDuration` caps at `MaxBackoff` after sufficient attempts.

## Tests required

- `TestConnPool_LazyConnect` — first `Get` establishes connection; second `Get` returns same pointer.
- `TestConnPool_DoubleChecked` — concurrent `Get` for same addr → single connection created (verify with mock dial that counts calls).
- `TestConnPool_Close` — `Close`; all connections closed.
- `TestMetaCache_TTLExpiry` — `Put` with `FetchedAt = time.Now().Add(-ttl-1s)`; `Get` returns nil.
- `TestMetaCache_SetLeader` — `Put` full meta; `SetLeader` updates partition 1 leader; other partitions unchanged; `FetchedAt` unchanged.
- `TestMetaCache_Invalidate` — `Put`; `Invalidate`; `Get` returns nil.
- `TestBackoffDuration_Caps` — 10 attempts; result ≤ `MaxBackoff`.
- `TestExtractBunnyError_NotLeader` — gRPC status with `BunnyErrorDetail` + `NotLeaderDetail`; both extracted correctly.

## Dependencies

T-034 (proto stubs for `BunnyErrorDetail`, `NotLeaderDetail`).
T-001 (go.mod with grpc dependency).

## Notes

`grpc.NewClient` (replacing the deprecated `grpc.Dial`) is non-blocking — the TCP connection is established on the first RPC. The `ConnPool` does not need explicit reconnect logic; gRPC's internal channel state machine handles it. Keepalive options (`keepalive.ClientParameters{Time: 30s, Timeout: 10s}`) should be applied via `grpc.WithKeepaliveParams` in `NewConnPool`'s default options so callers don't have to remember them.
