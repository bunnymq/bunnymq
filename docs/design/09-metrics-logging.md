# 09 — Metrics & Logging

BunnyMQ's observability layer has two independent concerns: structured logging (per-event textual narrative) and metrics (numeric time-series). Logging uses `go.uber.org/zap` emitting newline-delimited JSON to stdout; the process supervisor or container runtime is responsible for capturing and shipping log output. Metrics use `github.com/prometheus/client_golang` and are scraped via a dedicated HTTP endpoint; no push gateway is used. A `net/http/pprof` endpoint is available behind an opt-in flag for CPU and heap profiling during development and incident investigation.

---

## 1. Logging

### 1.1 Library choice

`go.uber.org/zap` — specifically the `zap.Logger` type, not the `sugaredLogger` wrapper. Rationale:

- Allocates zero heap objects per log call at `Info` and above when the level is enabled (important on the hot `Update()` path in the Partition FSM, though logging there is disabled at `Debug`).
- Structured fields are typed (`zap.String`, `zap.Int64`, etc.), not `any`, so field encoding is inlined without reflection.
- JSON output is production-standard; `zap` produces RFC 3339 nano timestamps and respects log-level filtering atomically (level can be changed at runtime via `zap.AtomicLevel`).

Alternative (`zerolog`) is comparable in performance but has a smaller ecosystem and less precedent in Go infrastructure projects.

### 1.2 Output format

JSON, one object per line, written to `os.Stdout`. Production deployments capture stdout via the container runtime (Docker, Kubernetes). No file rotation is handled by the process itself.

Example output:

```json
{"ts":"2026-04-27T14:00:00.000000001Z","level":"info","module":"storage","msg":"segment rolled","topic":"orders","partition_id":3,"shard_id":4,"old_base_offset":0,"new_base_offset":131072,"bytes_written":134217728}
```

### 1.3 Root logger construction

A single `*zap.Logger` is constructed at process start and passed through the dependency graph via constructor parameters. No global logger is used (global loggers create hidden dependencies and complicate testing).

```go
func NewLogger(level zapcore.Level, development bool) (*zap.Logger, error) {
    cfg := zap.NewProductionConfig()
    cfg.Level = zap.NewAtomicLevelAt(level)
    cfg.EncoderConfig.TimeKey = "ts"
    cfg.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
    return cfg.Build()
}
```

Each module receives a child logger scoped with its name:

```go
storageLog := rootLogger.With(zap.String("module", "storage"))
raftLog    := rootLogger.With(zap.String("module", "raft"))
```

### 1.4 Standard log fields

Every log event must include these fields. Module-scope fields (`module`, `node_id`) are attached once at logger construction. Request-scope fields are attached at the call site.

| Field | Type | When present | Description |
|---|---|---|---|
| `ts` | RFC3339Nano string | always | UTC timestamp of the event |
| `level` | string | always | `debug`, `info`, `warn`, `error` |
| `module` | string | always | Owning subsystem: `storage`, `raft`, `server` |
| `msg` | string | always | Human-readable event description |
| `node_id` | uint64 | always | dragonboat NodeID of this process |
| `topic` | string | where applicable | Topic name; omitted for module-level events |
| `partition_id` | int32 | where applicable | Partition index within topic |
| `shard_id` | uint64 | where applicable | dragonboat shard ID for this partition |
| `request_id` | string | where applicable | Propagated from gRPC metadata; omitted if absent |
| `error` | string | on warn/error | `err.Error()` string |

### 1.5 Per-module logging contracts

#### Storage

| Level | Event | Key fields |
|---|---|---|
| `info` | Segment rolled (new active segment created) | `topic`, `partition_id`, `old_base_offset`, `new_base_offset`, `bytes_written` |
| `info` | Segment deleted by retention | `topic`, `partition_id`, `base_offset`, `reason` (`time` or `bytes`) |
| `info` | Storage opened on startup | `topic`, `partition_id`, `segment_count`, `earliest_offset`, `latest_offset` |
| `warn` | CRC mismatch during crash recovery scan | `topic`, `partition_id`, `byte_position`, `truncated_to` |
| `warn` | fallocate not supported; fell back to write | `topic`, `partition_id` |
| `error` | Log write failed | `topic`, `partition_id`, `error` |
| `error` | Index msync failed on segment seal | `topic`, `partition_id`, `error` |
| `debug` | Individual batch appended | `topic`, `partition_id`, `base_offset`, `batch_bytes` |
| `debug` | Read request served | `topic`, `partition_id`, `offset`, `bytes_returned` |

`debug` events on the append/read path are gated by log level and add zero cost when the level is `info` or above (zap checks the level atomically before constructing any fields).

#### Raft / FSMs

| Level | Event | Key fields |
|---|---|---|
| `info` | NodeHost started | `node_id`, `listen_addr`, `data_dir` |
| `info` | Shard started | `shard_id`, `initial_members` |
| `info` | Leader changed for shard | `shard_id`, `new_leader_node_id`, `term` |
| `info` | Metadata command applied | `command_type`, `topic` (where applicable) |
| `info` | Snapshot saved (metadata FSM) | `shard_id`, `snapshot_index`, `bytes` |
| `info` | Snapshot recovered (metadata FSM) | `shard_id`, `snapshot_index` |
| `info` | PartitionFSM opened | `shard_id`, `topic`, `partition_id`, `last_applied_index` |
| `warn` | AssignPartitionLeader rejected: stale epoch | `shard_id`, `current_epoch`, `received_epoch` |
| `error` | Storage.Append failed inside Update() — panicking | `shard_id`, `topic`, `partition_id`, `error` |
| `debug` | Raft entry proposed | `shard_id`, `client_id` |

dragonboat's own internal logging is passed through a dragonboat `ILogger` adapter that routes at `debug` level; it is suppressed in production by setting the root level to `info`.

#### Server / gRPC layer

| Level | Event | Key fields |
|---|---|---|
| `info` | gRPC server started | `listen_addr` |
| `info` | gRPC server stopped | |
| `info` | Metrics HTTP server started | `metrics_addr` |
| `warn` | Unrecognized RPC method called | `method` |
| `error` | gRPC handler returned error | `method`, `request_id`, `error` |
| `debug` | RPC request received | `method`, `request_id` |

---

## 2. Metrics

### 2.1 Library

`github.com/prometheus/client_golang` — the official Go Prometheus client. Specifically:

- `prometheus.NewRegistry()` — a non-global registry. The default global registry (`prometheus.DefaultRegisterer`) is not used; all metrics are registered against an explicit registry so that tests can instantiate isolated registries without cross-contamination.
- `promhttp.HandlerFor(registry, promhttp.HandlerOpts{})` — serves `/metrics` from the explicit registry.

### 2.2 Registry ownership

The `Server` struct (the process entry point) owns the `*prometheus.Registry`. It passes the registry to each subsystem constructor, which registers its own metrics at construction time. No subsystem stores a reference to the registry after construction; metrics are manipulated only via the typed metric objects returned by `prometheus.MustRegister`.

```go
reg := prometheus.NewRegistry()
reg.MustRegister(collectors.NewGoCollector())
reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

storageMetrics := storage.NewMetrics(reg)
raftMetrics    := raft.NewMetrics(reg)
```

Each module defines a `Metrics` struct with exported fields for each metric object, and a `NewMetrics(reg prometheus.Registerer) *Metrics` constructor. This keeps metric registration co-located with the module that uses each metric.

### 2.3 HTTP endpoint

The `/metrics` endpoint is served on a separate port from the gRPC listener, configured by `--metrics-addr` (default: `:9090`). It must never be exposed on the same port as the gRPC API to prevent accidental public exposure.

The metrics server is a plain `net/http.Server` with a single route: `GET /metrics`. It has a 5-second read/write timeout. It shuts down gracefully when the main server receives a termination signal.

```
GET http://<metrics-addr>/metrics   → Prometheus text exposition format
```

### 2.4 Metric catalog

Label cardinality rule: only fixed-cardinality labels are used on high-frequency metrics (e.g., `topic` and `partition_id` on per-segment counters). `request_id` is never a label. `node_id` is a process-level constant exposed via the `build_info` gauge, not a per-metric label.

#### 2.4.1 Storage metrics

All storage metrics are labeled `{topic, partition_id}` unless noted.

| Metric name | Type | Labels | Description |
|---|---|---|---|
| `bunnymq_storage_bytes_appended_total` | Counter | topic, partition_id | Total bytes written to the log (raw batch bytes, excluding batch header) |
| `bunnymq_storage_batches_appended_total` | Counter | topic, partition_id | Total number of batches successfully appended |
| `bunnymq_storage_segments_rolled_total` | Counter | topic, partition_id | Number of times the active segment has been rolled |
| `bunnymq_storage_segments_deleted_total` | Counter | topic, partition_id, reason | Segments deleted by retention; `reason` is `time` or `bytes` |
| `bunnymq_storage_active_segment_bytes` | Gauge | topic, partition_id | Current size of the active `.log` file in bytes |
| `bunnymq_storage_segment_count` | Gauge | topic, partition_id | Total number of segments (sealed + active) |
| `bunnymq_storage_earliest_offset` | Gauge | topic, partition_id | Earliest available offset after retention enforcement |
| `bunnymq_storage_latest_offset` | Gauge | topic, partition_id | Latest written offset |
| `bunnymq_storage_append_duration_seconds` | Histogram | topic, partition_id | Wall time for a single `Append()` call; buckets: 50µs, 100µs, 250µs, 500µs, 1ms, 5ms, 10ms |
| `bunnymq_storage_read_duration_seconds` | Histogram | topic, partition_id | Wall time for a single `Read()` call; same buckets |
| `bunnymq_storage_recovery_duration_seconds` | Histogram | topic, partition_id | Wall time for `Storage.Open()` (crash recovery); buckets: 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s |
| `bunnymq_storage_crc_errors_total` | Counter | topic, partition_id | Batches with invalid CRC-32C found during crash recovery |

#### 2.4.2 Raft metrics

Labeled `{shard_id}` unless noted.

| Metric name | Type | Labels | Description |
|---|---|---|---|
| `bunnymq_raft_committed_index` | Gauge | shard_id | Last committed Raft log index for this shard |
| `bunnymq_raft_applied_index` | Gauge | shard_id | Last applied Raft log index for this shard |
| `bunnymq_raft_is_leader` | Gauge | shard_id | 1 if this node is the current leader for the shard, 0 otherwise |
| `bunnymq_raft_term` | Gauge | shard_id | Current Raft term for this shard |
| `bunnymq_raft_propose_duration_seconds` | Histogram | shard_id, acks | Wall time for a propose round-trip; `acks` label: `all` (SyncPropose) or `zero` (Propose); buckets: 500µs, 1ms, 2ms, 5ms, 10ms, 50ms, 100ms, 500ms |
| `bunnymq_raft_snapshot_save_duration_seconds` | Histogram | shard_id | Time to serialize and write a snapshot |
| `bunnymq_raft_snapshot_recover_duration_seconds` | Histogram | shard_id | Time to install a received snapshot |
| `bunnymq_raft_leader_changes_total` | Counter | shard_id | Number of leadership changes observed on this node |
| `bunnymq_raft_fsm_update_duration_seconds` | Histogram | shard_id | Time spent inside `Update()` (includes Storage.Append); buckets same as propose |

#### 2.4.3 Server-wide metrics

| Metric name | Type | Labels | Description |
|---|---|---|---|
| `bunnymq_build_info` | Gauge | version, go_version, node_id, commit | Always 1; carries build metadata as labels |
| `bunnymq_uptime_seconds` | Gauge | — | Seconds since the process started |
| `bunnymq_grpc_requests_total` | Counter | method, code | RPC requests by method and gRPC status code |
| `bunnymq_grpc_request_duration_seconds` | Histogram | method | End-to-end handler latency; buckets: 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s |

`bunnymq_build_info` is set to `1.0` once at startup; its labels carry the version string, Go runtime version, node ID, and git commit hash. This is the standard Prometheus pattern for process metadata.

Standard Go runtime and process metrics (`go_goroutines`, `go_memstats_*`, `process_cpu_seconds_total`, etc.) are registered via `collectors.NewGoCollector()` and `collectors.NewProcessCollector()` on the same registry.

---

## 3. pprof endpoint

The pprof HTTP endpoint is disabled by default and enabled with the `--pprof-addr` flag (e.g., `--pprof-addr=127.0.0.1:6060`). When enabled, a plain `net/http` server registers the standard `net/http/pprof` handlers:

```
GET /debug/pprof/             → index
GET /debug/pprof/cmdline
GET /debug/pprof/profile      → 30-second CPU profile (default)
GET /debug/pprof/symbol
GET /debug/pprof/trace
```

The pprof server must be bound to a loopback or private address only. It must never be reachable from the public network. If `--pprof-addr` is set to `0.0.0.0`, the process logs a `warn` event and refuses to start.

The pprof server uses the same graceful shutdown path as the metrics server.

---

## 4. Configuration parameters

| Parameter | Default | Description |
|---|---|---|
| `log.level` | `info` | Minimum log level: `debug`, `info`, `warn`, `error` |
| `log.development` | `false` | If true, use zap development encoder (colored, non-JSON) |
| `metrics.addr` | `:9090` | Bind address for `/metrics` HTTP endpoint |
| `metrics.read_timeout_ms` | `5000` | HTTP server read timeout in milliseconds |
| `metrics.write_timeout_ms` | `5000` | HTTP server write timeout in milliseconds |
| `pprof.addr` | `""` (disabled) | Bind address for pprof HTTP endpoint; empty means disabled |

---

## 5. Open questions

1. **Histogram bucket boundaries:** The buckets listed for `append_duration_seconds` and `propose_duration_seconds` are estimates. They should be validated against benchmark data once the implementation phase begins; the bucket set can be changed without breaking the API.
2. **Label cardinality at scale:** With 10 000 partitions, `{topic, partition_id}` label pairs produce 10 000 time series per metric. Prometheus handles this without issue at course-project scale, but at production scale a roll-up metric (per-topic total) is preferable for the high-frequency counters. No design change is needed for v1.
3. **dragonboat internal metrics:** dragonboat exposes some internal Prometheus metrics of its own. Whether to include them on the same registry (potential name collision) or isolate them needs verification against the dragonboat v4 API.

---

*Module 3/3 complete: Metrics & Logging*
