# T-062: Metrics — API metrics, metrics HTTP server, and pprof endpoint

**Milestone:** M5 — Integration and polish
**Effort:** M
**Status:** TODO

## Goal

Implement the four server-wide metrics from `09-metrics-logging.md §2.4.3` (build_info, uptime, gRPC request counters/histograms), add a Prometheus metrics HTTP server on `:9090`, and add an opt-in pprof HTTP server behind `--pprof-addr`.

## Context

The metrics HTTP endpoint is how the cluster's health is observed from outside the process. The gRPC interceptor-level metrics (`grpc_requests_total`, `grpc_request_duration_seconds`) provide per-method latency and error rates without each handler needing to record them individually. The pprof endpoint is essential during development and incidents. Both servers must share the same graceful shutdown path.

References:
- [09-metrics-logging.md §2.3 — Metrics HTTP endpoint](../../design/09-metrics-logging.md#23-http-endpoint)
- [09-metrics-logging.md §2.4.3 — Server-wide metrics](../../design/09-metrics-logging.md#243-server-wide-metrics)
- [09-metrics-logging.md §3 — pprof endpoint](../../design/09-metrics-logging.md#3-pprof-endpoint)
- [09-metrics-logging.md §4 — Configuration parameters](../../design/09-metrics-logging.md#4-configuration-parameters)

## Scope

- Create `internal/api/metrics_server.go`:
  - `MetricsServer` struct: `addr string`, `registry *prometheus.Registry`, `srv *http.Server`.
  - `NewMetricsServer(addr string, registry *prometheus.Registry) *MetricsServer`.
  - `Start() error` — listen on `addr`; serve `GET /metrics` via `promhttp.HandlerFor(registry, promhttp.HandlerOpts{})` with 5 s read/write timeouts.
  - `Stop(ctx context.Context) error` — graceful `srv.Shutdown(ctx)`.
- Create `internal/api/pprof_server.go`:
  - `PprofServer` struct: `addr string`, `srv *http.Server`.
  - `NewPprofServer(addr string) (*PprofServer, error)` — returns error if `addr` starts with `0.0.0.0` (public bind guard; log a `warn` and return error per §3).
  - `Start() error` — register standard `net/http/pprof` handlers; listen on `addr`.
  - `Stop(ctx context.Context) error` — graceful shutdown.
- Modify `internal/api/logging/interceptor.go` (T-035's stub) — promote to full implementation:
  - `ServerMetricsInterceptor(metrics *ServerMetrics) grpc.UnaryServerInterceptor` — records `bunnymq_grpc_requests_total{method, code}` and `bunnymq_grpc_request_duration_seconds{method}` for each handled RPC.
- Create `internal/api/server_metrics.go`:
  - `ServerMetrics` struct:
    - `GRPCRequestsTotal *prometheus.CounterVec` — labels: `method`, `code`
    - `GRPCRequestDuration *prometheus.HistogramVec` — label: `method`; buckets: 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s
    - `BuildInfo *prometheus.GaugeVec` — labels: `version`, `go_version`, `node_id`, `commit`
    - `UptimeSeconds prometheus.Gauge`
  - `NewServerMetrics(reg prometheus.Registerer) *ServerMetrics`.
  - `RecordBuildInfo(version, goVersion, nodeID, commit string)` — sets `BuildInfo` to 1.0 with provided labels.
  - `StartUptimeTicker(ctx context.Context, startTime time.Time)` — goroutine updating `UptimeSeconds` every 10 s.
- Modify `cmd/bunnymq/main.go`:
  - Create `prometheus.NewRegistry()`; register `collectors.NewGoCollector()` + `collectors.NewProcessCollector()`.
  - Instantiate `ServerMetrics`, `StorageMetrics`, `RaftMetrics`; pass to constructors.
  - Start `MetricsServer` on `--metrics-addr` (default `:9090`).
  - Start `PprofServer` if `--pprof-addr` is set.
  - Call `RecordBuildInfo` + `StartUptimeTicker` at startup.
  - Add both servers to graceful shutdown sequence.

## Out of scope

- Storage metrics wiring — T-060.
- Raft metrics wiring — T-061.
- Logging implementation — T-063.

## Definition of done

- [ ] `go build ./...` passes.
- [ ] `go test ./internal/api/...` passes.
- [ ] `GET http://localhost:9090/metrics` returns 200 with Prometheus text format.
- [ ] Response includes `bunnymq_build_info` with expected labels.
- [ ] Response includes `go_goroutines` (Go collector registered).
- [ ] `bunnymq_grpc_requests_total` and `bunnymq_grpc_request_duration_seconds` populated after gRPC call.
- [ ] `PprofServer` with `addr=0.0.0.0:6060` returns error at construction.
- [ ] `PprofServer` with `addr=127.0.0.1:6060`: `GET /debug/pprof/` returns 200.
- [ ] `MetricsServer.Stop` completes within 5 s.

## Tests required

- `TestMetricsServer_ServeMetrics` — start server on random port; `GET /metrics`; response code 200 and body contains `go_goroutines`.
- `TestMetricsServer_BuildInfo` — `RecordBuildInfo` called; scrape response contains `bunnymq_build_info{version="test"}`.
- `TestServerMetricsInterceptor_RecordsRequest` — invoke interceptor with a fake handler returning `codes.OK`; `grpc_requests_total{code="OK"}` incremented; `grpc_request_duration_seconds` has observation.
- `TestServerMetricsInterceptor_RecordsError` — handler returns `codes.NotFound`; `grpc_requests_total{code="NotFound"}` incremented.
- `TestPprofServer_PublicBindRejected` — `NewPprofServer("0.0.0.0:6060")` returns non-nil error.
- `TestPprofServer_LoopbackOK` — `NewPprofServer("127.0.0.1:0")`; `Start()`; `GET /debug/pprof/` returns 200.

## Dependencies

- T-035 (logging interceptor stub to promote; auth interceptor chain to extend with metrics interceptor).
- T-060 (StorageMetrics — passed to MetricsServer registry).
- T-061 (RaftMetrics — passed to MetricsServer registry).
- T-010 (`cmd/bunnymq` main to wire everything into).

## Notes

The `bunnymq_build_info` gauge label set includes `node_id` as a string, not a numeric Prometheus label — this is the standard pattern for process-level metadata. The `version` value comes from a build-time `ldflags` injection: `go build -ldflags "-X main.version=v0.1.0"`. Add a `var version = "dev"` sentinel in `cmd/bunnymq/main.go`. The gRPC metrics interceptor must be inserted into the interceptor chain (T-035) between the logging and handler interceptors: Auth → Logging → **Metrics** → Handler.
