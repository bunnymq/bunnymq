# T-063: Structured logging — zap construction and cross-module audit

**Milestone:** M5 — Integration and polish
**Effort:** M
**Status:** TODO

## Goal

Wire `go.uber.org/zap` through the entire codebase: construct the root logger in `cmd/bunnymq`, pass child loggers to each module, and add all per-module log events listed in `09-metrics-logging.md §1.5`, ensuring uniform field names and JSON output.

## Context

Previous milestones added functionality without committing to a logging library. This ticket closes that gap: adds the `NewLogger` constructor, threads loggers through constructors, and audits every module to add the log events specified in the design doc. It is a cross-cutting change touching many files but not adding new logic.

References:
- [09-metrics-logging.md §1.1 — Library choice (zap)](../../design/09-metrics-logging.md#11-library-choice)
- [09-metrics-logging.md §1.2 — Output format](../../design/09-metrics-logging.md#12-output-format)
- [09-metrics-logging.md §1.3 — Root logger construction](../../design/09-metrics-logging.md#13-root-logger-construction)
- [09-metrics-logging.md §1.4 — Standard log fields](../../design/09-metrics-logging.md#14-standard-log-fields)
- [09-metrics-logging.md §1.5 — Per-module logging contracts](../../design/09-metrics-logging.md#15-per-module-logging-contracts)

## Scope

- Create `internal/observability/logger.go`:
  - `NewLogger(level zapcore.Level, development bool) (*zap.Logger, error)` — exact implementation from §1.3; RFC3339Nano `ts` key.
  - `NewNopLogger() *zap.Logger` — returns `zap.NewNop()` for use in tests.
- Modify `cmd/bunnymq/main.go`:
  - Call `NewLogger(level, dev)` at startup; pass to each subsystem constructor.
  - Log `info` event: `"bunnymq starting"` with `node_id`, `version`.
  - Log `info` event: `"bunnymq stopped"` on graceful exit.
- Propagate logger through constructors (add `*zap.Logger` parameter where missing):
  - `internal/storage.NewStorage(config, logger)` — add `logger *zap.Logger` param; child: `logger.With(zap.String("module","storage"), zap.String("topic",t), zap.Int32("partition_id",p))`.
  - `internal/cluster.NewClusterCoordinator(config, nh, dc, logger)`.
  - `internal/cluster.NewGroupCoordinator(config, nh, logger)`.
  - `internal/data.NewDataCoordinator(config, nh, logger)`.
  - `internal/api/management.NewServer(cc, logger)`.
  - `internal/api/data.NewServer(dc, gc, logger)`.
- Add log events per §1.5 table in each module:
  - **Storage**: segment rolled (info), segment deleted (info), storage opened (info), CRC mismatch (warn), fallocate fallback (warn), write failed (error), index msync failed (error), batch appended (debug), read served (debug).
  - **Raft/FSMs**: NodeHost started (info), shard started (info), leader changed (info), metadata command applied (info), snapshot saved/recovered (info), PartitionFSM opened (info), stale epoch rejected (warn), Storage.Append panic (error), entry proposed (debug).
  - **Server**: gRPC server started/stopped (info), metrics server started (info), unrecognized method (warn), handler returned error (error), RPC received (debug).
- Update `internal/api/logging/interceptor.go` (stub from T-035) to log `debug` on RPC receive and `error` on non-OK gRPC status using the zap logger injected at construction.

## Out of scope

- dragonboat internal logger adapter (route dragonboat's own logs) — not required for M5 DoD; document as future work.
- Metrics implementation — T-060, T-061, T-062.

## Definition of done

- [ ] `go build ./...` passes.
- [ ] `go test ./...` passes (tests use `NewNopLogger()`).
- [ ] `cmd/bunnymq` emits JSON to stdout; each line parses as valid JSON with `ts`, `level`, `msg`, `module` fields.
- [ ] Storage segment roll emits `info` log with `topic`, `partition_id`, `old_base_offset`, `new_base_offset`, `bytes_written`.
- [ ] gRPC handler error emits `error` log with `method`, `request_id`, `error`.
- [ ] `NewNopLogger()` used in all existing unit tests; no test imports `zap` except for logger setup.
- [ ] No global logger (`zap.L()`, `zap.S()`, `log.Print*`) used anywhere in `internal/` or `pkg/`.

## Tests required

- `TestNewLogger_JSONOutput` — construct logger with `zap.InfoLevel`; log one event; capture output; `json.Unmarshal` succeeds; `ts`, `level`, `msg`, `module` fields present.
- `TestNewLogger_LevelFilter` — construct at `zap.WarnLevel`; log at `Info`; no output emitted.
- `TestStorage_LogsSegmentRoll` — configure storage with observer logger (zaptest/observer); trigger segment roll; verify one `info` log entry with expected fields.
- `TestStorage_LogsCRCError` — inject corrupt batch; open storage; verify one `warn` log with `byte_position` field.
- `TestDataServer_LogsHandlerError` — gRPC handler stub returns error; interceptor emits `error` log with `method` and `error` fields.

## Dependencies

- All prior implementation tickets (adds logger param to constructors throughout the codebase).
- T-062 (logging interceptor wired alongside metrics interceptor).

## Notes

The `zaptest/observer` package (`go.uber.org/zap/zaptest/observer`) is the standard way to assert on log output in unit tests: `core, observed := observer.New(zap.DebugLevel); logger := zap.New(core)`. Use it rather than capturing stdout. When adding the logger parameter to constructors, use `*zap.Logger` (not a logger interface) — zap's `*zap.Logger` is already a value type with a no-op implementation (`zap.NewNop()`), so an interface would add no value.
