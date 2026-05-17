# T-071: CLI — consume command

**Milestone:** M6 — CLI
**Effort:** M
**Status:** TODO

## Goal

Implement a `consume` cobra command in `cmd/bunnymq-cli/` that fetches records from a topic and prints them as JSON to stdout, analogous to `kafka-console-consumer.sh`. Supports both manual mode (explicit partition + offset) and consumer-group mode.

## Context

With produce working (T-070), operators need a symmetric way to read records from the shell. The consume command runs until `--count` records have been printed or the process is interrupted with SIGINT/SIGTERM. In group mode it issues `Subscribe` and lets the broker assign partitions; in manual mode it calls `Seek` directly.

References:
- `pkg/client/consumer.go` — `Consumer.Subscribe`, `Consumer.Seek`, `Consumer.Poll`, `Consumer.Commit`, `Consumer.Close`
- `pkg/client/record.go` — `Record`, `TP`, `OffsetResetEarliest`, `OffsetResetLatest`
- `pkg/client/config.go` — `ConsumerConfig`
- `cmd/bunnymq-cli/flags.go` — global `buildClientConfig` helper (from T-069)

## Scope

- **Create** `cmd/bunnymq-cli/consume.go` — `package main`:
  - `consumeCmd` — `cobra.Command{Use: "consume", Short: "Read messages from a topic"}`.
  - Flags:
    - `--topic` string (required) — topic to consume.
    - `--partition` int32 (default `-1`) — partition to read; `-1` means all partitions in group mode; required when `--group` is not set.
    - `--offset` int64 (default `0`) — starting offset for manual mode (ignored in group mode).
    - `--group` string (default `""`) — consumer group ID; when set, enables group mode.
    - `--offset-reset` string (default `"earliest"`) — group mode only; `"earliest"` or `"latest"`.
    - `--count` int (default `0`) — stop after this many records; `0` means consume indefinitely.
    - `--max-wait-ms` int64 (default `500`) — max wait per `Poll` call.
  - `formatRecord(r client.Record) ([]byte, error)` — marshals the record to a JSON object with fields:
    ```json
    {"topic":"…","partition":0,"offset":0,"key":"…","value":"…","headers":{"k":"v"},"timestamp_ms":0}
    ```
    `key`, `value`, and header values are base64-encoded when they contain non-UTF-8 bytes (use `encoding/json` default behaviour for `[]byte`).
  - Behaviour:
    1. Validate: if `--group` is empty and `--partition` is -1, return usage error ("--partition is required in manual mode").
    2. Build `client.ConsumerConfig{Config: buildClientConfig(), GroupID: flagGroup, MaxFetchWaitMs: flagMaxWaitMs, AutoOffsetReset: parseOffsetReset(flagOffsetReset)}`.
    3. Call `client.NewConsumer`; defer `Close`.
    4. **Group mode** (`flagGroup != ""`): call `consumer.Subscribe([]string{flagTopic})`.
    5. **Manual mode**: call `consumer.Seek(flagTopic, flagPartition, flagOffset)`.
    6. Install a signal handler on `SIGINT`/`SIGTERM` that cancels a context.
    7. Poll loop: call `consumer.Poll(ctx, flagMaxWaitMs)` in a loop; for each returned record call `formatRecord` and write the JSON line + `"\n"` to `os.Stdout`; increment a counter; break when counter == `flagCount` (if `flagCount > 0`) or `ctx` is done.
    8. In group mode: call `consumer.Commit(ctx)` before returning.
  - `parseOffsetReset(s string) client.OffsetResetPolicy` — `"earliest"` → `client.OffsetResetEarliest`, anything else → `client.OffsetResetLatest`.
  - `init()` registers `consumeCmd` onto `rootCmd`.

- **Create** `cmd/bunnymq-cli/consume_test.go` — `package main`:
  - Unit tests for `formatRecord` and `parseOffsetReset`.

## Out of scope

- Committing on every record — a single `Commit` at the end is sufficient for a CLI tool.
- `--from-beginning` shorthand flag (use `--offset 0` instead).
- Output formats other than JSON-per-line.

## Definition of done

- [ ] `go build ./cmd/bunnymq-cli/...` passes.
- [ ] `go test ./cmd/bunnymq-cli/...` passes.
- [ ] `bunnymq-cli consume --topic foo --partition 0 --count 1` against a live cluster (with a previously produced record) prints one valid JSON line and exits 0.
- [ ] `bunnymq-cli consume --topic foo --group mygroup --count 5` exits 0 after printing 5 records.
- [ ] `bunnymq-cli consume --topic foo` (no `--partition`, no `--group`) exits non-zero with a usage error.
- [ ] `Ctrl-C` terminates the consume loop cleanly (no panic, non-zero exit acceptable).

## Tests required

- `TestFormatRecord_UTF8` — creates a `client.Record` with a plain UTF-8 key and value; calls `formatRecord`; asserts the JSON contains `"topic"`, `"partition"`, `"offset"`, `"value"` fields with the expected string values.
- `TestFormatRecord_BinaryValue` — creates a `client.Record` with a binary (non-UTF-8) value; calls `formatRecord`; asserts the result is valid JSON and the `"value"` field is a base64 string (i.e., `json.Unmarshal` into `map[string]interface{}` and check `value` is a string that round-trips through `base64.StdEncoding.DecodeString`).
- `TestParseOffsetReset` — table-driven: `"earliest"` → `OffsetResetEarliest`, `"latest"` → `OffsetResetLatest`, `""` → `OffsetResetLatest`, `"EARLIEST"` → `OffsetResetLatest` (case-sensitive; unknown values fall back to latest).

## Dependencies

- T-069 (global flags and `buildClientConfig` helper must exist in `cmd/bunnymq-cli/flags.go`).
- T-046 (Consumer manual mode — `pkg/client/consumer.go` must exist and compile).
- T-055 (Consumer group subscribe/poll — required for group mode).

## Notes

`encoding/json` marshals `[]byte` fields as base64 automatically, so `formatRecord` needs no special handling for binary payloads beyond marshalling the struct directly. Use `os/signal.NotifyContext` (Go 1.16+) to wire SIGINT/SIGTERM into the poll loop context cleanly — no manual signal channel needed. The signal handler must be set up before the first `Poll` call, not after, to avoid a window where a signal is missed. In manual mode with `--count 0`, the command runs until interrupted; document this in the command's `Long` description.
