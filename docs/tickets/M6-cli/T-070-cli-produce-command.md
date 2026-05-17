# T-070: CLI — produce command

**Milestone:** M6 — CLI
**Effort:** S
**Status:** TODO

## Goal

Implement a `produce` cobra command in `cmd/bunnymq-cli/` that sends one message to a topic using `Producer.Send`, analogous to `kafka-console-producer.sh`.

## Context

With admin commands in place (T-069), operators need a way to push test messages from the shell. The produce command accepts a key and value as flags or reads value lines from stdin for batch sending. It prints the assigned offset (or a confirmation for `acks=0`) to stdout.

References:
- `pkg/client/producer.go` — `Producer.Send`, `Producer.Close`
- `pkg/client/config.go` — `ProducerConfig`, `AcksMode`, `AcksAll`, `AcksZero`
- `cmd/bunnymq-cli/flags.go` — global `buildClientConfig` helper (from T-069)

## Scope

- **Create** `cmd/bunnymq-cli/produce.go` — `package main`:
  - `produceCmd` — `cobra.Command{Use: "produce", Short: "Send a message to a topic"}`.
  - Flags:
    - `--topic` string (required) — target topic name.
    - `--key` string (default `""`) — message key; empty string means nil key (triggers round-robin partition selection in the producer).
    - `--value` string (default `""`) — message value; if empty, the command reads lines from stdin and sends each line as a separate message.
    - `--acks` string (default `"all"`) — acknowledgement mode; accepted values: `"all"` → `client.AcksAll`, `"zero"` → `client.AcksZero`; any other value is a usage error.
  - `parseAcks(s string) (client.AcksMode, error)` — pure function converting the flag string to `AcksMode`.
  - Behaviour:
    1. Parse `--acks` with `parseAcks`; return usage error on unknown value.
    2. Build `client.ProducerConfig{Config: buildClientConfig()}`.
    3. Call `client.NewProducer`; defer `Close`.
    4. If `--value` is non-empty: call `Producer.Send` once with `[]byte(flagKey)` / `[]byte(flagValue)` and print `"offset: <n>"` (or `"sent (acks=0)"` when offset == -1).
    5. If `--value` is empty: read from `os.Stdin` line by line (`bufio.Scanner`); for each non-empty line call `Producer.Send` and print the result; stop on EOF or context cancellation.
  - `init()` registers `produceCmd` onto `rootCmd`.

- **Create** `cmd/bunnymq-cli/produce_test.go` — `package main`:
  - Unit tests for `parseAcks`.

## Out of scope

- `--partition` flag for explicit partition targeting — the producer's key-based routing is sufficient for v1.
- Batch file input (e.g., `--file`) — stdin line-by-line covers the use case.
- Headers support via flag.

## Definition of done

- [ ] `go build ./cmd/bunnymq-cli/...` passes.
- [ ] `go test ./cmd/bunnymq-cli/...` passes.
- [ ] `echo "hello" | bunnymq-cli produce --topic foo` exits 0 against a live cluster and prints `offset: 0`.
- [ ] `bunnymq-cli produce --topic foo --value "hi" --acks zero` exits 0 and prints `sent (acks=0)`.
- [ ] `bunnymq-cli produce --topic foo --acks bad` exits non-zero with a usage message.
- [ ] Missing `--topic` flag exits non-zero with a usage message.

## Tests required

- `TestParseAcks` — table-driven: `"all"` → `(AcksAll, nil)`, `"zero"` → `(AcksZero, nil)`, `""` → error, `"1"` → error, `"ALL"` → error (case-sensitive).

## Dependencies

- T-069 (global flags and `buildClientConfig` helper must exist in `cmd/bunnymq-cli/flags.go`).
- T-044 (Producer — `pkg/client/producer.go` must exist and compile).

## Notes

Key must be passed as `[]byte(flagKey)` — pass `nil` only when the flag value is the empty string, so that the producer's key-based hash is bypassed in favour of round-robin. Use a `bufio.Scanner` with the default `ScanLines` split function for stdin; the scanner's default buffer is sufficient for typical message sizes. Do not set `ProducerConfig.DefaultAcks` — always pass the parsed acks mode directly to `Send`.
