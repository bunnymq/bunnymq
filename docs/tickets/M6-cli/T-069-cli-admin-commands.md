# T-069: CLI — topic and cluster admin commands

**Milestone:** M6 — CLI
**Effort:** M
**Status:** TODO

## Goal

Implement the `topic` and `cluster` cobra subcommand trees in `cmd/bunnymq-cli/` so that operators can perform all `AdminClient` operations from the shell, analogous to `kafka-topics.sh` and `kafka-broker-api-versions.sh`.

## Context

The CLI binary already has a cobra root command but no subcommands. This ticket adds the global connection flags shared by all future commands, a helper that constructs an `AdminClient` from those flags, and all admin operations: topic create/delete/list/describe/alter-partitions/alter-retention/list-partitions and cluster describe.

References:
- `pkg/client/admin.go` — `AdminClient` methods
- `pkg/client/types.go` — `TopicInfo`, `TopicDescription`, `ClusterDescription`, `PartitionInfoWithOffsets`
- `pkg/client/config.go` — `Config`, `RetryPolicy`

## Scope

- **Modify** `cmd/bunnymq-cli/main.go` — register `topicCmd` and `clusterCmd` via `rootCmd.AddCommand` in `init()`.

- **Create** `cmd/bunnymq-cli/flags.go` — `package main`:
  - Package-level flag variables used across all command files:
    ```go
    var (
        flagBrokers string        // "--brokers", default "localhost:9091"
        flagToken   string        // "--token",   default ""
        flagTimeout time.Duration // "--timeout", default 10s
    )
    ```
  - `init()` that calls `rootCmd.PersistentFlags().StringVar(...)` for each.
  - `buildClientConfig() client.Config` — splits `flagBrokers` on commas, populates `Config{BootstrapServers, AuthToken, RequestTimeout}`.
  - `newAdminClient() (*client.AdminClient, error)` — calls `buildClientConfig` then `client.NewAdminClient`.

- **Create** `cmd/bunnymq-cli/topic.go` — `package main`:
  - `topicCmd` — `cobra.Command{Use: "topic", Short: "Manage topics"}`.
  - `topicCreateCmd` — flags: `--name` (required), `--partitions` int32 (default 1), `--replication-factor` int32 (default 1), `--retention-ms` int64 (default -1), `--retention-bytes` int64 (default -1). Calls `AdminClient.CreateTopic`; prints the returned `TopicInfo` via `printTopicInfo`.
  - `topicDeleteCmd` — flag: `--name` (required). Calls `AdminClient.DeleteTopic`; prints `"deleted: <name>"`.
  - `topicListCmd` — no extra flags. Calls `AdminClient.ListTopics`; prints a tab-aligned table via `printTopicTable`.
  - `topicDescribeCmd` — flag: `--name` (required). Calls `AdminClient.DescribeTopic`; prints detailed view via `printTopicDescription`.
  - `topicAlterPartitionsCmd` — flags: `--name` (required), `--partitions` int32 (required). Calls `AdminClient.AlterTopicPartitions`; prints `"updated: <name>"`.
  - `topicAlterRetentionCmd` — flags: `--name` (required), `--retention-ms` int64, `--retention-bytes` int64. Calls `AdminClient.AlterTopicRetention`; prints `"updated: <name>"`.
  - `topicListPartitionsCmd` — flag: `--name` (required). Calls `AdminClient.ListPartitions`; prints a tab-aligned table via `printPartitionTable`.
  - Formatting helpers (all write to `io.Writer` for testability):
    - `printTopicInfo(w io.Writer, t client.TopicInfo)` — key: value lines.
    - `printTopicTable(w io.Writer, topics []client.TopicInfo)` — `text/tabwriter` table with columns `NAME / PARTITIONS / RF / RETENTION-MS / RETENTION-BYTES`.
    - `printTopicDescription(w io.Writer, d client.TopicDescription)` — header fields then partition sub-table with columns `PARTITION / SHARD / LEADER-NODE / EPOCH`.
    - `printPartitionTable(w io.Writer, parts []client.PartitionInfoWithOffsets)` — columns `PARTITION / LEADER-NODE / SHARD / EARLIEST / LATEST`.
  - `init()` registers all subcommands onto `topicCmd`, and `topicCmd` onto `rootCmd`.

- **Create** `cmd/bunnymq-cli/cluster.go` — `package main`:
  - `clusterCmd` — `cobra.Command{Use: "cluster", Short: "Describe the broker cluster"}`.
  - `clusterDescribeCmd` — no extra flags. Calls `AdminClient.DescribeCluster`; prints table via `printClusterTable`.
  - `printClusterTable(w io.Writer, cd client.ClusterDescription)` — `text/tabwriter` table with columns `NODE-ID / ADDRESS`.
  - `init()` registers `clusterDescribeCmd` onto `clusterCmd`, and `clusterCmd` onto `rootCmd`.

- **Create** `cmd/bunnymq-cli/format_test.go` — `package main`:
  - Unit tests for every `print*` formatting helper. Each test calls the function with a fixed input and asserts that `strings.Contains` or exact equality holds for the expected output lines. No broker connection needed.

## Out of scope

- TLS configuration flags — connection is plaintext only in v1.
- Output formats other than human-readable text (JSON mode is a future extension).
- `produce` and `consume` commands — T-070 and T-071.

## Definition of done

- [ ] `go build ./cmd/bunnymq-cli/...` passes.
- [ ] `go test ./cmd/bunnymq-cli/...` passes (formatting unit tests).
- [ ] `bunnymq-cli topic create --name foo --partitions 3 --replication-factor 3` exits 0 against a live cluster and prints topic info.
- [ ] `bunnymq-cli topic list` prints a table row for `foo`.
- [ ] `bunnymq-cli topic describe --name foo` prints partition sub-table.
- [ ] `bunnymq-cli topic delete --name foo` exits 0.
- [ ] `bunnymq-cli cluster describe` prints at least one node row.
- [ ] Missing required flags produce a usage error on stderr and non-zero exit code.

## Tests required

- `TestPrintTopicInfo` — calls `printTopicInfo` with a fixed `client.TopicInfo`; asserts output contains `"Name:"`, the topic name, and the retention values.
- `TestPrintTopicTable` — calls `printTopicTable` with two `client.TopicInfo` values; asserts both names appear in the output and the header row contains `"NAME"`.
- `TestPrintTopicDescription` — calls `printTopicDescription` with a `client.TopicDescription` containing two partitions; asserts header fields and both partition IDs appear.
- `TestPrintPartitionTable` — calls `printPartitionTable` with two `client.PartitionInfoWithOffsets` values; asserts header `"PARTITION"` and both earliest/latest offsets appear.
- `TestPrintClusterTable` — calls `printClusterTable` with a `client.ClusterDescription` containing two nodes; asserts `"NODE-ID"` header and both addresses appear.

## Dependencies

- T-045 (AdminClient — `pkg/client/admin.go` must exist and compile).

## Notes

Use `text/tabwriter` with `minwidth=1, tabwidth=8, padding=2, flags=0` for all tables. Flush the writer before returning from each `print*` function. Errors from `AdminClient` methods should be printed to `os.Stderr` and cause a non-zero exit via `cobra.Command.RunE` returning the error. Do not call `os.Exit` directly inside command handlers — let cobra handle it.
