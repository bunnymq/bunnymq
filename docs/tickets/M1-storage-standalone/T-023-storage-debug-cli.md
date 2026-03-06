# T-023: Storage debug CLI tool

**Milestone:** M1 — Storage standalone
**Effort:** S
**Status:** TODO

## Goal

Implement a CLI tool at `cmd/storage-debug/` that lets a developer append batches, read batches by offset, and dump a segment's contents for ad-hoc storage validation outside of the full broker stack.

## Context

M1's milestone DoD explicitly requires a standalone CLI tool for ad-hoc testing and a visual way to verify storage behaviour. The tool is a developer utility only — no performance or security requirements. It enables manual verification of the crash-recovery, retention, and segment-roll behaviours built in T-020–T-022 without writing end-to-end integration tests.

References:
- [M1 Milestone DoD — CLAUDE.md](../../CLAUDE.md#m1--storage-standalone)
- [02-storage.md §2 — Public interface](../../design/02-storage.md#2-public-interface)

## Scope

- Create `cmd/storage-debug/main.go` with a `cobra`-based CLI.
- Subcommand `append <dir>`:
  - Reads one record value per line from stdin (raw bytes treated as record values; nil key).
  - Encodes each line as a single-record batch using `EncodeBatch`.
  - Opens `Storage` at `<dir>` and calls `Append` for each batch.
  - Prints `base_offset=<N>` for each batch.
- Subcommand `read <dir> <offset> [--max-bytes N]`:
  - Opens `Storage` at `<dir>`, calls `Read(offset, maxBytes)`.
  - Decodes and prints each batch: `batch offset=<N> records=<K>`, followed by each record's value (truncated to 80 bytes if longer) and timestamp.
- Subcommand `dump-segment <segment.log>`:
  - Opens the `.log` file directly (not via `Storage`; bypasses recovery).
  - Scans all batches using `DecodeNextBatch`.
  - Prints for each batch: `offset=<N> length=<L> records=<K> ts=[<base_ts>..<max_ts>] crc=OK|FAIL`.
- Subcommand `stats <dir>`:
  - Lists each segment: `<filename>: base_offset=<N> size=<bytes>`.
  - Prints `EarliestOffset`, `LatestOffset`, segment count.
- Flag `--config <file>` for overriding `segment_max_bytes`, `index_sample_bytes` (YAML or environment variables via `viper`).

## Out of scope

- Integration with broker configuration — M3.
- Performance benchmarks — not required.
- gRPC client operations — M3 tickets.

## Definition of done

- [ ] `go build ./cmd/storage-debug/...` passes.
- [ ] `storage-debug append <dir>` reads stdin, appends batches, prints base offsets.
- [ ] `storage-debug read <dir> <offset>` reads and prints batches from the given offset.
- [ ] `storage-debug dump-segment <file>` scans a raw `.log` file and reports per-batch stats.
- [ ] `storage-debug stats <dir>` prints segment list and offset range.
- [ ] Tool exits non-zero on error with a human-readable message.

## Tests required

`TestStorageDebugCLI` — N/A for unit tests; this ticket is validated manually and by the milestone DoD requirement "a standalone CLI tool exists for ad-hoc testing". Document the manual test procedure in the ticket notes.

Manual test procedure:
1. `storage-debug append /tmp/test-storage` — type 3 lines, verify `base_offset=0`, `base_offset=1`, `base_offset=2`.
2. `storage-debug read /tmp/test-storage 0` — verify all 3 batches printed.
3. `storage-debug dump-segment /tmp/test-storage/00000000000000000000.log` — verify batch table with `crc=OK`.
4. `storage-debug stats /tmp/test-storage` — verify segment count = 1, offsets = 0..3.

## Dependencies

T-015 (EncodeBatch, DecodeBatch).
T-020 (Storage.Open, Append, Read).
T-012 (cobra and viper already in go.mod).

## Notes

Keep the CLI minimal. No TUI, no colour codes, no pagination. Output is line-based for easy `grep`/`awk` inspection. The `dump-segment` subcommand opens the `.log` directly without calling `Storage.Open` so it works on individual files detached from a partition directory (useful for inspecting files copied from a production node). Use `cobra.Command` for argument parsing; register subcommands in `main.go`. Do not add flags that the milestone does not require.
