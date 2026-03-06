# T-033: Single-node restart integration test

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** M
**Status:** TODO

## Goal

Write an integration test that starts a single-node dragonboat cluster in-process, performs topic creation, partition appends, and fetches through the full Raft path, then kills the node and verifies state survives a restart — exercising `Open()`, crash recovery, and Raft log replay end-to-end.

## Context

The M2 milestone DoD requires: "Integration test exists: kill the single-node Raft, restart, verify state recovers." This test is the capstone of M2 — it verifies that all the components from T-024 through T-031 compose correctly under real dragonboat lifecycle. It runs with an in-process NodeHost (no network; single node is both leader and follower for itself).

References:
- [03-raft-fsm.md §1.2 — Lifecycle](../../design/03-raft-fsm.md#12-lifecycle)
- [M2 Milestone DoD — CLAUDE.md](../../CLAUDE.md#m2--raft--fsms-on-a-single-node)

## Scope

- Create test file `internal/integration/single_node_test.go` (or `internal/raft/integration_test.go`):

  **Setup helper** `startSingleNodeCluster(t *testing.T, dataDir string) *raft.Host`:
  - Constructs `MetadataFSM` factory and `PartitionFSM` factory.
  - Creates `raft.Host` with single-node `initialMembers = {1: "localhost:0"}` (port 0 for OS-assigned).
  - Starts metadata shard (shard 0).
  - Returns the `Host`.

  **`TestSingleNode_CreateTopicAndAppend`:**
  - Start cluster.
  - `SyncProposeMetadata`: RegisterNode (node 1).
  - `SyncProposeMetadata`: CreateTopic ("test-topic", partitionCount=1, rf=1, replicaNodes=[[1]]).
  - Lookup: `QueryGetPartition` → get `shardID`.
  - `StartPartitionShard(shardID, ...)`.
  - `SyncProposePartition(shardID, AppendBatch)` × 3 batches.
  - `LookupPartition(shardID, QueryLatestOffset)` → verify = 3.
  - `LookupPartition(shardID, QueryRead, offset=0)` → verify all 3 batches returned.
  - Close the host.

  **`TestSingleNode_RestartRecovery`:**
  - Start cluster in `dataDir`.
  - CreateTopic, StartPartitionShard, append 5 batches, close.
  - Restart: create new `raft.Host` with same `dataDir`, same `initialMembers`.
  - Wait for shard 0 leadership (poll `LookupMetadata` until no error, max 5s).
  - `LookupMetadata(QueryGetTopic)` → topic must exist (Raft log replay or metadata snapshot).
  - Restart partition shard; `LookupPartition(QueryLatestOffset)` → must equal 5.
  - Append 2 more batches; read all 7.

  **`TestSingleNode_PartitionFSM_CrashRecovery`:**
  - Start, create partition shard, append 3 batches, `storage.Sync()`.
  - Simulate crash: close host WITHOUT calling `PartitionFSM.Close()` (i.e., skip the graceful flush; use `os.Exit` workaround or directly simulate by truncating the last batch in the `.log` file after close).
  - Reopen; verify LatestOffset ≥ 2 (at least 2 of 3 batches survived).

## Out of scope

- Multi-node cluster tests — M3 ticket.
- Consumer group integration — M4 ticket.

## Definition of done

- [ ] `go test ./internal/integration/... -timeout 60s` passes.
- [ ] `TestSingleNode_CreateTopicAndAppend`: all 3 batches readable after single-session appends.
- [ ] `TestSingleNode_RestartRecovery`: topic metadata survives restart; partition data survives restart.
- [ ] Tests use `t.TempDir()` for isolation; no leftover state between test runs.
- [ ] Tests complete within 30 seconds each (dragonboat convergence on localhost is fast).

## Tests required

- `TestSingleNode_CreateTopicAndAppend` (see Scope).
- `TestSingleNode_RestartRecovery` (see Scope).
- `TestSingleNode_PartitionFSM_CrashRecovery` (see Scope).

## Dependencies

T-024 (raft.Host).
T-025 through T-029 (MetadataFSM complete).
T-030, T-031 (PartitionFSM complete).
T-020, T-022 (Storage complete with crash recovery).
T-015 (EncodeBatch for test batch payloads).

## Notes

Use `t.TempDir()` for `DataDir` — Go testing framework cleans it up automatically. Single-node dragonboat: `initialMembers = map[uint64]string{nodeID: raftAddress}` with `join=false`. Use a local Unix socket or a loopback port for Raft address (single-node clusters don't actually send network RPCs). Wait for quorum by polling `LookupMetadata` in a loop with `time.Sleep(50ms)` up to 5s; a single-node cluster elects itself leader almost immediately. The "simulate crash" test is the hardest to make deterministic — the simplest approach is to append batches without calling `storage.Sync()` after the last one, then truncate the log file by 1 byte, then reopen and verify at least N-1 batches are recoverable.
