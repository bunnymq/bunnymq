# T-032: FSM determinism tests

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** M
**Status:** TODO

## Goal

Write a determinism test suite that applies the same sequence of commands to two independent FSM instances and verifies their state is byte-for-byte identical, proving that both FSMs satisfy dragonboat's determinism contract.

## Context

dragonboat's correctness depends on all replicas applying the same Raft log entries and arriving at identical state. Any non-determinism in `Update()` — map iteration order, uninitialized state, undeclared goroutine writes — would cause silent state divergence across nodes, which is a correctness failure. The determinism tests catch this class of bug before integration.

References:
- [03-raft-fsm.md §3.6 — Determinism rules](../../design/03-raft-fsm.md#36-determinism-rules)
- [03-raft-fsm.md §5 — Determinism contract](../../design/03-raft-fsm.md#5-determinism-contract)

## Scope

- Write test package `internal/metadata/determinism_test.go`:
  - Helper `applyAll(fsm *MetadataFSM, cmds []MetadataCommand)` that calls `fsm.Update(sm.Entry{Cmd: jsonBytes})` for each command.
  - Helper `snapshotBytes(fsm *MetadataFSM) []byte` that calls `fsm.SaveSnapshot` into a `bytes.Buffer` and returns the bytes.
  - `TestMetadataFSM_Determinism_Topics` — build a sequence of 20+ commands: RegisterNode×3, CreateTopic×5, AlterTopicRetention×2, AssignPartitionLeader×5, DeleteTopic×1, CreateTopic×1 (same name); apply to FSM-A and FSM-B (separate instances); compare `snapshotBytes(A)` == `snapshotBytes(B)`.
  - `TestMetadataFSM_Determinism_ConsumerGroups` — command sequence: CreateTopic, JoinGroup×3, Heartbeat×3, CommitOffset×2, LeaveGroup×1; verify snapshots identical.
  - `TestMetadataFSM_Determinism_RebalanceOrdering` — join group members in two different orders (M1 first then M2 on FSM-A; M2 first then M1 on FSM-B); since rebalance is deterministic on sorted member IDs, both FSMs should end up with identical assignments.
- Write test package `internal/partition/determinism_test.go`:
  - Helper that applies `AppendBatch` entries (with pre-encoded batch bytes) to two `PartitionFSM` instances sharing separate temporary directories.
  - `TestPartitionFSM_Determinism_AppendBatch` — apply 10 AppendBatch entries to FSM-A and FSM-B; read back all batches from Storage in both; verify byte-for-byte equality of the full batch sequence.
  - `TestPartitionFSM_Determinism_RetentionConfig` — apply RetentionConfig then AppendBatch on both FSMs; verify both call `SetRetentionConfig` with identical values.

## Out of scope

- Multi-node integration test — T-033.
- Race condition detection (covered by `-race` flag in `make test`).

## Definition of done

- [ ] `go test -race ./internal/metadata/... ./internal/partition/...` passes.
- [ ] `TestMetadataFSM_Determinism_Topics`: snapshots of FSM-A and FSM-B are byte-equal.
- [ ] `TestMetadataFSM_Determinism_RebalanceOrdering`: different join order → same final assignment on both FSMs.
- [ ] `TestPartitionFSM_Determinism_AppendBatch`: batch bytes in storage on both instances are byte-equal.

## Tests required

- `TestMetadataFSM_Determinism_Topics` (see Scope).
- `TestMetadataFSM_Determinism_ConsumerGroups` (see Scope).
- `TestMetadataFSM_Determinism_RebalanceOrdering` (see Scope).
- `TestPartitionFSM_Determinism_AppendBatch` (see Scope).
- `TestPartitionFSM_Determinism_RetentionConfig` (see Scope).

## Dependencies

T-026, T-027 (MetadataFSM Update handlers).
T-029 (MetadataFSM SaveSnapshot for snapshot comparison).
T-030, T-031 (PartitionFSM Update and storage integration).
T-015 (EncodeBatch for creating test batch payloads).

## Notes

The snapshot comparison (`snapshotBytes(A) == snapshotBytes(B)`) relies on JSON marshaling being deterministic for the same state. Go's `json.Marshal` sorts map keys alphabetically in the output (Go 1.12+), so this is safe. If any map key ordering issue is found, switch to `json.MarshalIndent` with explicit sort helpers and compare byte slices. The rebalance ordering test is the most valuable: it directly tests that the sort in `rebalance()` is correct and that no map-key-order non-determinism leaks through.
