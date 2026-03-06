# T-030: Partition FSM — Open and Update

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** M
**Status:** TODO

## Goal

Implement `PartitionFSM.Open()` and `PartitionFSM.Update()` in `internal/partition`, including the sidecar file (`applied.idx`) read/write, the crash-recovery reconciliation with Storage, and the `persistApplied` fsync+rename sequence.

## Context

`PartitionFSM` is the on-disk state machine that bridges dragonboat's Raft engine and `internal/storage`. `Open()` returns the last applied Raft index so dragonboat knows where to resume log replay. `Update()` applies committed entries to Storage and persists a sidecar atomically so that crash recovery can correctly reconcile Storage state against the applied index.

References:
- [03-raft-fsm.md §4.3 — Open() procedure](../../design/03-raft-fsm.md#43-open---partition-recovery)
- [03-raft-fsm.md §4.4 — Update()](../../design/03-raft-fsm.md#44-update)
- [T-007 — fsync strategy decision](../M0-foundations/T-007-decide-fsync-strategy.md)

## Scope

- Implement `PartitionFSM` struct in `internal/partition/fsm.go`:
  - Fields: `storage storage.Storage`, `dir string`, `sidecarPath string`, `lastAppliedIndex atomic.Uint64`.
- Implement `(*PartitionFSM).Open(stopc <-chan struct{}) (uint64, error)`:
  - Calls `storage.Open(dir)` (crash-recovery scan, T-022).
  - Reads sidecar at `sidecarPath` (16 bytes big-endian: `last_applied_raft_index` + `corresponding_storage_latest_offset`).
  - If sidecar absent (`os.IsNotExist`): returns `0, nil` (fresh partition).
  - If sidecar present and `storage.LatestOffset() > sidecar.LatestOffset`: calls `storage.TruncateTo(sidecar.LatestOffset)`.
  - Stores `lastAppliedIndex`; returns `sidecar.LastAppliedIndex, nil`.
- Define `readSidecar(path string) (*sidecarData, error)` and `encodeSidecar(index uint64, latestOffset int64) []byte`.
- Implement `(*PartitionFSM).Update(entries []sm.Entry) ([]sm.Entry, error)`:
  - Iterates entries; dispatches on `e.Cmd[0]`:
    - `CmdAppendBatch (0x01)`: calls `storage.Append(e.Cmd[1:])`, panics on error; sets `e.Result = sm.Result{Value: uint64(baseOffset)}`.
    - `CmdRetentionConfig (0x02)`: JSON-unmarshals `e.Cmd[1:]` into `RetentionConfigPayload`, calls `storage.SetRetentionConfig`, panics on unmarshal error.
    - Unknown byte: panics with message "unknown partition command type".
  - After processing all entries: calls `persistApplied(entries[len(entries)-1].Index)`, panics on failure.
  - Stores `lastAppliedIndex`; returns `entries, nil`.
- Implement `persistApplied(index uint64) error`:
  - `storage.Sync()` (fsync active log).
  - Write 16-byte sidecar to `sidecarPath + ".tmp"`.
  - fsync the `.tmp` file.
  - `os.Rename(tmp, sidecarPath)` (atomic on Linux).
- Confirm `PartitionFSM` implements `sm.IOnDiskStateMachine` (compile-time assertion in test).

## Out of scope

- Partition FSM Lookup — T-031.
- Strategy A snapshots — T-031.
- Storage implementation — M1 tickets.

## Definition of done

- [ ] `go build ./internal/partition/...` passes.
- [ ] `go test ./internal/partition/...` passes.
- [ ] `Open` on a fresh directory returns `(0, nil)`; `Storage` has a single empty active segment.
- [ ] `Update` with `AppendBatch` entry: `storage.Append` called with `e.Cmd[1:]`; result value = assigned base offset.
- [ ] `Update` with `RetentionConfig` entry: `storage.SetRetentionConfig` called with correct values.
- [ ] `persistApplied` writes 16-byte sidecar; re-read sidecar matches the index and offset.
- [ ] `Open` after crash (storage ahead of sidecar): `TruncateTo` called; storage offset matches sidecar value.
- [ ] Compile-time `IOnDiskStateMachine` assertion passes.

## Tests required

- `TestPartitionFSM_OpenFresh` — open on empty dir; returns (0, nil); LatestOffset = 0.
- `TestPartitionFSM_UpdateAppend` — propose AppendBatch entry; storage has the batch; sidecar written.
- `TestPartitionFSM_UpdateRetentionConfig` — propose RetentionConfig entry; no panic; sidecar written.
- `TestPartitionFSM_PersistApplied_Sidecar` — after Update, read sidecar file; index and offset match.
- `TestPartitionFSM_OpenReconcile` — simulate crash: manually append to storage past sidecar offset; reopen; TruncateTo called; LatestOffset matches sidecar.
- `TestPartitionFSM_OpenClean` — close gracefully; reopen; index from sidecar matches; no truncation.
- `TestPartitionFSM_InterfaceConformance` — compile-time assertion.

## Dependencies

T-025 (PartitionCommand types, RetentionConfigPayload).
T-020 (Storage.Open, Append, Sync, TruncateTo, SetRetentionConfig).
T-022 (crash recovery called from Storage.Open).
T-007 (fsync strategy: immediate fsync per Update batch via persistApplied).

## Notes

`persistApplied` calls `fsync` on the `.tmp` sidecar before `os.Rename` to ensure durability of the sidecar data itself. On Linux, `os.Rename` is atomic at the filesystem level — the old file is replaced atomically. This ensures: either the old sidecar is present (crash before rename) or the new one is (crash after rename), never a partial write. The `stopc` channel in `Open()` should be monitored in a select if Storage.Open is expected to be slow; for course-project scale, ignoring it is acceptable.
