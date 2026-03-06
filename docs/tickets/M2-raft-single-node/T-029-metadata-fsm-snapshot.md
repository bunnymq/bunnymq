# T-029: Metadata FSM — snapshot save and restore

**Milestone:** M2 — Raft + FSMs on a single node
**Effort:** S
**Status:** TODO

## Goal

Implement `MetadataFSM.SaveSnapshot` and `MetadataFSM.RecoverFromSnapshot` in `internal/metadata` using JSON encoding of the full `MetadataState`, and wire the FSM into dragonboat's `IStateMachine` interface by implementing `Close`.

## Context

The Metadata FSM snapshot enables fast restart: instead of replaying the full Raft log, dragonboat installs a snapshot and replays only log entries after the snapshot index. The snapshot is a JSON encoding of `MetadataState` — straightforward to implement and sufficient for course-project scale (< 1 MiB for a small cluster). The `SnapshotEntries` config for the metadata shard is set to a moderate value (e.g., 10 000) so dragonboat triggers snapshots periodically.

References:
- [03-raft-fsm.md §3.5 — Snapshot strategy](../../design/03-raft-fsm.md#35-snapshot-strategy)

## Scope

- Implement `(*MetadataFSM).SaveSnapshot(w io.Writer, sc sm.ISnapshotFileCollection, done <-chan struct{}) error`:
  - Checks `done` channel before starting; returns `sm.ErrSnapshotStopped` if closed.
  - Calls `json.NewEncoder(w).Encode(fsm.state)`.
- Implement `(*MetadataFSM).RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error`:
  - Checks `done` channel.
  - Allocates a new `MetadataState{}` and JSON-decodes into it.
  - Replaces `fsm.state` with the decoded value.
- Implement `(*MetadataFSM).Close() error`: returns nil (no owned resources beyond in-memory state).
- Confirm `MetadataFSM` implements `sm.IStateMachine` (compile-time assertion in `fsm_test.go`):
  ```go
  var _ sm.IStateMachine = (*MetadataFSM)(nil)
  ```

## Out of scope

- PartitionFSM snapshots — T-031.
- Snapshot trigger configuration (SnapshotEntries value) — part of T-024 (NodeHost config).

## Definition of done

- [ ] `go build ./internal/metadata/...` passes.
- [ ] `go test ./internal/metadata/...` passes.
- [ ] `SaveSnapshot` + `RecoverFromSnapshot` round-trip is identity for a non-trivial state (2 topics, 6 partitions, 1 consumer group with 2 members, committed offsets).
- [ ] Compile-time `IStateMachine` assertion passes.
- [ ] `done` channel checked; returns `sm.ErrSnapshotStopped` if closed before encoding completes.

## Tests required

- `TestMetadataFSM_SnapshotRoundTrip` — build state via Update commands; SaveSnapshot; RecoverFromSnapshot on a fresh FSM; Lookup returns identical values.
- `TestMetadataFSM_SnapshotDone` — pass a pre-closed `done` channel to SaveSnapshot; returns `sm.ErrSnapshotStopped`.
- `TestMetadataFSM_InterfaceConformance` — compile-time `var _ sm.IStateMachine = (*MetadataFSM)(nil)` in test file.

## Dependencies

T-025 (MetadataState JSON tags).
T-026 (Update handlers to build state for snapshot test).
T-027 (CG state for snapshot test).
T-028 (Lookup for post-restore verification).

## Notes

`MetadataState.CommittedOffsets` is `map[PartitionKey]int64`. `PartitionKey` has `Topic string` and `PartitionID int32` fields — Go JSON encoding requires map keys to be strings, so either implement `MarshalText`/`UnmarshalText` on `PartitionKey`, or switch the in-memory representation to a slice for the snapshot. The simplest approach: implement `PartitionKey.MarshalText() ([]byte, error)` as `fmt.Sprintf("%s:%d", pk.Topic, pk.PartitionID)` and a matching `UnmarshalText`.
