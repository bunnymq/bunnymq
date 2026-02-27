# raft-snapshot-metadata: Metadata FSM Snapshot Save and Recovery

The Metadata FSM uses dragonboat's standard `IStateMachine` snapshot mechanism. When dragonboat decides a snapshot is needed (triggered by `SnapshotEntries` threshold or an explicit request), it calls `SaveSnapshot` on the leader's FSM. The FSM JSON-serializes its full in-memory state and writes it to dragonboat's snapshot writer. On a follower that is too far behind to catch up via Raft log entries alone, dragonboat installs the snapshot by calling `RecoverFromSnapshot`, which replaces the FSM's in-memory state by deserializing the JSON. Both operations are performed on the FSM while dragonboat holds an internal read lock on the FSM state - no concurrent `Update()` calls occur during snapshot save.

```mermaid
sequenceDiagram
    participant DB as dragonboat
    participant FSM as MetadataFSM (leader)
    participant SW as SnapshotWriter (dragonboat-managed)
    participant FSM_F as MetadataFSM (lagging follower)
    participant SR as SnapshotReader (dragonboat-managed)

    Note over DB: SnapshotEntries threshold reached OR explicit RequestSnapshot

    DB->>FSM: SaveSnapshot(w SnapshotWriter, fc ISnapshotFileCollection, done chan)
    FSM->>FSM: json.NewEncoder(w).Encode(state)
    Note over FSM: Encodes: Topics, Partitions, Nodes, Groups, NextShardID
    FSM-->>DB: nil (snapshot written to w)

    DB->>DB: persist snapshot to disk; record snapshot index N

    Note over DB: Raft log entries up to index N may now be compacted
    DB->>DB: CompactLog(upToIndex: N - CompactionOverhead)

    Note over DB,FSM_F: Follower is too far behind; dragonboat sends snapshot instead of log entries

    DB->>FSM_F: RecoverFromSnapshot(r SnapshotReader, files []SnapshotFile, done chan)
    FSM_F->>FSM_F: newState = &MetadataState{}
    FSM_F->>FSM_F: json.NewDecoder(r).Decode(newState)
    FSM_F->>FSM_F: state = newState
    FSM_F-->>DB: nil (state replaced)

    Note over FSM_F: FSM is now at index N; dragonboat resumes sending log entries from N+1
```

## Participants

| Participant | Role |
|-------------|------|
| `dragonboat` | Decides when to snapshot; manages snapshot persistence and transmission to lagging followers. |
| `MetadataFSM (leader)` | Serializes the full `MetadataState` to JSON via the snapshot writer. |
| `SnapshotWriter` | dragonboat-managed `io.Writer` backed by a local file; content is later streamed to followers. |
| `MetadataFSM (lagging follower)` | Replaces its in-memory state by deserializing the snapshot. |
| `SnapshotReader` | dragonboat-managed `io.Reader`; delivers the leader's snapshot bytes. |

## Edge Cases

- **Concurrent Lookup during SaveSnapshot:** dragonboat does not call `Update()` during `SaveSnapshot()`, but `Lookup()` may be called concurrently. The `SaveSnapshot` implementation uses `json.NewEncoder(w).Encode(state)` which reads `state` fields - if `Lookup` reads concurrently, there is a data race. The FSM must take a read lock during `SaveSnapshot` (or `Lookup`) if dragonboat can call both concurrently. Verify dragonboat's guarantee here (Open Question - raised in §6 of [03-raft-fsm.md](../03-raft-fsm.md)).
- **Snapshot size:** At 100 000 partitions the JSON snapshot is ≈ 20–50 MiB. JSON encoding is CPU-bound at ≈ 200 MiB/s → ≈ 100–250 ms. During this window dragonboat does not call `Update()` on the FSM. This adds tail latency to metadata operations while the snapshot runs. Acceptable for course-project workloads; at production scale, a binary encoding (protobuf) would be faster.
- **`done` channel:** dragonboat closes `done` to signal cancellation (e.g., node is shutting down). The FSM should select on `done` between encoding large fields. For simplicity in v1, `json.Encode` is not cancellable; the `done` channel is not checked.
- **Follower divergence recovery:** After `RecoverFromSnapshot` returns, the follower's FSM state is at index N. If the follower previously had a different state (e.g., after a partial network partition), the snapshot atomically replaces it. Any goroutines doing `Lookup` concurrently must not see a half-replaced state - the FSM should acquire a write lock during `RecoverFromSnapshot`.
