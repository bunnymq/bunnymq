# Sequence: Graceful Single-Node Shutdown

Planned shutdown of one node. The goal is to stop accepting new RPCs, drain in-flight requests, transfer Raft leadership away from any shards this node leads (to minimise client disruption), and flush Storage before exiting.

---

## Phase 1 - Stop accepting new RPCs

```mermaid
sequenceDiagram
    participant Signal as OS Signal<br/>(SIGTERM / SIGINT)
    participant Main as main goroutine
    participant GMGMT as ManagementService<br/>gRPC server
    participant GDATA as DataService<br/>gRPC server
    participant Client as Client (any)

    Signal->>Main: SIGTERM

    Note over Main: Initiate graceful shutdown sequence.<br/>Cancel the root context (ctx.cancel()).<br/>All background goroutines (reconcile, sweep, heartbeat)<br/>observe ctx.Done() and exit their loops.

    Main->>GMGMT: server.GracefulStop()
    Main->>GDATA: server.GracefulStop()

    Note over GMGMT,GDATA: GracefulStop() stops accepting new connections.<br/>Waits for all in-flight RPCs to complete<br/>(up to a configurable drain timeout, default 15s).<br/>After timeout, forcibly closes remaining connections.

    Client->>GDATA: (any RPC arriving after GracefulStop) → connection refused / RST
    Note over Client: Client receives a connection error and retries<br/>against another broker node.
```

---

## Phase 2 - Transfer Raft leadership

```mermaid
sequenceDiagram
    participant Main as main goroutine
    participant CC as ClusterCoordinator
    participant RH as RaftHost
    participant DB as dragonboat
    participant N1 as Node-1 (peer)
    participant N2 as Node-2 (peer)

    Note over Main: After gRPC drain, initiate Raft leadership transfer.

    Main->>CC: Stop()
    CC->>CC: Stop background goroutines (reconcile, sweep).

    Note over CC: Transfer leadership for all shards this node leads.

    CC->>RH: GetLeadingShards() → [shardID=0 (metadata), shardID=1, shardID=3]

    loop for each leading shard
        CC->>DB: RequestLeaderTransfer(shardID)
        Note over DB: dragonboat sends a LeaderTransfer message to a peer.<br/>A follower (Node-1 or Node-2) triggers an election<br/>and wins without waiting for an election timeout.<br/>VERIFY: dragonboat v4 API for leader transfer -<br/>candidate: nh.RequestLeaderTransfer(shardID, targetNodeID).<br/>Mark as VERIFY.
        DB->>N1: LeaderTransfer (shardID)
        N1->>N2: RequestVote
        N2-->>N1: VoteGranted
        Note over N1: Node-1 becomes leader for shardID.<br/>Node-3 is now a follower (or about to leave).
    end

    Note over CC: Leadership transfer is best-effort.<br/>If transfer does not complete within a timeout (default 5s),<br/>proceed with shutdown anyway - dragonboat will elect<br/>a new leader automatically after this node disappears.
```

---

## Phase 3 - Stop partition shards and flush Storage

```mermaid
sequenceDiagram
    participant Main as main goroutine
    participant DC as DataCoordinator
    participant DB as dragonboat
    participant PFSM as PartitionFSM<br/>(per shard)
    participant STR as Storage<br/>(per shard)

    Main->>DC: Stop()

    loop for each running partition shard (in reverse start order)
        DC->>DB: StopNode(shardID, nodeID=thisNode)
        Note over DB: dragonboat stops the Raft state machine for this shard.<br/>Calls PartitionFSM.Close() before returning.<br/>VERIFY: dragonboat v4 shutdown API -<br/>candidate: nh.StopNode(shardID, nodeID) or nh.Close() (closes all shards).

        DB->>PFSM: Close()
        PFSM->>STR: Sync()
        Note over STR: fsync the active .log segment and .index file.<br/>Ensures all committed-and-applied data is on disk.
        STR->>STR: Write applied.idx sidecar<br/>(last_raft_index, latest_offset) - final flush.
        STR-->>PFSM: ok
        PFSM-->>DB: ok
    end

    DC-->>Main: done
```

---

## Phase 4 - Stop metadata shard and exit

```mermaid
sequenceDiagram
    participant Main as main goroutine
    participant CC as ClusterCoordinator
    participant DB as dragonboat
    participant MFSM as MetadataFSM

    Main->>CC: StopMetadataShard()
    CC->>DB: StopNode(shardID=0, nodeID=thisNode)
    Note over DB: dragonboat stops metadata shard replica.<br/>Calls MetadataFSM.Close() if defined.<br/>In-memory state is discarded (recovered from snapshot on restart).
    DB->>MFSM: Close() (no-op - state is in-memory; snapshot already persisted by dragonboat)
    DB-->>CC: ok
    CC-->>Main: done

    Main->>DB: NodeHost.Close()
    Note over DB: Closes the dragonboat NodeHost entirely.<br/>Flushes any pending Raft log entries to disk.<br/>Releases all file handles and goroutines.
    DB-->>Main: ok

    Main->>Main: os.Exit(0)
```

---

## Shutdown sequence summary

```
SIGTERM received
  │
  ├─ 1. Cancel root context
  │      → background goroutines exit (reconcile, sweep, heartbeat)
  ├─ 2. gRPC GracefulStop() on both servers
  │      → drain in-flight RPCs (timeout 15s)
  ├─ 3. RequestLeaderTransfer for each leading shard (timeout 5s per shard)
  │      → minimise client disruption during election
  ├─ 4. DataCoordinator.Stop()
  │      → StopNode per partition shard
  │      → PartitionFSM.Close() → Storage.Sync() + applied.idx flush
  ├─ 5. ClusterCoordinator.Stop()
  │      → StopNode for metadata shard
  ├─ 6. NodeHost.Close()
  │      → flush Raft log, release resources
  └─ 7. os.Exit(0)
```

Total expected shutdown time on a lightly loaded node: under 5 seconds. Under heavy load (many in-flight RPCs), the gRPC drain timeout dominates.

---

## Failure scenarios during shutdown

| Scenario | Behaviour |
|---|---|
| gRPC drain timeout exceeded (in-flight RPCs still active) | `server.Stop()` forcibly closes remaining connections. Clients receive transport errors and retry. |
| Leader transfer fails or times out | Shutdown proceeds without transfer. dragonboat on remaining nodes detects the election timeout and elects a new leader within `electionRTT` × `electionTimeoutMultiplier` ticks (configurable). Typical: ~150–300ms on LAN. |
| Storage.Sync() fails (I/O error) | Logged as fatal. Shutdown continues - the node is already exiting. On restart, the applied.idx sidecar may be stale; dragonboat replays any missed log entries from the leader. |
| Process killed (SIGKILL, no graceful shutdown) | dragonboat Raft log is consistent (fsync'd per entry). Storage may have a partial last write; the `applied.idx` sidecar indicates the last clean boundary. On restart, entries after `last_raft_index` in the log are replayed. See [startup-node-join.md](./startup-node-join.md). |
| All three nodes shut down simultaneously | Cluster becomes unavailable. Clients receive `UNAVAILABLE`. When nodes restart, they reform the cluster via fresh `StartCluster(join=true)` calls and the metadata shard leader is re-elected. |

---

## Notes

- **No client forwarding during shutdown.** The node stops accepting new RPCs immediately via `GracefulStop`. Clients that hit a connection error retry against bootstrap addresses. The leader transfer (Phase 2) ensures the new leader is ready before the node goes dark, so retries typically succeed on the first attempt.
- **VERIFY: dragonboat leader transfer API.** `nh.RequestLeaderTransfer(shardID, targetNodeID)` - verify exact signature and whether a `targetNodeID` must be specified or if dragonboat picks a suitable follower automatically. If target is required, the coordinator must pick a follower from the shard's current membership.
- **VERIFY: dragonboat shutdown API.** `nh.StopNode(shardID, nodeID)` vs `nh.Close()`. `Close()` shuts down the entire NodeHost (all shards). Prefer `Close()` in the final step rather than per-shard `StopNode`, to avoid partial states. Verify call order with dragonboat v4 documentation.
- **MetadataFSM snapshot timing.** dragonboat automatically triggers snapshots based on the log compaction configuration (`CompactionOverhead`, `SnapshotIntervalScale`). The shutdown does not trigger an explicit snapshot - the last auto-snapshot is sufficient since Raft log replay covers any remaining entries.
- **Consumer group sessions during shutdown.** When this node was the group coordinator (metadata shard leader), members will receive heartbeat errors during the shutdown window. After the new metadata leader is elected, members' next heartbeats will hit `NOT_LEADER`, causing them to discover and reconnect to the new coordinator. Session timeouts are not triggered by this transient gap (the new coordinator resets heartbeat timers as described in [08-consumer-groups.md §9](../08-consumer-groups.md)).
