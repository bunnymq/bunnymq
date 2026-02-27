# Sequence: Consumer Fetch - Long-Poll (waiting for new records)

Fetch request where no records are available at the requested offset and `maxWaitMs > 0`. The goroutine parks until a producer writes new data, the timeout fires, the leader changes, or the client disconnects.

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DAPI as DataAPI
    participant DC as DataCoordinator<br/>(this node - leader)
    participant RH as RaftHost
    participant PFSM as PartitionFSM
    participant STR as Storage

    participant P as Producer (different client)
    participant RHp as RaftHost (produce path)
    participant PFSMw as PartitionFSM (Update)

    C->>+DAPI: Fetch RPC {offset=200, maxBytes=1MiB, maxWaitMs=5000}
    DAPI->>+DC: Fetch(ctx, topic, partitionID, 200, 1MiB, 5000ms)

    DC->>RH: LookupMetadata(QueryGetPartition) → leader=this node
    DC->>DC: registryMu.RLock → shardID

    Note over DC: Begin long-poll loop.<br/>Deadline = now + 5000ms.

    loop until data / timeout / leader change / ctx cancel
        Note over DC: Step 1 - Snapshot newDataCh BEFORE reading<br/>(eliminates race where data arrives between<br/>the failed read and channel snapshot)

        DC->>+RH: LookupPartition(QueryGetNewDataCh, shardID)
        RH->>+PFSM: Lookup(QueryGetNewDataCh)
        PFSM->>STR: NewDataCh() - acquire chanMu, return current ch
        STR-->>PFSM: ch (chan struct{})
        PFSM-->>-RH: ch as interface{}
        RH-->>-DC: ch

        Note over DC: Step 2 - Re-verify leadership<br/>(leader may have changed since RPC arrived)
        DC->>RH: LookupMetadata(QueryGetPartition)
        RH-->>DC: LeaderNodeID == this node ✓

        Note over DC: Step 3 - Attempt read
        DC->>+RH: LookupPartition(QueryRead, offset=200, maxBytes=1MiB)
        RH->>+PFSM: Lookup(QueryRead{200, 1MiB})
        PFSM->>+STR: Read(200, 1MiB)
        Note over STR: offset=200 >= LatestOffset=200<br/>→ no records available yet
        STR-->>-PFSM: (nil, 200, nil)
        PFSM-->>-RH: ReadResult{Records:nil}
        RH-->>-DC: ReadResult{nil}

        Note over DC: Step 4 - Park on channel / timeout / ctx

        DC->>DC: select { case <-ch:... case <-time.After(remaining):... case <-ctx.Done():... }

        Note over DC,P: === Producer writes a new batch (separate flow) ===

        P->>RHp: SyncProposePartition(CmdAppendBatch)
        RHp->>PFSMw: Update([entry]) - quorum committed
        PFSMw->>STR: Append(batch) → base_offset=200, record_count=10
        Note over STR: Writes batch to .log segment.<br/>Closes current newDataCh (ch from above).<br/>Replaces with new channel.<br/>LatestOffset is now 210.
        STR-->>PFSMw: 200

        Note over DC: ch is now closed →<br/>select wakes on <-ch
    end

    Note over DC: Loop again after wake-up

    DC->>+RH: LookupPartition(QueryGetNewDataCh) → new ch2
    RH-->>-DC: ch2

    DC->>RH: LookupMetadata → leader=this node ✓

    DC->>+RH: LookupPartition(QueryRead, offset=200, maxBytes=1MiB)
    RH->>+PFSM: Lookup(QueryRead{200, 1MiB})
    PFSM->>+STR: Read(200, 1MiB)
    Note over STR: LatestOffset=210 > 200 → records found.<br/>Returns batches covering offsets [200, 209].
    STR-->>-PFSM: (records, nextOffset=210, nil)
    PFSM-->>-RH: ReadResult{records, 210}
    RH-->>-DC: ReadResult

    DC-->>-DAPI: records, nextOffset=210
    DAPI-->>-C: FetchResponse (OK) {records, next_offset=210}
```

---

## Timeout path

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DC as DataCoordinator<br/>(leader)
    participant STR as Storage

    Note over DC: Deadline reached (5000ms elapsed).<br/>select fires <-time.After(remaining).

    DC-->>C: FetchResponse (OK) {records=nil, next_offset=0}

    Note over C: Empty response with next_offset=0 signals<br/>"no data in this window".<br/>Client re-issues Fetch with the same offset<br/>and another maxWaitMs window.
```

---

## Leader-change path during long-poll

```mermaid
sequenceDiagram
    participant C as Consumer (client)
    participant DC as DataCoordinator<br/>(was leader, now follower)
    participant RH as RaftHost

    Note over DC: On next loop iteration,<br/>leaderCheck re-reads metadata.

    DC->>RH: LookupMetadata(QueryGetPartition)
    Note over RH: AssignPartitionLeader has committed<br/>- new leader elected.
    RH-->>DC: LeaderNodeID = other_node

    DC-->>C: FetchResponse (FAILED_PRECONDITION)<br/>NotLeader{other_node, "addr:port"}

    Note over C: Client reconnects to new leader<br/>and re-issues Fetch.
```

---

## Race elimination: why channel snapshot comes before Read

```text
Timeline (wrong order - channel snapshot AFTER read):

  t0: Read(200) → empty (LatestOffset=200)
  t1: [Producer] Append(batch) → LatestOffset=210, closes OLD_ch, creates NEW_ch
  t2: NewDataCh() → returns NEW_ch  ← wrong: we missed the notification
  t3: select on NEW_ch → blocks until NEXT append, missing offset 200..209

Timeline (correct order - channel snapshot BEFORE read):

  t0: NewDataCh() → returns OLD_ch
  t1: [Producer] Append(batch) → LatestOffset=210, closes OLD_ch, creates NEW_ch
  t2: Read(200) → may return empty (race) OR return data
      If empty: OLD_ch is already closed → select exits immediately → loop → Read again → finds data ✓
      If data:  return data directly ✓
```

This is the ordering invariant documented in [01-modules.md §5](../01-modules.md) and implemented in [05-data-coordinator.md §6.2](../05-data-coordinator.md).

---

## Notes

- **One goroutine per request.** Each long-poll Fetch occupies one gRPC handler goroutine for up to `maxWaitMs` ms. No goroutine pool is needed; gRPC's server handles the concurrency model.
- **Context cancellation.** If the consumer disconnects, the gRPC framework cancels `ctx`. The select's `<-ctx.Done()` arm exits the loop. No resource leaks.
- **`maxWaitMs` value selection.** The client library chooses `maxWaitMs = min(remaining_poll_timeout, server_max_wait)`. The server honours whatever value is sent (up to the request's gRPC deadline). See [07-client-library.md](../07-client-library.md).
- **Return value on timeout.** `FetchResponse` with `records=nil` and `next_offset=0` (or the original offset, depending on API Protocol decision in [06-api-protocol.md](../06-api-protocol.md)). The client re-uses its previous offset for the next poll. See Open Question 4 in [05-data-coordinator.md](../05-data-coordinator.md).
