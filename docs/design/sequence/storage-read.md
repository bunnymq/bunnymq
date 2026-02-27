# storage-read: Reading Batches by Offset

A `Read(offset, maxBytes)` call arrives from the Partition FSM's `Lookup()` method, itself triggered by the Data Coordinator when serving a fetch request. Storage snapshots the segment list under a read lock, releases the lock, locates the segment whose base-offset range contains `offset`, binary-searches the segment's offset index to find a conservative log position, linearly scans the log forward to the exact batch boundary, and returns as many complete batches as fit within `maxBytes`. If no data is available at the requested offset, Storage returns `(nil, offset, nil)` and the Data Coordinator enters a long-poll via `NewDataCh` (see [modules §5](../01-modules.md)).

```mermaid
sequenceDiagram
    participant DC as DataCoordinator
    participant FSM as PartitionFSM
    participant S as Storage
    participant SS as SegmentStorage
    participant OI as OffsetIndexSegment
    participant LS as LogSegment

    DC->>FSM: Lookup(ReadQuery{offset, maxBytes})
    FSM->>S: Read(offset, maxBytes)

    S->>S: acquire segMu read lock
    S->>S: check offset < EarliestOffset() → ErrOffsetOutOfRange
    S->>S: check offset >= nextOffset → return nil, offset, nil (no data yet)
    S->>S: binary search segments for largest baseOffset <= offset
    S->>S: snapshot target segment reference
    S->>S: release segMu read lock

    S->>SS: Read(offset, maxBytes)

    SS->>SS: relOffset = offset - segmentBaseOffset
    SS->>OI: BinarySearch(relOffset)
    Note over OI: find largest entry with relative_offset <= relOffset
    OI-->>SS: logPosition (conservative file offset)

    SS->>LS: ReadAt(logPosition, scanBuffer)
    loop scan forward until batch contains offset
        SS->>SS: parse batch header at currentPos (38 bytes)
        SS->>SS: batchEnd = batch.base_offset + batch.record_count
        alt batchEnd <= offset
            SS->>SS: currentPos += batch.batch_length (skip batch)
        else batch contains offset
            SS->>SS: matchPos = currentPos (first batch to return)
            SS->>SS: exit scan loop
        end
    end

    SS->>LS: ReadAt(matchPos, maxBytes)
    SS->>SS: trim to complete batch boundary
    loop accumulate batches while bytes remain
        SS->>SS: parse batch header at accumPos
        alt accumPos + batch_length <= matchPos + maxBytes
            SS->>SS: include batch; accumPos += batch_length
        else batch would exceed maxBytes
            SS->>SS: stop accumulation
        end
    end
    SS->>SS: nextOffset = last included batch.base_offset + last included batch.record_count
    SS-->>S: records[0:accumPos-matchPos], nextOffset, nil

    S-->>FSM: records, nextOffset, nil
    FSM-->>DC: records, nextOffset, nil

    alt records is empty (offset >= nextOffset)
        DC->>S: NewDataCh()
        S-->>DC: ch (current newDataCh snapshot)
        DC->>DC: select { case <-ch: retry Read | case <-time.After(maxWait): return empty | case <-ctx.Done() }
    end
```

## Participants

| Participant | Role |
|-------------|------|
| `DataCoordinator` | Initiates the fetch; drives long-poll if no data is available. |
| `PartitionFSM` | Implements `IOnDiskStateMachine.Lookup()`; calls `Storage.Read` and bounds the result by `lastAppliedIndex`. |
| `Storage` | Selects the target segment and enforces range checks. |
| `SegmentStorage` | Executes the two-phase index lookup + log scan. |
| `OffsetIndexSegment` | Provides the binary-search result (conservative log position). |
| `LogSegment` | Provides the raw bytes via `ReadAt`. |

## Edge Cases

- **Segment boundary:** If `offset` falls exactly on the `base_offset` of a segment, the binary search finds that segment directly; no scan of the previous segment is needed.
- **Active segment concurrent write:** `Read` may be issued while `Append` is running on the active segment. The FSM's `lastAppliedIndex` (not shown in this diagram for clarity) bounds the read to committed data. Storage's read request for the active segment uses `pread`, which is safe concurrently with `O_APPEND` writes on Linux.
- **maxBytes smaller than one batch:** If a single batch is larger than `maxBytes`, Storage returns that one batch anyway (truncating to `maxBytes` would produce a malformed batch). The Data Coordinator is responsible for enforcing client-side `max_bytes` at the gRPC layer and must handle a response slightly over the limit.
- **ReadByTime path:** `ReadByTime(timestampMs, maxBytes)` follows the same structure but uses the **time index** for the initial binary search: find the first segment whose first time-index entry has `timestamp_ms >= timestampMs` (or scan across segments), binary-search the segment's time index for the floor entry, then execute the same log-scan and batch-accumulation steps. The time index entry gives a `relative_offset`, not a log position directly; the offset index is then consulted for the log position, same as above.
