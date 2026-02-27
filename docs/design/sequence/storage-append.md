# storage-append: Appending a Batch to the Active Segment

A `Append(batch)` call arrives from the Partition FSM (which received a committed Raft entry). Storage acquires its write lock, assigns the `base_offset` by writing it into `batch[0:8]`, delegates the write to the active `SegmentStorage`, conditionally writes index entries when the sampling threshold is crossed, broadcasts the new-data notification, and - if the log file now meets or exceeds the size threshold - seals the active segment and creates a new one. The call returns the assigned `base_offset` to the FSM, which stores it as the `sm.Result.Value` returned to dragonboat.

```mermaid
sequenceDiagram
    participant FSM as PartitionFSM
    participant S as Storage
    participant SS as ActiveSegmentStorage
    participant LS as LogSegment
    participant OI as OffsetIndexSegment
    participant TI as TimeIndexSegment

    FSM->>S: Append(batch)
    S->>S: acquire segMu write lock
    S->>S: savedOffset = nextOffset
    S->>S: write savedOffset into batch[0:8] (big-endian int64)

    S->>SS: append(batch)
    SS->>SS: batchStartPos = logSize
    SS->>LS: Write(batch)
    LS-->>SS: n bytes written, nil
    SS->>SS: logSize += n

    alt batchStartPos - lastIndexedLogPos >= index_sample_bytes
        SS->>SS: relOffset = savedOffset - segmentBaseOffset
        SS->>OI: Append(relOffset, batchStartPos)
        OI->>OI: write entry into mmap[entryCount*8]; entryCount.Add(1)
        SS->>TI: Append(batch.max_timestamp, relOffset)
        TI->>TI: write entry into mmap[entryCount*12]; entryCount.Add(1)
        SS->>SS: lastIndexedLogPos = batchStartPos
    end

    SS-->>S: nil

    S->>S: nextOffset = savedOffset + int64(batch.record_count)

    S->>S: close(oldNewDataCh)
    S->>S: newDataCh = make(chan struct{})

    alt logSize >= segment_max_bytes
        Note over S,TI: Segment roll - full detail in storage-segment-roll.md
        S->>SS: Seal()
        SS->>OI: ftruncate to entryCount*8; msync; munmap; mmap PROT_READ
        SS->>TI: ftruncate to entryCount*12; msync; munmap; mmap PROT_READ
        SS->>LS: Fsync()
        SS-->>S: nil
        S->>S: append sealed SS to segments slice
        S->>S: create new ActiveSegmentStorage(baseOffset=nextOffset)
        S->>S: new .log O_WRONLY|O_APPEND|O_CREATE
        S->>S: pre-allocate + mmap new .index and .timeindex PROT_READ|PROT_WRITE
    end

    S->>S: release segMu write lock
    S-->>FSM: savedOffset, nil
```

## Participants

| Participant | Role |
|-------------|------|
| `PartitionFSM` | Caller: dragonboat has committed a Raft entry and invoked `Update()`, which calls `Append`. |
| `Storage` | Owns the segment list, `nextOffset`, and `newDataCh`. Coordinates the write and roll decision. |
| `ActiveSegmentStorage` | Performs the actual file write and index sampling. |
| `LogSegment` | Wraps the active `.log` file descriptor (O_APPEND); executes the `write` syscall. |
| `OffsetIndexSegment` | Maintains the mmap'd `.index` region and the atomic `entryCount`. |
| `TimeIndexSegment` | Maintains the mmap'd `.timeindex` region and its atomic `entryCount`. |

## Edge Cases

- **batch_length mismatch:** If `len(batch)` differs from `batch.batch_length` (bytes 8–12 of the header), the log will be corrupt. The FSM must ensure `len(batch) == batch_length` before calling `Append`. Storage does not validate this on the hot path.
- **Roll during append:** The roll is inlined at the end of `Append`, still under the write lock. Concurrent reads acquire the read lock to snapshot the segment list before file I/O; they are not blocked by the roll itself, only by the brief lock window for the slice update. File creation for the new segment happens inside the lock but is fast (metadata only; no data is written to the new log yet).
- **newDataCh broadcast:** `close(oldNewDataCh)` wakes all goroutines currently selecting on it. The new channel is created before the lock is released, so any goroutine that calls `NewDataCh()` after the lock is released gets the new (open) channel and will be woken by the *next* append.
- **fsync on roll:** The active segment is fsynced during `Seal()`, not on every `Append`. This is safe because dragonboat fsyncs its Raft log before calling `Update()`; a crash between the Raft fsync and the Storage write leaves the Raft entry intact for replay.
