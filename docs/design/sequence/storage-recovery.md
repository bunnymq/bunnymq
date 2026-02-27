# storage-recovery: Startup Recovery

`Storage.Open(dir)` is called by the Partition FSM during `IOnDiskStateMachine.Open()`. Recovery enumerates all `.log` files in the partition directory, opens sealed segments as read-only, then scans the active (last) segment batch-by-batch with CRC-32C validation to determine the last valid byte position. Any trailing partial or corrupt batch is truncated. The active segment's index is rebuilt from scratch by replaying the validated log region, ensuring index consistency regardless of what state the index files were in at crash time. After `Open` returns, `Storage` is ready to accept `Append` and `Read` calls.

```mermaid
sequenceDiagram
    participant PFSM as PartitionFSM
    participant S as Storage
    participant FS as Filesystem
    participant Sealed as SealedSegmentStorage
    participant Active as ActiveSegmentStorage

    PFSM->>S: Open(dir)

    S->>FS: list *.log files in dir
    FS-->>S: sorted file list [seg0.log, seg1.log, ..., segN.log]

    alt no .log files found
        S->>Active: NewSegmentStorage(baseOffset=0, dir)
        Active->>FS: create 00000000000000000000.log/.index/.timeindex
        Active-->>S: ready (empty partition)
        S-->>PFSM: ok
    end

    loop for each segment except last (sealed segments)
        S->>Sealed: OpenSealed(baseOffset, dir)
        Sealed->>FS: open .log O_RDONLY
        Sealed->>FS: open .index O_RDONLY
        Sealed->>FS: open .timeindex O_RDONLY
        Sealed->>FS: mmap(.index, PROT_READ, MAP_SHARED)
        Sealed->>FS: mmap(.timeindex, PROT_READ, MAP_SHARED)
        Sealed->>Sealed: validate indexSize % 8 == 0
        alt index file size not a multiple of entry size
            Sealed->>FS: ftruncate(.index, indexSize - indexSize%8)
        end
        Sealed->>Sealed: validate timeIndexSize % 12 == 0
        alt timeindex file size not a multiple of 12
            Sealed->>FS: ftruncate(.timeindex, timeIndexSize - timeIndexSize%12)
        end
        Sealed-->>S: ok
    end

    S->>Active: OpenForRecovery(baseOffset=segN, dir)
    Active->>FS: open .log O_RDWR
    Active->>FS: stat .log → fileSize
    Active->>FS: open .index O_RDWR
    Active->>FS: open .timeindex O_RDWR

    Active->>Active: position = 0; nextOffset = segN.baseOffset

    loop scan active .log from position=0 while position < fileSize
        Active->>FS: pread(.log, position, 38 bytes)
        FS-->>Active: header bytes
        alt read returned < 38 bytes
            Active->>FS: ftruncate(.log, position)
            Active->>Active: break
        end
        Active->>Active: batchLen = bigEndian.Int32(header[8:12])
        alt batchLen < 38 OR position+batchLen > fileSize
            Active->>FS: ftruncate(.log, position)
            Active->>Active: break
        end
        Active->>FS: pread(.log, position+38, batchLen-38 bytes)
        FS-->>Active: record bytes
        Active->>Active: computedCRC = crc32c(record bytes)
        Active->>Active: storedCRC = bigEndian.Uint32(header[16:20])
        alt computedCRC != storedCRC
            Active->>FS: ftruncate(.log, position)
            Active->>Active: break
        end
        Active->>Active: position += batchLen
        Active->>Active: nextOffset = bigEndian.Int64(header[0:8]) + int64(bigEndian.Int32(header[12:16]))
    end

    Active->>Active: logSize = position

    Note over Active: Rebuild index from validated log region [0, logSize)
    Active->>Active: reset index state: entryCount=0, lastIndexedLogPos=0
    Active->>FS: fallocate(.index, offsetIndexAllocBytes); mmap PROT_READ|PROT_WRITE
    Active->>FS: fallocate(.timeindex, timeIndexAllocBytes); mmap PROT_READ|PROT_WRITE

    loop replay batches at pos=0..logSize for index rebuild
        Active->>FS: pread(.log, pos, 38)
        Active->>Active: if pos - lastIndexedLogPos >= index_sample_bytes
        Active->>Active: write (relOffset, pos) into .index mmap; entryCount++
        Active->>Active: write (max_timestamp, relOffset) into .timeindex mmap; entryCount++
        Active->>Active: lastIndexedLogPos = pos
        Active->>Active: pos += batchLen
    end

    Active->>FS: open .log O_WRONLY|O_APPEND (re-open for writing)
    Active-->>S: logSize, nextOffset

    S->>S: activeSegment = Active; this.nextOffset = nextOffset
    S-->>PFSM: ok (nextOffset returned for FSM to compare with sidecar lastAppliedIndex)
```

## Participants

| Participant | Role |
|-------------|------|
| `PartitionFSM` | Calls `Storage.Open` from `IOnDiskStateMachine.Open()`; uses the recovered `nextOffset` alongside its own sidecar file to determine which Raft entries to re-apply. |
| `Storage` | Coordinates the full recovery sequence; constructs the segment list. |
| `Filesystem` | Executes all syscalls: `open`, `stat`, `pread`, `ftruncate`, `fallocate`, `mmap`. |
| `SealedSegmentStorage` | Opened read-only; index files validated and (if needed) truncated. |
| `ActiveSegmentStorage` | Subject of the CRC scan and index rebuild. Opened `O_RDWR` for scan, then re-opened `O_WRONLY|O_APPEND` after recovery. |

## Edge Cases

- **Empty partition directory:** Treated as a fresh partition; the first segment is created with `base_offset = 0`.
- **All segments sealed, no active segment:** Cannot happen under normal operation — `Storage` always keeps at least one active segment. If it occurs (e.g., manual file deletion), the recovery treats the last `.log` file as the active segment; if that file is also missing, a fresh segment is created.
- **Truncation of a sealed segment's index:** Sealed segments are normally msync'd before a new segment begins. Truncation of a sealed index (Step 2) only occurs if a crash happened during the `ftruncate` step of a prior seal. This is safe: the index entries are reconstructable from the log; the sealed log file itself is trusted.
- **Active segment with valid CRC but mismatched `base_offset`:** If `batch.base_offset < segmentBaseOffset` or `batch.base_offset > nextOffset`, this indicates severe corruption. The scan treats this as a CRC mismatch (truncates at this position) and logs an `error`-level message. This case should never occur under correct FSM operation.
- **Index rebuild performance:** Re-scanning up to 128 MiB of log data to rebuild the index is O(n) in the segment size. At sequential read throughput of ~1 GiB/s (NVMe), the worst case is ~128 ms — well within the 30-second recovery target from [REQUIREMENTS.md §6.5](../REQUIREMENTS.md).
