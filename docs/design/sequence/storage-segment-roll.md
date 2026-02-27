# storage-segment-roll: Sealing the Active Segment and Creating a New One

A segment roll is triggered at the end of `Storage.Append` when `activeSegment.logSize >= segment_max_bytes`. The roll is performed synchronously within the `Append` call while the write lock is held. It seals the current active `SegmentStorage` — truncating and msync'ing its index files, remapping them read-only, and fsyncing the log file — then constructs a new `SegmentStorage` with `baseOffset = nextOffset`, pre-allocates its index files, and maps them read-write. After the roll the new segment becomes the active one and the formerly active segment joins the sealed list.

```mermaid
sequenceDiagram
    participant S as Storage
    participant AS as ActiveSegmentStorage
    participant OI as OffsetIndexSegment
    participant TI as TimeIndexSegment
    participant LS as LogSegment
    participant FS as Filesystem
    participant NS as NewSegmentStorage

    Note over S: segMu write lock already held from Append

    S->>AS: Seal()

    AS->>OI: Truncate(entryCount * 8)
    OI->>FS: ftruncate(.index, entryCount * 8)
    FS-->>OI: ok
    AS->>OI: Msync()
    OI->>FS: msync(mmap region, MS_SYNC)
    FS-->>OI: ok
    AS->>OI: Remap(PROT_READ)
    OI->>FS: munmap(current region)
    OI->>FS: mmap(.index, PROT_READ, MAP_SHARED)
    FS-->>OI: read-only region

    AS->>TI: Truncate(entryCount * 12)
    TI->>FS: ftruncate(.timeindex, entryCount * 12)
    FS-->>TI: ok
    AS->>TI: Msync()
    TI->>FS: msync(mmap region, MS_SYNC)
    FS-->>TI: ok
    AS->>TI: Remap(PROT_READ)
    TI->>FS: munmap(current region); mmap(.timeindex, PROT_READ, MAP_SHARED)
    FS-->>TI: read-only region

    AS->>LS: Fsync()
    LS->>FS: fsync(.log fd)
    FS-->>LS: ok
    AS-->>S: sealed

    S->>S: append AS to sealed segments slice

    S->>NS: NewSegmentStorage(baseOffset=nextOffset, dir)
    NS->>FS: create <nextOffset>.log  O_WRONLY|O_APPEND|O_CREATE
    FS-->>NS: log fd

    NS->>FS: create <nextOffset>.index; fallocate(offsetIndexAllocBytes)
    FS-->>NS: index fd
    NS->>FS: mmap(.index, PROT_READ|PROT_WRITE, MAP_SHARED)
    FS-->>NS: RW mmap region

    NS->>FS: create <nextOffset>.timeindex; fallocate(timeIndexAllocBytes)
    FS-->>NS: timeindex fd
    NS->>FS: mmap(.timeindex, PROT_READ|PROT_WRITE, MAP_SHARED)
    FS-->>NS: RW mmap region

    NS->>NS: logSize=0; entryCount=0; lastIndexedLogPos=0
    NS-->>S: ready

    S->>S: activeSegment = NS
    Note over S: lock released in Append after this returns
```

## Participants

| Participant | Role |
|-------------|------|
| `Storage` | Orchestrates the roll while holding the write lock; updates `activeSegment`. |
| `ActiveSegmentStorage` | The segment being sealed; owns its index and log sub-components. |
| `OffsetIndexSegment` | Truncates, msyncs, and remaps its mmap region read-only. |
| `TimeIndexSegment` | Same as `OffsetIndexSegment`. |
| `LogSegment` | Fsyncs the `.log` file to ensure data is durable before the next segment starts. |
| `Filesystem` | Executes kernel syscalls: `ftruncate`, `msync`, `munmap`, `mmap`, `fsync`, `fallocate`. |
| `NewSegmentStorage` | The newly created active segment; initialised with zero counters. |

## Edge Cases

- **fallocate unavailability:** If `fallocate` returns `EOPNOTSUPP` (e.g., on tmpfs during tests), `NewSegmentStorage` falls back to writing `offsetIndexAllocBytes` / `timeIndexAllocBytes` bytes of zeroes. The fallback is logged at `warn` level. See [02-storage.md Open Question 5](../02-storage.md).
- **Crash between msync and fsync:** If the node crashes after the index files are msync'd but before the log fsync, the sealed segment's index is durable but the log may be missing its last pages. On recovery the sealed segment (all but the last `.log`) is treated as consistent (Step 2 of crash recovery in §6 of [02-storage.md](../02-storage.md)). This is safe because the write that triggered the roll has already been accepted into the Raft log; it will be re-applied on restart.
- **Crash during new segment creation:** If the crash happens after the sealed segment's fsync but before `NewSegmentStorage` completes, on restart the last `.log` file belongs to the new (partially initialised) segment. It may be empty (0 bytes), which the scan loop in Step 3 of recovery handles gracefully: an empty active segment is valid.
- **Read concurrency during roll:** Concurrent readers snapshot the segment list before file I/O (under read lock). During the roll, the write lock is held, so readers either see the pre-roll list (and read from the soon-to-be-sealed segment) or the post-roll list (and read from the sealed segment). Both are safe because the sealed segment's files remain open and readable throughout.
