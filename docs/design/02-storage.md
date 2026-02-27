# Storage — Detailed Design

Storage is the physical persistence layer for a single partition replica. It manages a segmented, append-only log on disk (`.log` files) alongside two sparse, mmap'd index files per segment — a `.index` file for offset-to-file-position lookup and a `.timeindex` file for timestamp-to-offset lookup. Storage is used exclusively by the Partition FSM (`internal/partition`); nothing else in BunnyMQ calls Storage directly. The interface is intentionally shaped around that one caller: a single sequential writer driven by committed Raft entries, plus concurrent readers bounded by the FSM's `lastAppliedIndex`.

See [overview](./00-overview.md) for the architectural rationale behind the segmented-log design (§5.5), and [modules](./01-modules.md) for the module boundary and the long-polling mechanism (§5).

---

## 1. Component Hierarchy

```text
Storage
└── []SegmentStorage          // ordered by base offset; last element is active
    ├── LogSegment            // wraps the .log file
    ├── OffsetIndexSegment    // wraps the .index file (mmap'd)
    └── TimeIndexSegment      // wraps the .timeindex file (mmap'd)
```

### Storage

Top-level component. Owns the ordered slice of `SegmentStorage` instances (oldest first, active last). Implements the `Storage` interface exposed to the Partition FSM. Responsibilities:

- Directs `Append` calls to the active segment.
- Routes `Read` and `ReadByTime` calls to the correct segment via binary search on base offsets.
- Triggers a segment roll when the active log file reaches `segment_max_bytes`.
- Maintains `nextOffset` (the offset that will be assigned to the next batch).
- Broadcasts new-data notifications via `newDataCh`.
- Runs the background retention goroutine.
- Executes crash recovery during `Open`.

**Lifecycle:** Created by `internal/partition` during `IOnDiskStateMachine.Open()`. Closed during `IOnDiskStateMachine.Close()`.

### SegmentStorage

Manages one segment triple: a `.log` file, a `.index` file, and a `.timeindex` file sharing the same base-offset filename prefix. A segment is either **active** (writable) or **sealed** (read-only). Responsibilities:

- Appending a batch to the log file and updating the index at the configured sampling rate.
- Serving reads: binary-search the offset index, then linear-scan the log to the first batch that contains the requested offset, then return complete batches up to `maxBytes`.
- Sealing: truncating index files to actual length, msync, remapping read-only, fsyncing the log.

**Owned files:** `<base_offset>.log`, `<base_offset>.index`, `<base_offset>.timeindex`.

### LogSegment

Wraps an `*os.File`. An active segment's file is opened `O_WRONLY|O_APPEND|O_CREATE`. After sealing it is re-opened `O_RDONLY`. Maintains an in-memory `logSize` counter (derived from `os.Stat` at open, then incremented by each write). Does not perform any index operations.

### OffsetIndexSegment

Wraps a `[]byte` mmap region over the `.index` file. Each entry is **8 bytes** (big-endian):

```text
relative_offset : int32   // = batch.base_offset - segment.base_offset
position        : int32   // byte offset of the batch start within the .log file
```

Active state: `PROT_READ|PROT_WRITE`, pre-allocated to a fixed size. Sealed state: truncated to actual length, msync'd, remapped `PROT_READ`. Maintains an in-memory `entryCount` counter.

### TimeIndexSegment

Wraps a `[]byte` mmap region over the `.timeindex` file. Each entry is **12 bytes** (big-endian):

```text
timestamp_ms    : int64   // = batch.max_timestamp
relative_offset : int32   // = batch.base_offset - segment.base_offset
```

Same lifecycle as `OffsetIndexSegment`. Maintains its own `entryCount` counter.

---

## 2. Public Interface

```go
// Storage is the persistence interface for a single partition replica.
// Append is called only from the Partition FSM's Update(), which dragonboat
// guarantees is never concurrent with itself. Read, ReadByTime, EarliestOffset,
// and LatestOffset may be called concurrently from Lookup() and fetch goroutines.
// EnforceRetention and SetRetentionConfig are called from the background goroutine.
// Close must be called only after all other callers have stopped.
type Storage interface {
    // Append writes batch to the active segment and returns the base offset
    // assigned to this batch. Storage overwrites batch[0:8] with the assigned
    // base_offset (big-endian int64) before writing to disk. The CRC field in
    // the batch header must already be valid over records[] (bytes at [38, batch_length)).
    // Append is not safe to call concurrently.
    Append(batch []byte) (baseOffset int64, err error)

    // Read returns up to maxBytes of serialised batch data starting at the batch
    // whose range [base_offset, base_offset+record_count) contains offset.
    // The returned slice contains only complete batches. nextOffset is the first
    // offset not included in the result; use it as offset in the next Read call.
    // Returns (nil, offset, nil) when no data is available at that offset yet.
    Read(offset int64, maxBytes int) (records []byte, nextOffset int64, err error)

    // ReadByTime returns up to maxBytes of batch data starting at the first batch
    // whose base_timestamp >= timestampMs. nextOffset follows the same convention
    // as Read. Returns (nil, 0, ErrTimestampNotFound) if no such batch exists.
    ReadByTime(timestampMs int64, maxBytes int) (records []byte, nextOffset int64, err error)

    // EarliestOffset returns the base_offset of the oldest available batch.
    // Returns 0 when no batches have been written.
    EarliestOffset() int64

    // LatestOffset returns the next offset to be assigned (i.e., base_offset +
    // record_count of the last written batch). Returns 0 before the first Append.
    LatestOffset() int64

    // EnforceRetention deletes the oldest sealed segments that violate the
    // retention policy. The active segment is never deleted. A segment is eligible
    // if it satisfies the time constraint OR the bytes constraint. At least one
    // segment (the active one) is always retained.
    EnforceRetention(retentionMs int64, retentionBytes int64) (deletedSegments int, err error)

    // NewDataCh returns a channel closed by the next successful Append.
    // Callers must snapshot this channel before calling Read; if Read returns
    // no data, select on the snapshot to wake when data arrives. The channel
    // is replaced after each Append, so the caller must re-fetch after waking.
    NewDataCh() <-chan struct{}

    // SetRetentionConfig updates the retention parameters used by the background
    // goroutine. Applied on the next retention tick. Safe to call concurrently.
    SetRetentionConfig(retentionMs int64, retentionBytes int64)

    // Sync fsyncs the active log file. Called by the Partition FSM's Update()
    // before writing the applied.idx sidecar to ensure batch bytes are durable
    // before the sidecar records them as applied.
    Sync() error

    // TruncateTo removes all batch data at or after the given base_offset by
    // truncating the active segment's .log file to the byte position that
    // corresponds to that offset. Used by the Partition FSM during Open() to
    // undo writes that completed on disk but were not recorded in the applied.idx
    // sidecar before a crash. offset must be <= LatestOffset(); calling with
    // offset == LatestOffset() is a no-op. TruncateTo rebuilds the active
    // segment index after truncation.
    TruncateTo(offset int64) error

    // Close msyncs active index files, fsyncs the active log file, and closes
    // all file descriptors. The Storage instance must not be used after Close returns.
    Close() error
}
```

> **base_offset assignment:** Storage is the authoritative source of the next offset and writes it into `batch[0:8]` before appending to disk. The CRC field covers only `records[]` (bytes [38, batch_length)), so overwriting `base_offset` does not invalidate the CRC. This is the decided approach — single call, single source of truth for offset assignment.

---

## 3. On-Disk Format

### 3.1 Directory Layout

One directory per partition replica, placed under the node's data root. Directory name: `<topic>-<partition_id>`. Example:

```text
/var/lib/bunnymq/partitions/
├── orders-0/
│   ├── 00000000000000000000.log
│   ├── 00000000000000000000.index
│   ├── 00000000000000000000.timeindex
│   ├── 00000000000000001337.log
│   ├── 00000000000000001337.index
│   └── 00000000000000001337.timeindex
└── orders-1/
    └── ...
```

File names use a **zero-padded 20-digit decimal** base offset (the `base_offset` of the first batch in that segment), matching Kafka's naming convention. There are no file-level headers; all necessary metadata is encoded in file names and batch headers.

### 3.2 Batch Format

This section reproduces [REQUIREMENTS.md §4.4](../REQUIREMENTS.md) for self-containment. All fixed-width fields are **big-endian**. The CRC algorithm is **CRC-32C** (Castagnoli, polynomial 0x1EDC6F41, as used by Kafka since batch format v2).

```text
Batch on disk (identical to batch on wire):

Offset  Len   Type     Field            Notes
──────  ───   ──────   ───────────────  ──────────────────────────────────────────────
0       8     int64    base_offset      Server-assigned offset of the first record.
8       4     int32    batch_length     Total byte length of this batch (header + records).
12      4     int32    record_count     Number of records in this batch.
16      4     uint32   crc32            CRC-32C over records[] (bytes [38, batch_length)).
20      2     int16    attributes       Reserved; must be 0 in v1.
22      8     int64    base_timestamp   Timestamp of the first record (ms since Unix epoch).
30      8     int64    max_timestamp    Timestamp of the last record (ms since Unix epoch).
──────  ───   ──────   ───────────────  Header total: 38 bytes
38      var   []byte   records          One or more Record values (see below).

Record (variable length, immediately follows prior record or header):
  length           : varint   Byte length of remaining record fields.
  attributes       : int8     Reserved; 0 in v1.
  timestamp_delta  : varint   record.timestamp_ms - base_timestamp.
  offset_delta     : varint   record.offset - base_offset.
  key_length       : varint   -1 if key is nil.
  key              : bytes    Absent when key_length == -1.
  value_length     : varint   Byte length of value.
  value            : bytes
  headers_count    : varint   Number of headers (0 if none).
  headers          : [Header]*

Header:
  key_length       : varint
  key              : bytes    UTF-8 encoded.
  value_length     : varint
  value            : bytes
```

The log reader advances through a segment by consuming exactly `batch_length` bytes per batch starting at file offset 0. There is no inter-batch separator or padding.

### 3.3 Index Entry Layouts

**Offset index** (`.index`) — 8 bytes per entry, big-endian:

| Offset | Size | Type  | Field            | Notes |
|--------|------|-------|------------------|-------|
| 0      | 4    | int32 | relative_offset  | `batch.base_offset − segment.base_offset` |
| 4      | 4    | int32 | position         | Byte offset of the batch start within the `.log` file |

**Time index** (`.timeindex`) — 12 bytes per entry, big-endian:

| Offset | Size | Type  | Field            | Notes |
|--------|------|-------|------------------|-------|
| 0      | 8    | int64 | timestamp_ms     | `batch.max_timestamp` |
| 8      | 4    | int32 | relative_offset  | `batch.base_offset − segment.base_offset` |

Indexes are **sparse**: one entry is added when the log bytes written since the previous index entry reach `index_sample_bytes` (default 4 096). Both indexes are monotonically non-decreasing in their first column, enabling binary search. The binary search returns the floor entry (largest entry whose key ≤ the search key), giving a conservative starting position for the subsequent log scan.

---

## 4. mmap Mechanics

### Pre-allocation

At segment creation each index file is pre-allocated to a fixed size before the mmap call, ensuring the file has enough capacity for the segment lifetime:

```text
maxIndexEntries        = ceil(segment_max_bytes / index_sample_bytes)
offsetIndexAllocBytes  = maxIndexEntries × 8,  rounded up to the OS page size
timeIndexAllocBytes    = maxIndexEntries × 12, rounded up to the OS page size
```

With defaults (`segment_max_bytes` = 128 MiB, `index_sample_bytes` = 4 096):
`maxIndexEntries` = 32 768; `offsetIndexAllocBytes` = 256 KiB; `timeIndexAllocBytes` = 384 KiB.

Pre-allocation is done via `fallocate(FALLOC_FL_KEEP_SIZE, ...)` on Linux. If `fallocate` is unavailable (e.g., on tmpfs), fall back to writing zeroes up to the target size and log a `warn`-level message so operators are aware that index pre-allocation is degraded.

### Active Segment

The mmap region covers the entire pre-allocated file (`PROT_READ|PROT_WRITE`, `MAP_SHARED`). An in-memory `entryCount` field per index tracks the next write slot. Index entries are written directly into the mmap slice; Linux's page cache propagates changes to the underlying file.

**msync frequency:** Index files are **not** msync'd on every index entry write. msync is called only during segment seal. Rationale: the index is fully reconstructable from the validated log on crash recovery, so losing unflushed index entries on a crash costs only slightly slower startup, not data loss. Per-write msync would add a syscall on the hot append path for no safety benefit beyond what the log already provides.

### Segment Seal

When rolling a segment (see §5.3):

1. `ftruncate(.index, entryCount × 8)` — shrink the pre-allocated file to actual size.
2. `msync(.index, MS_SYNC)` — flush dirty index pages to disk.
3. `munmap(.index)` — release the write mapping.
4. `mmap(.index, PROT_READ, MAP_SHARED)` — re-map read-only.
5. Repeat steps 1–4 for `.timeindex` (entry size 12).
6. `fsync(.log)` — flush log file pages.

After these six steps the sealed segment is entirely read-only and safe to serve from concurrent goroutines.

### Crash Semantics

If the node crashes after an `Append` write but before a seal, the following is possible:
- The `.log` file may or may not contain the last batch (OS page cache flush is non-deterministic without fsync).
- The `.index`/`.timeindex` files may contain 0, some, or all entries for that batch.

Both cases are handled by §6: the log is scanned batch-by-batch with CRC-32C verification to determine the last valid byte position; the active segment's index is then rebuilt from scratch by re-scanning the validated region.

---

## 5. Segment Lifecycle

### States

| State    | Description |
|----------|-------------|
| **Active**  | The one segment being written. Exactly one at any time. |
| **Sealed**  | Read-only after roll. Index files truncated and remapped. Zero or more sealed segments exist. |
| **Deleted** | Removed from the segment list and from disk during retention enforcement. |

### 5.1 Segment Creation

A new `SegmentStorage` is created in two situations:

- **Bootstrap:** no `.log` files exist in the partition directory. Create the first segment with `base_offset = 0`.
- **Segment roll:** `base_offset = Storage.nextOffset` at the moment of the roll.

Files created: `<base_offset>.log` (O_WRONLY|O_APPEND|O_CREATE), `<base_offset>.index` (pre-allocated, mmap'd PROT_READ|PROT_WRITE), `<base_offset>.timeindex` (pre-allocated, mmap'd PROT_READ|PROT_WRITE).

### 5.2 Segment Growth

Each `Append` call adds `len(batch)` bytes to the active `.log` file. The `SegmentStorage.logSize` counter is incremented. Conditionally one offset-index entry and one time-index entry are written (§3.3). No size enforcement occurs within `SegmentStorage`.

### 5.3 Roll Criteria

After a successful write, `Storage.Append` checks:

```go
if activeSegment.logSize >= config.SegmentMaxBytes {
    roll()
}
```

Only size-based roll is implemented in v1. Time-based roll (`segment_max_age_ms`) is explicitly out of scope.

### 5.4 Roll Procedure

See [sequence/storage-segment-roll.md](./sequence/storage-segment-roll.md) for the full flow. In summary: seal the active `SegmentStorage` (truncate + msync + remap indexes, fsync log), append it to the sealed list, then create a new `SegmentStorage` with `base_offset = nextOffset`.

### 5.5 Segment Deletion

Retention enforcement (§7) removes a sealed segment by:
1. Acquiring the segment list write lock.
2. Removing the element from the slice.
3. Releasing the lock.
4. Calling `os.Remove` on the three files (outside the lock — file removal is slow).

Deletion happens oldest-first. The active segment is never removed.

---

## 6. Crash Recovery

`Storage.Open(dir)` is invoked by `internal/partition` during `IOnDiskStateMachine.Open()`. The caller receives the last-applied Raft index from a sidecar file managed by the Partition FSM (not by Storage); `Open` returns the highest `nextOffset` derived from the log itself.

### Step 1 — Enumerate Segments

List all `.log` files in the partition directory. Sort by the numeric base offset encoded in the filename. If no `.log` files exist, create the first segment with `base_offset = 0` and return.

### Step 2 — Open Sealed Segments

For each segment except the last:
- Open `.log`, `.index`, `.timeindex` read-only.
- Mmap `.index` and `.timeindex` with `PROT_READ|MAP_SHARED`.
- Validate: index file size must be a multiple of its entry size (8 for `.index`, 12 for `.timeindex`). If not (implies a crash during `ftruncate`), truncate to the nearest valid multiple.
- These segments are trusted as logically consistent: they were fsynced and msync'd before any subsequent data was committed.

### Step 3 — Recover the Active Segment

The last `.log` file is the active segment and may be partially written. Scan from byte 0:

```go
position := int64(0)
fileSize := stat(logFile).Size()
nextOffset := segmentBaseOffset

for position < fileSize {
    header, err := readFull(logFile, position, 38)
    if err != nil || len(header) < 38 {
        truncate(logFile, position)
        break
    }

    batchLen := int(bigEndian.Int32(header[8:12]))
    if batchLen < 38 || position+int64(batchLen) > fileSize {
        truncate(logFile, position)
        break
    }

    records, _ := readFull(logFile, position+38, batchLen-38)
    if crc32c(records) != bigEndian.Uint32(header[16:20]) {
        truncate(logFile, position)
        break
    }

    position += int64(batchLen)
    baseOffset := bigEndian.Int64(header[0:8])
    recordCount := int64(bigEndian.Int32(header[12:16]))
    nextOffset = baseOffset + recordCount
}
logSize = position
```

After the scan, `position` is the first byte of invalid data (or end of file on a clean shutdown). `nextOffset` is the offset that will be assigned to the next batch.

### Step 4 — Rebuild Active Segment Index

Re-scan the validated region (bytes `[0, position)`) of the active `.log` file. Apply the same sampling logic as normal appends. Write offset-index and time-index entries into the mmap region (freshly allocated if the index file was missing or corrupt). This fully reconstructs the index, tolerating any crash-time inconsistency in the `.index` and `.timeindex` files.

### Step 5 — Open Active Segment for Writing

Re-open `.log` with `O_WRONLY|O_APPEND`. Set `logSize = position`. The mmap region from Step 4 remains active (read-write).

### Three Crash Scenarios

| Scenario | Outcome |
|----------|---------|
| **Clean shutdown** | `Close()` called `fsync` on the log and `msync` on indexes. Scan completes normally with no truncation. |
| **Crash during active-segment write** | Last batch may be partially written. CRC check fails; log is truncated at the start of that batch. At most `batch_length` bytes (≤ 4 MiB per [REQUIREMENTS.md §5](../REQUIREMENTS.md)) are lost. The Partition FSM re-applies the entry from the Raft log on restart. |
| **Crash during segment seal** | The seal sequence is: ftruncate index → msync index → remap index → fsync log → create new segment. If crash occurs at any point before fsync, the still-active segment is recovered by the normal scan. If crash occurs after fsync but before the new segment is created, the sealed segment is the last `.log` file; its scan completes normally (clean data, no truncation). The partially executed seal is detected by missing or zero-length replacement files and is retried. |

---

## 7. Retention Enforcement

`Storage.Open` starts a background goroutine that ticks every `retention_check_interval_ms` and calls `EnforceRetention(retentionMs, retentionBytes)`. The goroutine reads the retention configuration atomically on each tick (updated via `SetRetentionConfig`).

### Eligibility

Only **sealed** segments are eligible. The active segment is always kept. If only the active segment remains, `EnforceRetention` returns immediately.

### Expiration Criteria

Two constraints are evaluated independently; a sealed segment is deleted if it satisfies **either**:

**Time retention:** Segment `S` at index `i` is expired if:

```text
segments[i+1].firstBatchBaseTimestamp < now - retentionMs
```

Using the *next* segment's first batch timestamp (from time-index entry 0) ensures that a segment is not deleted until all of its messages are definitively older than the threshold. This matches Kafka's time-retention semantics.

**Bytes retention:** Accumulate sizes from the oldest sealed segment forward. Mark segments for deletion until the remaining total (all segments not marked) fits within `retentionBytes`. If total size ≤ `retentionBytes`, nothing is deleted.

### Deletion Order and Invariant

Segments are deleted oldest-first. The invariant that at least one segment (the active one) always exists is enforced by excluding the active segment from all eligibility calculations.

See [sequence/storage-retention.md](./sequence/storage-retention.md).

---

## 8. Concurrency Model

### Goroutines and Their Roles

| Goroutine | Operations | Notes |
|-----------|-----------|-------|
| dragonboat FSM thread (via `Update`) | `Append` | Sole writer; serialised by dragonboat |
| dragonboat FSM thread (via `Lookup`) | `Read`, `ReadByTime`, `EarliestOffset`, `LatestOffset` | Concurrent with Append |
| Data Coordinator fetch goroutines | Same read operations via `FSM.Lookup` | Multiple concurrent callers possible |
| Background retention goroutine | `EnforceRetention`, reads `retentionConfig` | One goroutine; runs infrequently |

### Locking

| Lock | Protects | Holders |
|------|---------|---------|
| `segMu sync.RWMutex` | `segments []SegmentStorage`, `nextOffset` | Write: Append (entire duration), EnforceRetention (per-deletion, slice mutation only). Read: all read ops (snapshot only; released before file I/O). |
| `chanMu sync.Mutex` | `newDataCh chan struct{}` | Append (channel replace), NewDataCh (channel snapshot) |
| `retMu sync.Mutex` | `retentionMs`, `retentionBytes` | SetRetentionConfig, retention goroutine |

### Why a Single Writer is Acceptable

dragonboat guarantees that `IOnDiskStateMachine.Update()` is never invoked concurrently on the same instance. Therefore `Append` is called by exactly one goroutine at a time and `segMu.Lock()` in `Append` has zero contention from other writers. The only write-lock contender is the retention goroutine, which holds the lock only to splice a pointer out of a slice — O(n) where n is the number of segments (≤ a few dozen) — not during the slower file removal.

### Active Segment Read Safety

`Read` and `Append` may execute concurrently on the active segment's `.log` file. This is safe because:

- The FSM's `Lookup` path is bounded by `lastAppliedIndex`, which is updated by the FSM only after a successful `Append` returns. Therefore `Read` never requests bytes beyond the last committed write position.
- On Linux, `pread` and a concurrent `write` (via `O_APPEND`) to the same open file descriptor operate on non-overlapping byte ranges by the time the read is issued (since the read boundary is below the write frontier). No torn reads can occur.

### mmap Index Concurrency

The active segment's index mmap is written by the single Append goroutine and read by concurrent readers during binary search. The `entryCount` field (number of valid index entries) is stored as an `atomic.Int64`. The writer increments `entryCount` after writing the entry into the mmap slice. Readers load `entryCount` first, then binary-search within that range. Go's atomic operations provide sufficient memory ordering on x86/arm64 to make this safe without an additional mutex.

---

## 9. Failure Modes

### Append Errors

| Error | Cause | Action |
|-------|-------|--------|
| `ENOSPC` (disk full) | No space left on device | Panic. Let the process supervisor restart the node. The Raft entry will be re-applied after restart. |
| Segment roll failure | Cannot create new `.log`/`.index`/`.timeindex` | Panic. Same recovery path as `ENOSPC`. |
| Other I/O error | Unexpected kernel error during write | Panic. Same recovery path. |

**Rationale:** dragonboat's contract for `IOnDiskStateMachine.Update()` is that it must either succeed or cause the node to restart. Returning an error causes dragonboat to halt the shard with no recovery path. Panicking triggers the supervisor, which restarts the node; the Raft log replay re-applies the failed entry through a fresh `Append`. Attempting error recovery inside `Update()` (e.g., retries) risks non-determinism across replicas.

### Read Errors

| Error | Cause | FSM/Coordinator action |
|-------|-------|----------------------|
| `ErrOffsetOutOfRange` | `offset < EarliestOffset()` — deleted by retention | Data Coordinator returns `OFFSET_OUT_OF_RANGE` to the client. Client must seek to `EarliestOffset`. |
| `(nil, offset, nil)` | `offset >= LatestOffset()` — not yet written | Data Coordinator enters long-poll via `NewDataCh`; see [modules §5](./01-modules.md). |
| I/O error reading sealed segment | Corrupt or missing sealed file | Return error to FSM. FSM returns error from `Lookup`. dragonboat propagates the error to the Data Coordinator, which returns a retriable error to the client. |

---

## 10. Configuration Parameters

| Config key | Type | Default | Description |
|------------|------|---------|-------------|
| `storage.segment_max_bytes` | int64 | 134 217 728 (128 MiB) | Log file size threshold that triggers a segment roll. |
| `storage.index_sample_bytes` | int | 4 096 (4 KiB) | Minimum log bytes between consecutive index entries (both offset and time indexes). |
| `storage.retention_check_interval_ms` | int64 | 300 000 (5 min) | How often the background retention goroutine fires. |
| `storage.default_retention_ms` | int64 | 604 800 000 (7 days) | Broker-wide default time retention threshold per partition. Overridable per topic via Raft retention-config command. |
| `storage.default_retention_bytes` | int64 | 1 073 741 824 (1 GiB) | Broker-wide default byte retention cap per partition. `-1` = unlimited. Overridable per topic. |

> `default_retention_ms` and `default_retention_bytes` are the initial values when a partition `Storage` is constructed. Per-topic overrides are delivered through the Partition FSM's `ApplyRetentionConfig` command (see [modules §6](./01-modules.md)) and applied via `SetRetentionConfig`.

---

## 11. Open Questions

1. **Retention — union semantics confirmed?** The current design deletes a sealed segment if it violates `retentionMs` OR `retentionBytes` (union, matching Kafka). Both constraints are required by [REQUIREMENTS.md §3.1.6](../REQUIREMENTS.md) and §4.1. `retentionBytes` is the primary enforced cap in typical deployments; `retentionMs` applies when data is old but the byte cap has not yet been reached. Confirm that union semantics are acceptable, or specify a different priority ordering.

> **Resolved decisions:** (1) Storage assigns `base_offset` in-place. (2) No `fsync` per `Append` — Raft log replay is the recovery mechanism. (3) No time-based segment roll in v1. (4) `fallocate` fallback logs `warn`.
