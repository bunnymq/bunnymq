# storage-retention: Retention Enforcement Loop

`Storage.Open` starts a background goroutine that fires every `retention_check_interval_ms` (default 5 minutes) and calls `EnforceRetention`. On each tick the goroutine reads the current retention configuration (which may have been updated via `SetRetentionConfig`), snapshots the segment list under a read lock, evaluates both time and bytes constraints against sealed segments, and deletes eligible segments oldest-first. The active segment is never deleted. Segment list mutations (removing a pointer from the slice) acquire the write lock only for the slice update; the slower file-deletion syscalls run outside the lock.

```mermaid
sequenceDiagram
    participant BG as RetentionGoroutine
    participant S as Storage
    participant FS as Filesystem

    loop every retention_check_interval_ms
        BG->>S: tick
        S->>S: read retentionMs, retentionBytes (under retMu)

        S->>S: acquire segMu read lock
        S->>S: snapshot segments slice (N segments, last is active)
        S->>S: release segMu read lock

        Note over S: Evaluate time constraint against sealed segments [0..N-2]
        loop for i = 0 to N-2 (sealed segments)
            S->>S: nextSegFirstTimestamp = segments[i+1].firstBatchBaseTimestamp()
            alt nextSegFirstTimestamp < now() - retentionMs
                S->>S: mark segments[i] as expired-by-time
            end
        end

        Note over S: Evaluate bytes constraint against sealed segments [0..N-2]
        S->>S: totalBytes = sum of all segment logSizes
        S->>S: accum = 0
        loop for i = 0 to N-2 while totalBytes - accum > retentionBytes
            S->>S: accum += segments[i].logSize
            S->>S: mark segments[i] as expired-by-bytes
        end

        Note over S: Delete union of expired-by-time and expired-by-bytes, oldest first
        loop for each marked segment seg (ascending base_offset order)
            S->>S: acquire segMu write lock
            S->>S: remove seg from segments slice
            S->>S: release segMu write lock

            S->>FS: seg.Close()
            FS-->>S: munmap .index, .timeindex; close fds
            S->>FS: os.Remove(<baseOffset>.log)
            S->>FS: os.Remove(<baseOffset>.index)
            S->>FS: os.Remove(<baseOffset>.timeindex)
            FS-->>S: ok
            S->>S: deletedSegments++
        end

        S->>S: log info "retention: deleted N segments"
    end
```

## Participants

| Participant | Role |
|-------------|------|
| `RetentionGoroutine` | Internal goroutine started by `Storage.Open`; drives the periodic enforcement tick. Stopped when `Storage.Close` closes the done channel. |
| `Storage` | Executes `EnforceRetention`; owns the segment list and retention config. |
| `Filesystem` | Executes `munmap`, `close`, and `os.Remove` for deleted segments. |

## Edge Cases

- **Only the active segment exists:** If `N == 1` (no sealed segments), both constraint loops are skipped. `EnforceRetention` returns immediately with `deletedSegments = 0`. The invariant that at least one segment remains is naturally maintained.
- **retentionBytes == -1 (unlimited):** The bytes constraint loop is skipped. Only time-based expiration applies.
- **retentionMs == 0:** All sealed segments whose next segment has any non-zero timestamp are immediately eligible for time-based deletion. This effectively clears all data except the active segment. This configuration is valid but unusual; the operator is responsible for setting reasonable values.
- **Segment deleted externally between snapshot and removal:** `EnforceRetention` snapshots the list and then attempts file deletion. If a file is already absent (e.g., operator manually removed it), `os.Remove` returns `ENOENT`, which is treated as a no-op (the segment was already removed). The slice mutation still proceeds.
- **firstBatchBaseTimestamp for the last sealed segment:** `segments[N-2+1]` is the active segment. Its `firstBatchBaseTimestamp()` is the `base_timestamp` of the first batch in the active log (read from file offset 22, bytes [22:30] of the first batch header). If the active segment is empty (just created), the timestamp is undefined; that sealed segment is not eligible for time-based deletion in this tick.
- **Concurrent SetRetentionConfig:** The retention goroutine reads the configuration under `retMu` at the start of each tick. A `SetRetentionConfig` call during the tick applies on the next tick, not mid-evaluation. This is intentional: partially updated retention config mid-loop would produce inconsistent eligibility decisions.
- **Logging:** Each deletion is logged at `info` level with segment base_offset, reason (time|bytes|both), and size in bytes. This satisfies the logging contract from [09-metrics-logging.md](../09-metrics-logging.md).
