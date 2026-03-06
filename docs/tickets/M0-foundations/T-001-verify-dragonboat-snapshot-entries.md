# T-001: VERIFY dragonboat v4 — SnapshotEntries large value and metadata shard value

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

## Goal

Confirm that dragonboat v4 accepts `SnapshotEntries = 1 << 62` without overflow or validation error for partition shards, and decide the correct `SnapshotEntries` value for the metadata shard.

## Context

`03-raft-fsm.md §1.1` sets `SnapshotEntries = 1 << 62` and `CompactionOverhead = 1 << 62` on partition shard `RaftConfig` to effectively disable automatic snapshots (Strategy A). Open Question 1 of `03-raft-fsm.md` asks to verify dragonboat v4 accepts this value. Open Question 2 asks to confirm `SnapshotEntries = 10_000` for the metadata shard (where periodic snapshots are beneficial to bound restart replay time).

References:
- [03-raft-fsm.md §1.1](../../design/03-raft-fsm.md#11-configuration) — RaftConfig defaults and Strategy A rationale
- [03-raft-fsm.md §4.6](../../design/03-raft-fsm.md#46-snapshot-strategy--strategy-a-v1) — Strategy A definition

## Scope

- Read dragonboat v4 `config.Config` struct definition and any validation logic in `NodeHost.StartCluster` / `StartOnDiskCluster` to determine accepted range for `SnapshotEntries` and `CompactionOverhead`.
- Confirm: does dragonboat clamp, reject, or panic on `1 << 62`? If rejected, find the practical maximum or the supported mechanism to disable auto-snapshots.
- Confirm or propose an alternative value for the metadata shard `SnapshotEntries` (10,000 entries proposed; adjust if the dragonboat-recommended approach differs).
- Document findings in a decision note that future implementation tickets can reference (e.g., as a constant block comment in `internal/raft/config.go` created in T-012).

## Out of scope

- Implementing snapshot logic — see M2 FSM tickets.
- Testing snapshot behavior end-to-end.

## Definition of done

- [ ] Accepted value for partition shard `SnapshotEntries` documented (confirm `1 << 62` or provide alternative).
- [ ] Accepted value for partition shard `CompactionOverhead` documented.
- [ ] Metadata shard `SnapshotEntries` value decided and documented.
- [ ] If `1 << 62` is rejected: alternative mechanism (specific max value, or `DisableAutoCompaction` option if available) identified and documented.

## Tests required

N/A — research ticket. Output is a documented decision used to fill in constants in T-012 (repository skeleton). No executable Go test is required. A minimal scratch `_test.go` calling `config.Config{SnapshotEntries: 1 << 62}` and passing it through any validation function may be written as part of investigation but is not a deliverable.

## Dependencies

None.

## Notes

Look at `github.com/lni/dragonboat/v4/config` package. The field is `config.Config.SnapshotEntries uint64`. Check whether there is any `sanityCheck()` or `validate()` function called during `NodeHost.StartCluster` that validates this field. Also verify that `CompactionOverhead` set to `1 << 62` does not conflict with `SnapshotEntries`.

---

## Resolution

**Investigated:** dragonboat v4 source (`config/config.go`, `config/validator.go`, `nodehost.go`) via GitHub (lni/dragonboat, v4 branch).

### Finding 1 — `SnapshotEntries = 1 << 62` is accepted, but `0` is the correct disable mechanism

dragonboat v4's `Config.Validate()` performs **no bounds check** on `SnapshotEntries` or `CompactionOverhead`. The only snapshot-related guard is:

```go
if c.IsWitness && c.SnapshotEntries > 0 {
    return errors.New("witness node can not take snapshot")
}
```

So `SnapshotEntries = 1 << 62` does not panic or return a validation error for regular (non-witness) nodes. However, the dragonboat-documented mechanism to disable automatic snapshots is:

> "Once automatic snapshotting is disabled by setting the `SnapshotEntries` field to **0**, users can still use `NodeHost.RequestSnapshot` or `SyncRequestSnapshot` to manually request snapshots."

**Decision for partition shards:** Use `SnapshotEntries = 0` (the documented disable mechanism) rather than `1 << 62`. Both work, but `0` is idiomatic and avoids any hypothetical future overflow in index arithmetic (`index <= n.config.SnapshotEntries + applied`).

### Finding 2 — `CompactionOverhead = 1 << 62` is accepted

No bounds check exists for `CompactionOverhead`. The compaction logic uses comparison (`if index > n.config.CompactionOverhead`), so no arithmetic overflow occurs at runtime. However, since partition shards use `SnapshotEntries = 0` (no automatic snapshots), compaction is never triggered automatically, making `CompactionOverhead` irrelevant in practice for Strategy A.

**Decision for partition shards:** Keep `CompactionOverhead = 0` (default). Additionally, `DisableAutoCompactions = true` can be set to make the intent explicit, though it has no functional effect when `SnapshotEntries = 0`.

### Finding 3 — Metadata shard `SnapshotEntries = 10_000` is confirmed

No upper-bound validation exists. `10_000` is a safe, reasonable value for the metadata shard and aligns with the design doc's proposal. It bounds restart replay to at most 10 000 metadata commands.

**Decision for metadata shard:** `SnapshotEntries = 10_000`. No change from design doc proposal.

### Constants for T-012 (`internal/raft/config.go`)

```go
const (
    // PartitionSnapshotEntries disables automatic snapshots on partition shards
    // (Strategy A). dragonboat v4 treats 0 as "auto-snapshots off".
    PartitionSnapshotEntries uint64 = 0

    // PartitionCompactionOverhead is irrelevant when SnapshotEntries = 0
    // (compaction is never triggered). Set to 0 (default).
    PartitionCompactionOverhead uint64 = 0

    // MetadataSnapshotEntries triggers a metadata snapshot every 10 000 committed
    // entries, bounding restart replay time.
    MetadataSnapshotEntries uint64 = 10_000
)
```

### DoD checklist

- [x] Accepted value for partition shard `SnapshotEntries` documented — use `0` (disable auto-snapshots).
- [x] Accepted value for partition shard `CompactionOverhead` documented — use `0`; `DisableAutoCompactions = true` optional.
- [x] Metadata shard `SnapshotEntries` value decided — confirmed `10_000`.
- [x] Alternative mechanism identified — `SnapshotEntries = 0` is the documented disable path; `1 << 62` also accepted but non-idiomatic.
