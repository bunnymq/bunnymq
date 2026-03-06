# T-002: VERIFY dragonboat v4 — GetLeaderID and leader query API

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

## Goal

Confirm the exact signature of dragonboat v4's leader-query method on `NodeHost` and determine whether the returned value can serve as `LeaderEpoch` in `AssignPartitionLeaderCmd`.

## Context

Two design documents reference a `GetLeaderID`-style call with differing proposed signatures:

- `04-cluster-coordinator.md §6` proposes `NodeHost.GetLeaderID(clusterID uint64) (leaderID uint64, term uint64, valid bool)`.
- `08-consumer-groups.md §9` proposes `nh.GetLeaderID(shardID) (leaderID uint64, valid bool, err error)`.

The ClusterCoordinator's leader sweep (`04-CC.md §6`) uses this call to detect leadership changes and propose `AssignPartitionLeaderCmd` with a `LeaderEpoch`. The GroupCoordinator's session timeout sweep (`08-consumer-groups.md §9`) uses it to guard against running the sweep on a non-leader node.

References:
- [04-cluster-coordinator.md §6](../../design/04-cluster-coordinator.md#6-leader-epoch-tracking)
- [08-consumer-groups.md §9](../../design/08-consumer-groups.md#9-session-timeout-enforcement)

## Scope

- Inspect dragonboat v4 `NodeHost` exported methods for leader queries.
- Confirm exact method name, parameter types, and return types.
- Determine whether a `term` or equivalent value is returned; if so, confirm it is monotonically increasing and suitable for use as `LeaderEpoch` in `AssignPartitionLeaderCmd`.
- Document the correct call pattern for: (a) ClusterCoordinator leader sweep, (b) GroupCoordinator sweep guard.

## Out of scope

- Implementing either sweep loop — covered in M3 ClusterCoordinator and M4 GroupCoordinator tickets.

## Definition of done

- [ ] Exact dragonboat v4 method signature for leader query documented (name, params, returns).
- [ ] Confirmed whether a `term` or epoch equivalent is returned.
- [ ] Decision documented: what value to use as `LeaderEpoch` in `AssignPartitionLeaderCmd`.
- [ ] Call patterns for CC leader sweep and GC sweep guard documented.

## Tests required

N/A — research ticket. No executable test required.

## Dependencies

None.

## Notes

Look in `github.com/lni/dragonboat/v4` for `NodeHost.GetLeaderID` or similar. Check whether the method is on `NodeHost` directly or on an `INodeHost` interface. The `valid bool` return likely indicates whether an election is in progress. If no `term` is returned, the `LeaderEpoch` can be derived from a monotonically increasing counter maintained in the coordinator rather than from dragonboat.
