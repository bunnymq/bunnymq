# T-050: Range-based partition assignment algorithm

**Milestone:** M4 — Consumer groups
**Effort:** S
**Status:** TODO

## Goal

Implement `rangeAssign`, a pure function that maps (sorted member IDs, topic→partitionCount) to (memberID→[]TopicPartition), using the range-based algorithm from `08-consumer-groups.md §6`.

## Context

Every membership change (join, leave, session-timeout eviction) requires computing a fresh partition assignment. The algorithm is described verbatim in `08-consumer-groups.md §6`: for each topic, sort members lexicographically; divide partitions into contiguous ranges; give the first `remainder` members one extra partition. This is a pure utility function with no side effects, isolated in its own file for easy testing and reuse by the GroupCoordinator handlers in T-051 and T-052.

References:
- [08-consumer-groups.md §6 — Range-based partition assignment](../../design/08-consumer-groups.md#6-range-based-partition-assignment)
- [08-consumer-groups.md §7 — When assignment is computed](../../design/08-consumer-groups.md#when-assignment-is-computed)

## Scope

- Create `internal/cluster/assignment.go`:
  - `rangeAssign(memberIDs []string, topics []string, partitionCounts map[string]int32) map[string][]TopicPartition`:
    - Sort `memberIDs` lexicographically (do not mutate caller's slice).
    - For each topic in `topics` (in deterministic order — sort topics too):
      - `n_partitions = partitionCounts[topic]`, `n_members = len(memberIDs)`.
      - Handle edge cases: `n_members == 0` → return empty map; `n_partitions == 0` → skip topic.
      - `base = n_partitions / n_members`, `remainder = n_partitions % n_members`.
      - Assign ranges: first `remainder` members get `base+1` partitions; remainder get `base`.
    - Returns `map[string][]TopicPartition` (memberID → assigned partitions across all topics).
  - Uses `TopicPartition` type from T-049.

## Out of scope

- Sticky assignment — not in v1.
- Mixed-subscription validation — T-051 handles the guard; this function assumes inputs are valid.
- GroupCoordinator integration — T-051.

## Definition of done

- [ ] `go build ./internal/cluster/...` passes.
- [ ] `go test ./internal/cluster/...` passes.
- [ ] 8 partitions, 3 members → [3, 3, 2] distribution (per §6 example).
- [ ] 1 partition, 3 members → one member gets 1 partition; others get 0 (not omitted, present with empty slice).
- [ ] 3 partitions, 3 members → [1, 1, 1].
- [ ] 0 members → empty map returned without panic.
- [ ] Multiple topics → each topic assigned independently; total per member is sum across topics.

## Tests required

- `TestRangeAssign_EightPartitionsThreeMembers` — example from §6: 8P, 3M → m-a:[0,1,2], m-b:[3,4,5], m-c:[6,7].
- `TestRangeAssign_EvenDistribution` — 6P, 3M → [2, 2, 2].
- `TestRangeAssign_MoreMembersThanPartitions` — 1P, 3M → exactly one member gets the partition; other two get empty slice (not absent from map).
- `TestRangeAssign_SingleMember` — all partitions go to the single member.
- `TestRangeAssign_ZeroMembers` — returns empty map, no panic.
- `TestRangeAssign_MultiTopic` — 2 topics each with 4 partitions, 2 members → each member gets 2 partitions per topic.
- `TestRangeAssign_Deterministic` — same inputs in different map iteration order still produce identical output.

## Dependencies

- T-049 (`TopicPartition` type).

## Notes

Sort both `memberIDs` and `topics` at the start of `rangeAssign` to guarantee determinism regardless of the caller's ordering. The algorithm processes topics in sorted order so that the assignment is the same whether the coordinator proposes the command on node A or the same command is replayed on node B during log replay. Do not mutate the input slices.
