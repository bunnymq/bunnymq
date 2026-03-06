# T-003: VERIFY dragonboat v4 — StartCluster join semantics

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

## Goal

Confirm the correct dragonboat v4 `StartCluster` call parameters for (a) bootstrapping a brand-new shard simultaneously across all replica nodes, and (b) a node joining an already-initialized shard.

## Context

`04-cluster-coordinator.md §7.4` includes a VERIFY comment about join semantics and proposes a heuristic: the node with the lowest `NodeID` in `ReplicaNodeIDs` calls `StartCluster` with `join=false` and `initialMembers=all replicas`; other nodes call with `join=true`. Open Question 3 of `04-CC.md` asks to confirm whether this heuristic is correct, or whether all nodes should simultaneously call `StartCluster` with `join=false` and the full `initialMembers` map.

References:
- [04-cluster-coordinator.md §7.4](../../design/04-cluster-coordinator.md#74-startshard)
- [04-cluster-coordinator.md OQ3](../../design/04-cluster-coordinator.md#13-open-questions)
- [03-raft-fsm.md §1.2](../../design/03-raft-fsm.md#12-lifecycle)

## Scope

- Read dragonboat v4 `NodeHost.StartCluster` / `StartOnDiskCluster` documentation and source.
- Confirm the correct parameter combination when all replica nodes start a brand-new shard simultaneously: do all use `join=false` with `initialMembers` = full replica map, or does one node use `join=false` and others use `join=true`?
- Confirm the correct parameters when an existing shard is already running in the cluster and a fresh node needs to join it (e.g., after `CreateTopic`, non-lowest-NodeID replicas start their shard instances).
- Confirm or replace the lowest-NodeID heuristic.
- Document the correct procedure for both scenarios.

## Out of scope

- Implementing `startShard` in ClusterCoordinator — M3 ticket.

## Definition of done

- [ ] New shard bootstrap: correct `StartCluster` parameters documented for all replica nodes.
- [ ] Existing shard join: correct `StartCluster` parameters documented.
- [ ] The lowest-NodeID heuristic confirmed or replaced with the correct dragonboat-recommended approach.
- [ ] Decision noted for use in T-012 repository skeleton and future CC implementation tickets.

## Tests required

N/A — research ticket.

## Dependencies

None.

## Notes

dragonboat v4 may expose `StartOnDiskCluster` for `IOnDiskStateMachine` types (partition shards) and `StartCluster` for `IStateMachine` types (metadata shard). Verify whether there is a separate method for on-disk FSMs. Also confirm: does dragonboat v4 require `join=true` nodes to know the `initialMembers` map, or can it be nil when joining an existing cluster?
