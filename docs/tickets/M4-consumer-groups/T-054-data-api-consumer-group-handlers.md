# T-054: Data API gRPC — consumer group handlers and coordinator wiring

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Replace the `codes.Unimplemented` stubs in `DataServer` (T-037) with real implementations of the five consumer group RPCs (`JoinGroup`, `Heartbeat`, `LeaveGroup`, `CommitOffset`, `FetchCommittedOffsets`), wire `GroupCoordinator` into the server, and add NOT_LEADER routing for non-coordinator nodes.

## Context

The DataService gRPC server (T-037) already stubs all 8 RPCs; the consumer group five returned `Unimplemented` in M3. This ticket replaces those stubs with real dispatch to `GroupCoordinator`. The coordinator only runs on the metadata shard leader; non-leaders must return `NOT_LEADER` with the current leader's address (per `08-consumer-groups.md §3`). The `cmd/bunnymq` entry point must also be updated to instantiate `GroupCoordinator` and pass it to `DataServer`.

References:
- [08-consumer-groups.md §3 — Coordinator discovery](../../design/08-consumer-groups.md#3-coordinator-discovery)
- [08-consumer-groups.md §7–13 — RPC flows](../../design/08-consumer-groups.md#7-joingroup-flow)
- [06-api-protocol.md §4 — DataService proto definition](../../design/06-api-protocol.md#4-dataservice)

## Scope

- Modify `internal/api/data/server.go`:
  - Add `groupCoord GroupCoordinatorIface` field to `DataServer` struct.
  - Add `isMetadataLeader func() (bool, string)` helper (or inline): calls `nh.GetLeaderID(MetadataShardID)`; returns `(isLeader, leaderAddress)`; used by all 5 group handlers.
  - Replace each stub with a real handler:
    - `JoinGroup(ctx, req)`: call `isMetadataLeader`; if not leader → `FAILED_PRECONDITION` + `BunnyErrorDetail{NOT_LEADER, leaderAddr}`; else delegate to `groupCoord.JoinGroup`; map `ErrNotGroupMember` → `NOT_FOUND`; map `INVALID_ARGUMENT` from coordinator → `INVALID_ARGUMENT`; convert response to proto.
    - `Heartbeat(ctx, req)`: leader check; delegate to `groupCoord.Heartbeat`; map `ErrNotGroupMember` → `NOT_FOUND`; map response to proto.
    - `LeaveGroup(ctx, req)`: leader check; delegate to `groupCoord.LeaveGroup`; map errors.
    - `CommitOffset(ctx, req)`: leader check; delegate to `groupCoord.CommitOffset`; map `ErrStaleGeneration` → `FAILED_PRECONDITION + BunnyErrorDetail{STALE_GENERATION}`.
    - `FetchCommittedOffsets(ctx, req)`: leader check; delegate to `groupCoord.FetchCommittedOffsets`; build proto response from `map[TopicPartition]int64`.
- Modify `internal/api/data/errors.go` (or `mapping.go`): add error-to-status mappings for `ErrStaleGeneration`, `ErrNotGroupMember`, `ErrMixedSubscriptions`.
- Update `cmd/bunnymq/main.go` (T-010's file):
  - Instantiate `GroupCoordinator` with the NodeHost and metadata shard ID.
  - Call `groupCoord.RebuildHeartbeatTable()` after coordinator starts.
  - Start the sweep goroutine via `groupCoord.Start(ctx)`.
  - Pass `groupCoord` into `NewDataServer`.

## Out of scope

- GroupCoordinator business logic — T-051, T-052, T-053.
- Proto definitions — T-034.

## Definition of done

- [ ] `go build ./...` passes (including `cmd/bunnymq`).
- [ ] `go test ./internal/api/data/...` passes.
- [ ] `JoinGroup` on non-leader node returns `FAILED_PRECONDITION` with `NOT_LEADER` detail.
- [ ] `JoinGroup` on leader delegates to `GroupCoordinator`; proto response populated correctly.
- [ ] `Heartbeat` on leader: `rebalance_required` field in response matches coordinator result.
- [ ] `CommitOffset` with stale generation: response is `FAILED_PRECONDITION + STALE_GENERATION` detail.
- [ ] `FetchCommittedOffsets`: missing partition returns offset `-1` in proto response map.
- [ ] `cmd/bunnymq` builds and instantiates `GroupCoordinator`.

## Tests required

- `TestDataServer_JoinGroup_NotLeader` — stub `isMetadataLeader` returns false; response is `FAILED_PRECONDITION` with `NOT_LEADER` detail.
- `TestDataServer_JoinGroup_Success` — stub coordinator `JoinGroup` returns populated `JoinGroupResponse`; proto response matches.
- `TestDataServer_Heartbeat_RebalanceRequired` — coordinator returns `rebalance_required=true`; proto response field set.
- `TestDataServer_CommitOffset_StaleGeneration` — coordinator returns `ErrStaleGeneration`; gRPC status is `FAILED_PRECONDITION` with `STALE_GENERATION` detail.
- `TestDataServer_FetchCommittedOffsets_MissingPartition` — coordinator returns map with `-1` for a partition; proto response includes that entry.
- `TestDataServer_LeaveGroup_NotMember` — coordinator returns `ErrNotGroupMember`; gRPC `NOT_FOUND`.

## Dependencies

- T-037 (DataServer struct and proto wiring to extend).
- T-051 (GroupCoordinatorIface: JoinGroup, LeaveGroup).
- T-052 (GroupCoordinatorIface: Heartbeat, Start, RebuildHeartbeatTable).
- T-053 (GroupCoordinatorIface: CommitOffset, FetchCommittedOffsets).
- T-034 (proto stubs for consumer group RPCs — already generated in M3).

## Notes

The `GroupCoordinatorIface` interface is defined in T-051 and covers all 5 handler methods — use it as the `DataServer` field type so tests can inject a stub. The NOT_LEADER response for group RPCs must carry the metadata shard leader's management address (not the data address), because clients reconnect via `DescribeCluster` → coordinator address from node list. The exact address to return is read from `LookupMetadata(QueryGetNode{leaderID})` or from the ClusterCoordinator's cached node address.
