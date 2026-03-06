# T-055: Client Consumer — group mode core (Subscribe/JoinGroup, coordinator discovery, Commit)

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Extend `pkg/client.Consumer` to support group mode (`GroupID != ""`): `Subscribe` discovers the group coordinator via `DescribeCluster` and issues `JoinGroup`; after join, `initFetchOffsets` fetches committed offsets and seeks each assigned partition; `Commit` / `CommitOffsets` send `CommitOffset` RPCs to the coordinator.

## Context

In M3, `Consumer` operated in manual mode only (T-046): `Seek` required before `Poll`, no `JoinGroup`, no heartbeat, no rebalance. This ticket adds the group-mode entry path. The background heartbeat goroutine and rebalance handling are separate concerns added in T-056. This ticket covers the one-time operations: coordinator discovery, join, offset initialisation, and commit. Group mode is activated when `ConsumerConfig.GroupID != ""`.

References:
- [07-client-library.md §7 — Consumer](../../design/07-client-library.md#7-consumer)
- [07-client-library.md §7.5 — Manual consumer](../../design/07-client-library.md#75-manual-consumer-no-groupid)
- [08-consumer-groups.md §3 — Coordinator discovery](../../design/08-consumer-groups.md#3-coordinator-discovery)
- [08-consumer-groups.md §7 — JoinGroup flow](../../design/08-consumer-groups.md#7-joingroup-flow)
- [08-consumer-groups.md §12 — Offset commit flow](../../design/08-consumer-groups.md#12-offset-commit-flow)

## Scope

- Modify `pkg/client/consumer.go` (T-046's file):
  - Add fields to `Consumer` struct:
    - `memberID string`
    - `generationID int32`
    - `coordAddr string` // resolved coordinator address
    - `assignedPartitions []TopicPartition` // from JoinGroup response
  - Modify `Subscribe(topics []string) error`:
    - Manual mode (`GroupID == ""`): unchanged (stores topic list, no RPC).
    - Group mode (`GroupID != ""`):
      1. `findCoordinator(ctx)` — call `ManagementService.DescribeCluster` on any bootstrap server; extract `metadata_leader_node_id`; resolve its data address from node list; store in `coordAddr`.
      2. Call `DataService.JoinGroup(ctx, JoinGroupRequest{GroupID, MemberID: c.memberID, Topics: topics, SessionTimeoutMs, HeartbeatIntervalMs})` on `coordAddr`.
      3. On `NOT_LEADER` response: refresh coordinator via `DescribeCluster`; retry once.
      4. Store `memberID`, `generationID`, `assignedPartitions` from response.
      5. Call `initFetchOffsets(ctx, topics)`.
  - `initFetchOffsets(ctx context.Context, topics []string)`:
    - `DataService.FetchCommittedOffsets(ctx, {GroupID, Partitions: assignedPartitions})` on `coordAddr`.
    - For each assigned partition:
      - If committed offset > -1: `Seek(topic, partitionID, committedOffset)`.
      - Else apply `AutoOffsetReset`: `EARLIEST` → `Seek(topic, partitionID, 0)`; `LATEST` → fetch `GetOffsets(LATEST)` → `Seek(topic, partitionID, latestOffset)`.
  - Modify `Commit(ctx context.Context) error`:
    - Manual mode: return nil (unchanged).
    - Group mode: call `CommitOffsets(ctx, currentFetchOffsets)`.
  - Modify `CommitOffsets(ctx context.Context, offsets map[TP]int64) error`:
    - Manual mode: return nil (unchanged).
    - Group mode: `DataService.CommitOffset(ctx, CommitOffsetRequest{GroupID, MemberID, GenerationID, Offsets})` on `coordAddr`; on `STALE_GENERATION` → return typed `ErrStaleGeneration` to caller.
  - `findCoordinator(ctx context.Context) error` — helper; updates `coordAddr`.
- Add to `pkg/client/types.go` (or `record.go`):
  - `ErrStaleGeneration` sentinel error.
  - `OffsetResetPolicy` enum (`EARLIEST`, `LATEST`) if not already in T-046.

## Out of scope

- Background heartbeat goroutine and rebalance loop — T-056.
- `Poll` in group mode is unchanged from T-046 — it fetches from `soughtPartitions` which are populated by `Seek` calls from `initFetchOffsets`.
- Server-side handlers — T-054.

## Definition of done

- [ ] `go build ./pkg/client/...` passes.
- [ ] `go test ./pkg/client/...` passes.
- [ ] `Subscribe` in group mode: `JoinGroup` RPC called on coordinator address.
- [ ] `Subscribe` in group mode: `MemberID` stored from response.
- [ ] `initFetchOffsets` with committed offset > -1: `Seek` to committed offset.
- [ ] `initFetchOffsets` with committed offset = -1 and `EARLIEST` policy: `Seek` to offset 0.
- [ ] `initFetchOffsets` with committed offset = -1 and `LATEST` policy: `Seek` to latest offset fetched from server.
- [ ] `CommitOffsets` in group mode: `CommitOffset` RPC called with correct `GenerationID`.
- [ ] `CommitOffsets` with `STALE_GENERATION` response: `ErrStaleGeneration` returned to caller.
- [ ] `Subscribe` with `NOT_LEADER` on first coordinator attempt: refreshes coordinator and retries.

## Tests required

- `TestConsumer_GroupMode_Subscribe_CallsJoinGroup` — stub coordinator `JoinGroup` returns 2 assigned partitions; `Subscribe` stores memberID + generationID; `soughtPartitions` has 2 entries after initFetchOffsets.
- `TestConsumer_GroupMode_InitFetchOffsets_Committed` — `FetchCommittedOffsets` stub returns offset 10; `Seek` called with offset 10.
- `TestConsumer_GroupMode_InitFetchOffsets_Earliest` — `FetchCommittedOffsets` returns -1; `AutoOffsetReset=EARLIEST`; `Seek(0)` called.
- `TestConsumer_GroupMode_InitFetchOffsets_Latest` — `FetchCommittedOffsets` returns -1; `AutoOffsetReset=LATEST`; `GetOffsets` stub returns 42; `Seek(42)` called.
- `TestConsumer_GroupMode_CommitOffsets_Success` — `CommitOffset` stub succeeds; no error returned.
- `TestConsumer_GroupMode_CommitOffsets_StaleGeneration` — `CommitOffset` stub returns `STALE_GENERATION`; `ErrStaleGeneration` returned.
- `TestConsumer_GroupMode_Subscribe_NotLeader_Retry` — first `JoinGroup` returns `NOT_LEADER`; `findCoordinator` refreshes; second `JoinGroup` to new address succeeds.
- `TestConsumer_ManualMode_CommitOffsets_Noop` — `GroupID == ""`; `CommitOffsets` returns nil without calling any RPC.

## Dependencies

- T-046 (Consumer struct and manual mode to extend).
- T-043 (ConnPool, MetaCache — coordinator address cached in MetaCache).
- T-034 (proto stubs: JoinGroupRequest/Response, CommitOffsetRequest, FetchCommittedOffsetsRequest).

## Notes

`findCoordinator` resolves the data-service address of the metadata shard leader, not the management-service address — because JoinGroup, CommitOffset, and FetchCommittedOffsets are `DataService` RPCs on port `:9092`. The `DescribeCluster` response carries `metadata_leader_node_id`; the consumer cross-references this with the node-address list in the response to find the `:9092` address of the leader. Cache this address in `MetaCache` (or the consumer's `coordAddr` field) with a short TTL; refresh on `NOT_LEADER`.
