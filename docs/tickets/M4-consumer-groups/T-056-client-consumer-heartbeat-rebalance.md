# T-056: Client Consumer — background heartbeat goroutine and rebalance handling

**Milestone:** M4 — Consumer groups
**Effort:** M
**Status:** TODO

## Goal

Add the background heartbeat goroutine to `pkg/client.Consumer` (group mode): it sends `Heartbeat` RPCs every `HeartbeatIntervalMs`, detects `rebalance_required=true`, and triggers a re-`JoinGroup` + `initFetchOffsets` cycle; `Close` stops the goroutine cleanly.

## Context

A group consumer must send periodic heartbeats to the coordinator to signal liveness. If the coordinator detects the generation has changed (another member joined or left), it sets `rebalance_required=true` in the heartbeat response — the client must then re-issue `JoinGroup` to receive the new partition assignment. The heartbeat goroutine runs from the moment `Subscribe` is called in group mode until `Close`. On a NOT_LEADER heartbeat response the goroutine refreshes the coordinator address and retries.

References:
- [07-client-library.md §7 — Consumer](../../design/07-client-library.md#7-consumer)
- [08-consumer-groups.md §8 — Heartbeat flow](../../design/08-consumer-groups.md#8-heartbeat-flow)
- [08-consumer-groups.md §11 — Rebalance flow](../../design/08-consumer-groups.md#11-rebalance-flow)

## Scope

- Modify `pkg/client/consumer.go` (T-055's file):
  - Add fields to `Consumer` struct:
    - `stopHeartbeat context.CancelFunc`
    - `rebalanceCh chan struct{}` // buffered(1); signals Poll to pause and rebalance
    - `subscribedTopics []string` // set by Subscribe; needed for re-JoinGroup
  - Modify `Subscribe` (group mode) to start heartbeat goroutine after successful `JoinGroup`:
    ```
    ctx, cancel := context.WithCancel(context.Background())
    c.stopHeartbeat = cancel
    go c.heartbeatLoop(ctx)
    ```
  - `heartbeatLoop(ctx context.Context)`:
    - `ticker := time.NewTicker(time.Duration(config.HeartbeatIntervalMs) * time.Millisecond)`.
    - On each tick: call `DataService.Heartbeat(ctx, HeartbeatRequest{GroupID, MemberID, GenerationID})` on `coordAddr`.
    - On `NOT_LEADER`: call `findCoordinator(ctx)`; update `coordAddr`; continue.
    - On `UNAVAILABLE` / timeout: back off; continue.
    - On `NOT_GROUP_MEMBER` (evicted by session timeout): signal `rebalanceCh`; re-join via `rebalance(ctx)`.
    - On `rebalance_required=true` in response: signal `rebalanceCh`; call `rebalance(ctx)`.
    - On ctx cancel: return.
  - `rebalance(ctx context.Context)`:
    1. Drain old `soughtPartitions` (clear fetch offsets for previously assigned partitions).
    2. Re-issue `JoinGroup` on `coordAddr` (same as T-055 `Subscribe` join logic, reusing `c.memberID`).
    3. On success: update `memberID`, `generationID`, `assignedPartitions`.
    4. Call `initFetchOffsets(ctx, c.subscribedTopics)` to seek to committed positions.
    5. Send to `rebalanceCh` (non-blocking, already drained by Poll or callers).
  - Modify `Poll(ctx context.Context, maxWaitMs int64) ([]Record, error)`:
    - Group mode: before fetching, check `rebalanceCh` non-blocking; if signalled, wait for `rebalance` to complete (or use an `atomic.Bool` flag that `heartbeatLoop` sets and `Poll` clears after re-join completes).
  - Modify `Close() error`:
    - Group mode: call `c.stopHeartbeat()` to stop the goroutine; call `DataService.LeaveGroup(ctx, LeaveGroupRequest{GroupID, MemberID})` on `coordAddr` (best-effort, ignore error); then `pool.Close()`.

## Out of scope

- Subscribe / JoinGroup / initFetchOffsets core — T-055.
- CommitOffset / FetchCommittedOffsets — T-055.
- Server-side handlers — T-054.

## Definition of done

- [ ] `go build ./pkg/client/...` passes.
- [ ] `go test ./pkg/client/...` passes.
- [ ] `heartbeatLoop` sends `Heartbeat` RPC at approximately `HeartbeatIntervalMs` intervals.
- [ ] `heartbeatLoop` on `rebalance_required=true`: `rebalance()` called; new `generationID` stored.
- [ ] `rebalance()`: drains old `soughtPartitions`; calls `JoinGroup`; calls `initFetchOffsets`; new partitions seeked.
- [ ] `heartbeatLoop` on `NOT_LEADER`: `findCoordinator` called; next tick uses new `coordAddr`.
- [ ] `Close()` stops heartbeat goroutine within 2× `HeartbeatIntervalMs`.
- [ ] `Close()` sends `LeaveGroup` RPC to coordinator (best-effort).
- [ ] `Poll` in group mode: if rebalance in progress, waits until rebalance completes before fetching.

## Tests required

- `TestConsumer_Heartbeat_SentPeriodically` — stub records calls; advance time by 3 × HeartbeatIntervalMs; 3 heartbeat RPCs recorded.
- `TestConsumer_Heartbeat_RebalanceRequired_TriggersRejoin` — heartbeat stub returns `rebalance_required=true`; JoinGroup stub called; new generationID updated in consumer.
- `TestConsumer_Heartbeat_NotLeader_RefreshesCoord` — heartbeat stub returns `NOT_LEADER`; `findCoordinator` stub called; next heartbeat sent to new address.
- `TestConsumer_Heartbeat_NotGroupMember_Rebalance` — heartbeat returns `NOT_GROUP_MEMBER` (evicted); `rebalance()` called; consumer re-joins.
- `TestConsumer_Close_StopsHeartbeat` — `Close()` called; heartbeat goroutine exits within deadline.
- `TestConsumer_Close_SendsLeaveGroup` — `Close()` stub records `LeaveGroup` RPC was sent.
- `TestConsumer_Poll_WaitsForRebalance` — rebalance flag set before Poll; Poll blocks until rebalance completes; returns records from new assignment.

## Dependencies

- T-055 (Consumer group mode core: Subscribe, JoinGroup, initFetchOffsets, coordAddr field).
- T-043 (ConnPool for coordinator connection).
- T-034 (proto stubs: HeartbeatRequest/Response, LeaveGroupRequest).

## Notes

The coordination between `heartbeatLoop` and `Poll` around rebalance is the trickiest concurrency aspect. A simple approach: use an `atomic.Bool` (`c.rebalancing`) that `heartbeatLoop` sets to `true` before calling `rebalance()` and `rebalance()` sets back to `false` on completion. `Poll` checks `c.rebalancing.Load()` at the top of its loop and parks on a short `time.Sleep` + retry until `false`. This avoids a channel-based handshake but requires `rebalance()` to be called synchronously from `heartbeatLoop` (not in a separate goroutine) so `rebalancing` is cleared before `Poll` resumes. Document this invariant in a code comment.
