# T-046: Client library — basic Consumer (manual mode, no group)

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** M
**Status:** TODO

## Goal

Implement `pkg/client.Consumer` for manual (no-GroupID) mode: `Subscribe`, `Poll`, `Seek`, `Commit` (no-op), `Close`, including the batch decoder that converts `FetchResponse.records` to `[]Record`.

## Context

In M3 the Consumer operates in manual mode: no `JoinGroup`, no heartbeat goroutine, no rebalance. The caller specifies topics via `Subscribe`, sets the starting offset per partition via `Seek`, and calls `Poll` to fetch records. Consumer group support (automatic partition assignment, heartbeat, rebalance) lands in M4. This ticket delivers a working consumer for the M3 end-to-end smoke test.

References:
- [07-client-library.md §7 — Consumer](../../design/07-client-library.md#7-consumer)
- [07-client-library.md §7.5 — Manual consumer](../../design/07-client-library.md#75-manual-consumer-no-groupid)
- [07-client-library.md §5 — Batch decoder](../../design/07-client-library.md#5-batch-encoder--decoder)

## Scope

- Create `pkg/client/consumer.go`:
  - `ConsumerConfig` struct (embeds `Config`): `GroupID string` (empty = manual), `MaxFetchBytes int`, `MaxFetchWaitMs int64`, `AutoOffsetReset OffsetResetPolicy`.
  - `Consumer` struct: `config ConsumerConfig`, `pool *iclient.ConnPool`, `meta *iclient.MetaCache`, `fetchOffsets map[TP]int64`, `soughtPartitions []TP`.
  - `NewConsumer(config ConsumerConfig) (*Consumer, error)`.
  - `Subscribe(topics []string) error` — manual mode: records topic list; no JoinGroup; no heartbeat.
  - `Seek(topic string, partitionID int32, offset int64)` — sets `fetchOffsets[TP{topic, partitionID}]`.
  - `Poll(ctx context.Context, maxWaitMs int64) ([]Record, error)`:
    - Iterate `soughtPartitions` (partitions that have been `Seek`'d).
    - For each: look up leader address from `MetaCache`; call `DataService.Fetch(ctx, topic, partitionID, fetchOffsets[tp], MaxFetchBytes, perPartitionWaitMs)`.
    - On `NOT_LEADER`: update cache from `NotLeaderDetail`; skip partition (retry on next Poll).
    - On success: decode `FetchResponse.records` via `BatchDecoder`; advance `fetchOffsets[tp] = nextOffset`; append decoded records.
    - Return accumulated `[]Record`.
  - `Commit(ctx) error` — returns nil (no-op in manual mode).
  - `CommitOffsets(ctx, offsets map[TP]int64) error` — returns nil.
  - `Close() error` — `pool.Close()`.
- Create `internal/client/batch_decoder.go`:
  - `BatchDecoder.Decode(data []byte) ([]Record, error)` — parse consecutive batches; CRC-32C verify; decode records with offset/timestamp delta reconstruction.
  - Returns partial results (records from valid batches before the first bad CRC).

## Out of scope

- Group consumer (JoinGroup, heartbeat, rebalance) — M4.
- `initFetchOffsets` (committed offset initialization) — M4 (requires `FetchCommittedOffsets` server API).

## Definition of done

- [ ] `go build ./pkg/client/...` passes.
- [ ] `go test ./pkg/client/...` passes.
- [ ] `Seek` + `Poll`: Fetch RPC called with the seeked offset.
- [ ] `Poll` returns decoded `[]Record` with correct `Offset`, `Key`, `Value`, `TimestampMs`.
- [ ] `Poll` advances `fetchOffsets` to `nextOffset` after each successful fetch.
- [ ] `Poll` on `NOT_LEADER`: cache updated; partition skipped; no error returned to caller.
- [ ] `BatchDecoder.Decode`: multi-batch response decoded into flat `[]Record`.
- [ ] `BatchDecoder.Decode`: CRC mismatch on second batch returns records from first batch + error.

## Tests required

- `TestConsumer_Seek_Poll_ReturnsRecords` — Seek offset=0; Poll with stub Fetch returning encoded batch; decoded records returned.
- `TestConsumer_Poll_AdvancesOffset` — after Poll returns 3 records (offsets 0,1,2); next Poll fetches from offset 3.
- `TestConsumer_Poll_NotLeader_SkipsPartition` — Fetch stub returns NOT_LEADER; `Poll` returns empty slice (not error); meta cache updated.
- `TestConsumer_Poll_MultiPartition` — Seek 2 partitions; Poll fetches both; records from both returned.
- `TestBatchDecoder_MultiBatch` — two back-to-back encoded batches; Decode returns all records from both.
- `TestBatchDecoder_CRCMismatch` — corrupt second batch; Decode returns first batch's records + error.
- `TestBatchDecoder_SingleRecord` — single-record batch; correct offset + timestamp reconstruction.

## Dependencies

T-034 (proto stubs).
T-043 (ConnPool, MetaCache).
T-015 (EncodeBatch — used in tests to produce valid encoded batches for the decoder).

## Notes

`BatchDecoder` is the client-side counterpart of `DecodeBatch` from T-015. It parses the on-wire format: read 38-byte header → `batch_length` → CRC verify → decode records (varint zigzag for `offset_delta` and `timestamp_delta`). The decoded `Record.Offset = base_offset + offset_delta`; `Record.TimestampMs = base_timestamp + timestamp_delta`. The `soughtPartitions` slice is populated by `Seek` calls — only partitions that have been Seek'd are fetched in `Poll`. This means `Subscribe` alone (without `Seek`) results in an empty `Poll` in manual mode, which is intentional and documented behaviour.
