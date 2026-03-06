# T-015: Batch encoder/decoder + test vectors

**Milestone:** M1 — Storage standalone
**Effort:** M
**Status:** TODO

## Goal

Implement `BatchEncoder` and `BatchDecoder` in `internal/storage` that produce and consume the exact on-disk/on-wire batch format from `02-storage.md §3.2`, validated by fixed byte-level test vectors.

## Context

Every layer of BunnyMQ that touches message data uses this batch format: Storage writes it, the Partition FSM applies it, the Data API forwards it on the wire, and the client library assembles it. Getting the encoding exactly right — fields, endianness, varint encoding for records, CRC-32C scope — is the single most critical correctness invariant in the system.

References:
- [02-storage.md §3.2 — Batch format](../../design/02-storage.md#32-batch-format)
- [REQUIREMENTS.md §4.4 — Wire batch format](../../design/REQUIREMENTS.md#44-wire-batch-format)

## Scope

- Implement `EncodeBatch(records []Record) ([]byte, error)` in `internal/storage`:
  - Writes the 38-byte fixed header: `base_offset` (set to 0 at encode time; overwritten by Storage.Append), `batch_length`, `record_count`, `crc32` (CRC-32C over bytes [38, batch_length)), `attributes` (0), `base_timestamp`, `max_timestamp`.
  - Appends each `Record` in varint-encoded format: `length`, `attributes` (0), `timestamp_delta`, `offset_delta`, `key_length`, `key`, `value_length`, `value`, `headers_count`, then each header as `key_length + key + value_length + value`.
  - All fixed-width fields big-endian; per `02-storage.md §3.2`.
- Implement `DecodeBatch(data []byte) (*Batch, error)`:
  - Parses the 38-byte header into `BatchHeader` struct fields.
  - Re-computes CRC-32C and returns `ErrCRCMismatch` on mismatch.
  - Decodes each varint-encoded `Record`.
- Implement `DecodeNextBatch(data []byte, pos int) (*Batch, int, error)` for sequential log scanning.
- Define the `Record`, `RecordHeader`, `BatchHeader`, and `Batch` types in `internal/storage/batch.go`.
- Write test vectors in `internal/storage/batch_test.go`:
  - At least one fixed-byte golden test: a known `Record` slice encodes to a known hex byte string; the inverse decodes back to the original `Record` slice with CRC check passing.
  - Test for nil key (`key_length = -1`).
  - Test for multi-record batch.
  - Test for batch with record headers.
  - Test for CRC mismatch detection (corrupt byte → `ErrCRCMismatch`).
  - Test for truncated input detection.

## Out of scope

- Writing batches to disk — T-016 (LogSegment).
- CRC validation during crash recovery scan — T-022 (Storage.Open).
- gRPC wire transport — M3 tickets.

## Definition of done

- [ ] `go build ./internal/storage/...` passes.
- [ ] `go test ./internal/storage/...` passes with all golden-vector tests.
- [ ] `DecodeBatch(EncodeBatch(records))` round-trip is identity for all record shapes.
- [ ] CRC-32C uses the Castagnoli polynomial (0x1EDC6F41); verified by the golden vector test.
- [ ] `batch_length`, `record_count`, `base_timestamp`, `max_timestamp` fields are computed correctly in the golden vector test.

## Tests required

- `TestEncodeBatch_GoldenVector` — fixed input records produce known hex byte string.
- `TestEncodeBatch_NilKey` — record with nil key encodes `key_length = -1` (varint: 0x01 after zigzag) correctly.
- `TestEncodeBatch_MultiRecord` — two-record batch: offsets 0 and 1, timestamps, correct `offset_delta` and `timestamp_delta`.
- `TestEncodeBatch_RecordHeaders` — record with one header; headers encoded and decoded correctly.
- `TestDecodeBatch_CRCMismatch` — flip one byte in records[] region → `ErrCRCMismatch`.
- `TestDecodeBatch_Truncated` — slice shorter than `batch_length` → error.
- `TestDecodeNextBatch_Sequential` — two concatenated encoded batches; two successive calls advance `pos` correctly.

## Dependencies

T-012 (go.mod and `internal/storage` package stub must exist).

## Notes

Use `github.com/klauspost/crc32` or the standard library's `hash/crc32` with `crc32.MakeTable(crc32.Castagnoli)`. CRC-32C covers only `records[]` (bytes [38, batch_length)), not the header. Varints follow the protobuf encoding convention (LEB128). Signed varints (`timestamp_delta`, `offset_delta`, `key_length`) use zigzag encoding: `(n << 1) ^ (n >> 63)`. The `base_offset` field in the encoded bytes is set to 0 at encode time because Storage.Append overwrites it with the real assigned offset before writing to disk — the CRC is computed over records[] only and therefore remains valid after the overwrite.
