package client

import (
	"fmt"

	"github.com/bunnymq/bunnymq/internal/storage"
)

// DecodedRecord is a record with offset and timestamp reconstructed from batch header deltas.
type DecodedRecord struct {
	Offset      int64
	Key         []byte
	Value       []byte
	Headers     map[string][]byte
	TimestampMs int64
}

// BatchDecoder decodes the on-wire batch format into DecodedRecord slices.
type BatchDecoder struct{}

// NewBatchDecoder returns a BatchDecoder.
func NewBatchDecoder() *BatchDecoder { return &BatchDecoder{} }

// Decode parses one or more consecutive batches from data, verifies CRC-32C per
// batch, and returns all decoded records. On CRC mismatch, records from earlier
// valid batches are returned together with an error describing the failed batch.
func (d *BatchDecoder) Decode(data []byte) ([]DecodedRecord, error) {
	var out []DecodedRecord
	pos := 0
	for pos < len(data) {
		batch, nextPos, err := storage.DecodeNextBatch(data, pos)
		if err != nil {
			if len(out) > 0 {
				return out, fmt.Errorf("decode batch at byte %d: %w", pos, err)
			}
			return nil, fmt.Errorf("decode batch at byte %d: %w", pos, err)
		}
		for i, rec := range batch.Records {
			dr := DecodedRecord{
				Offset:      batch.BaseOffset + int64(i),
				Key:         rec.Key,
				Value:       rec.Value,
				TimestampMs: rec.TimestampMs,
			}
			if len(rec.Headers) > 0 {
				dr.Headers = make(map[string][]byte, len(rec.Headers))
				for _, h := range rec.Headers {
					dr.Headers[string(h.Key)] = h.Value
				}
			}
			out = append(out, dr)
		}
		pos = nextPos
	}
	return out, nil
}
