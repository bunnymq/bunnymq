package client

import "github.com/bunnymq/bunnymq/internal/storage"

// BatchRecord is the input record type for BatchEncoder.Encode.
type BatchRecord struct {
	Key         []byte
	Value       []byte
	Headers     map[string][]byte
	TimestampMs int64
}

// BatchEncoder encodes BatchRecord slices into the on-wire batch format.
type BatchEncoder struct{}

// NewBatchEncoder returns a BatchEncoder.
func NewBatchEncoder() *BatchEncoder { return &BatchEncoder{} }

// Encode serialises records into the on-disk/on-wire batch format (base_offset=0).
func (e *BatchEncoder) Encode(records []BatchRecord) ([]byte, error) {
	srecs := make([]storage.Record, len(records))
	for i, r := range records {
		sr := storage.Record{
			TimestampMs: r.TimestampMs,
			Key:         r.Key,
			Value:       r.Value,
		}
		for k, v := range r.Headers {
			sr.Headers = append(sr.Headers, storage.RecordHeader{
				Key:   []byte(k),
				Value: v,
			})
		}
		srecs[i] = sr
	}
	return storage.EncodeBatch(srecs)
}
