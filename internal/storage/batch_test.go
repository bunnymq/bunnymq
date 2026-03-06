package storage

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

// golden: single record, key=nil, value="hello", timestamp=1000ms, no headers
const goldenHex = "000000000000000000000032000000017bba064e000000000000000003e800000000000003e80b000000010568656c6c6f00"

func goldenRecords() []Record {
	return []Record{
		{TimestampMs: 1000, Key: nil, Value: []byte("hello"), Headers: nil},
	}
}

func TestEncodeBatch_GoldenVector(t *testing.T) {
	encoded, err := EncodeBatch(goldenRecords())
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	want, _ := hex.DecodeString(goldenHex)
	if !bytes.Equal(encoded, want) {
		t.Errorf("encoded = %x\nwant    = %x", encoded, want)
	}

	// Verify round-trip: CRC check and record identity.
	batch, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("record count = %d, want 1", len(batch.Records))
	}
	r := batch.Records[0]
	if r.TimestampMs != 1000 {
		t.Errorf("TimestampMs = %d, want 1000", r.TimestampMs)
	}
	if r.Key != nil {
		t.Errorf("Key = %v, want nil", r.Key)
	}
	if !bytes.Equal(r.Value, []byte("hello")) {
		t.Errorf("Value = %q, want \"hello\"", r.Value)
	}

	// Verify CRC uses Castagnoli: header bytes [16:20] must match golden.
	if got := hex.EncodeToString(encoded[16:20]); got != "7bba064e" {
		t.Errorf("CRC32 field = %s, want 7bba064e", got)
	}

	// Verify structural fields.
	if batch.BatchLength != 50 {
		t.Errorf("BatchLength = %d, want 50", batch.BatchLength)
	}
	if batch.RecordCount != 1 {
		t.Errorf("RecordCount = %d, want 1", batch.RecordCount)
	}
	if batch.BaseTimestamp != 1000 {
		t.Errorf("BaseTimestamp = %d, want 1000", batch.BaseTimestamp)
	}
	if batch.MaxTimestamp != 1000 {
		t.Errorf("MaxTimestamp = %d, want 1000", batch.MaxTimestamp)
	}
}

func TestEncodeBatch_NilKey(t *testing.T) {
	// Nil key must encode key_length = -1 via zigzag → varint byte 0x01.
	rec := Record{TimestampMs: 500, Key: nil, Value: []byte("v")}
	encoded, err := EncodeBatch([]Record{rec})
	if err != nil {
		t.Fatal(err)
	}

	// The records region starts at byte 38.
	recsRegion := encoded[38:]
	// record: length varint, then body
	// body[0] = attributes(0), body[1] = ts_delta varint(0), body[2] = offset_delta varint(0),
	// body[3] = key_length varint → must be 0x01 (zigzag(-1)=1).
	bodyStart := 1 // skip the length varint (value < 128 so it's one byte)
	keyLengthPos := bodyStart + 1 + 1 + 1 // skip attributes, ts_delta, offset_delta
	if recsRegion[keyLengthPos] != 0x01 {
		t.Errorf("key_length byte = 0x%02x, want 0x01 (zigzag(-1))", recsRegion[keyLengthPos])
	}

	// Verify round-trip decodes nil key.
	batch, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Records[0].Key != nil {
		t.Errorf("Key = %v, want nil", batch.Records[0].Key)
	}
}

func TestEncodeBatch_MultiRecord(t *testing.T) {
	records := []Record{
		{TimestampMs: 2000, Key: []byte("k1"), Value: []byte("v1")},
		{TimestampMs: 2050, Key: []byte("k2"), Value: []byte("v2")},
	}
	encoded, err := EncodeBatch(records)
	if err != nil {
		t.Fatal(err)
	}

	batch, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if batch.RecordCount != 2 {
		t.Fatalf("RecordCount = %d, want 2", batch.RecordCount)
	}
	if batch.BaseTimestamp != 2000 {
		t.Errorf("BaseTimestamp = %d, want 2000", batch.BaseTimestamp)
	}
	if batch.MaxTimestamp != 2050 {
		t.Errorf("MaxTimestamp = %d, want 2050", batch.MaxTimestamp)
	}

	if !bytes.Equal(batch.Records[0].Key, []byte("k1")) {
		t.Errorf("record[0].Key = %q, want \"k1\"", batch.Records[0].Key)
	}
	if batch.Records[0].TimestampMs != 2000 {
		t.Errorf("record[0].TimestampMs = %d, want 2000", batch.Records[0].TimestampMs)
	}

	if !bytes.Equal(batch.Records[1].Key, []byte("k2")) {
		t.Errorf("record[1].Key = %q, want \"k2\"", batch.Records[1].Key)
	}
	if batch.Records[1].TimestampMs != 2050 {
		t.Errorf("record[1].TimestampMs = %d, want 2050", batch.Records[1].TimestampMs)
	}

	// Verify offset_delta and timestamp_delta are encoded correctly by checking
	// the decoded offsets (offset_delta for record[1] must be 1, ts_delta 50).
	// The decoded record's TimestampMs = base_timestamp + ts_delta, and we already
	// verified TimestampMs above. Confirm via round-trip re-encode equality.
	reEncoded, err := EncodeBatch(batch.Records)
	if err != nil {
		t.Fatal(err)
	}
	// base_offset is 0 in both; timestamps must match, so re-encoding equals original.
	if !bytes.Equal(encoded, reEncoded) {
		t.Errorf("re-encode differs from original")
	}
}

func TestEncodeBatch_RecordHeaders(t *testing.T) {
	records := []Record{
		{
			TimestampMs: 3000,
			Key:         []byte("key"),
			Value:       []byte("val"),
			Headers: []RecordHeader{
				{Key: []byte("h-key"), Value: []byte("h-val")},
			},
		},
	}
	encoded, err := EncodeBatch(records)
	if err != nil {
		t.Fatal(err)
	}

	batch, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}

	r := batch.Records[0]
	if len(r.Headers) != 1 {
		t.Fatalf("headers count = %d, want 1", len(r.Headers))
	}
	if !bytes.Equal(r.Headers[0].Key, []byte("h-key")) {
		t.Errorf("header.Key = %q, want \"h-key\"", r.Headers[0].Key)
	}
	if !bytes.Equal(r.Headers[0].Value, []byte("h-val")) {
		t.Errorf("header.Value = %q, want \"h-val\"", r.Headers[0].Value)
	}
}

func TestDecodeBatch_CRCMismatch(t *testing.T) {
	encoded, err := EncodeBatch(goldenRecords())
	if err != nil {
		t.Fatal(err)
	}

	// Flip a byte in the records[] region (byte 38 is the first records byte).
	corrupt := make([]byte, len(encoded))
	copy(corrupt, encoded)
	corrupt[38] ^= 0xFF

	_, err = DecodeBatch(corrupt)
	if !errors.Is(err, ErrCRCMismatch) {
		t.Errorf("error = %v, want ErrCRCMismatch", err)
	}
}

func TestDecodeBatch_Truncated(t *testing.T) {
	encoded, err := EncodeBatch(goldenRecords())
	if err != nil {
		t.Fatal(err)
	}

	// Trim the last byte so len(data) < batch_length.
	truncated := encoded[:len(encoded)-1]

	_, err = DecodeBatch(truncated)
	if err == nil {
		t.Error("expected error for truncated input, got nil")
	}
}

func TestDecodeNextBatch_Sequential(t *testing.T) {
	b1 := []Record{{TimestampMs: 100, Value: []byte("first")}}
	b2 := []Record{{TimestampMs: 200, Value: []byte("second")}}

	enc1, err := EncodeBatch(b1)
	if err != nil {
		t.Fatal(err)
	}
	enc2, err := EncodeBatch(b2)
	if err != nil {
		t.Fatal(err)
	}

	data := append(enc1, enc2...)

	batch1, pos1, err := DecodeNextBatch(data, 0)
	if err != nil {
		t.Fatalf("first DecodeNextBatch: %v", err)
	}
	if pos1 != len(enc1) {
		t.Errorf("pos after first batch = %d, want %d", pos1, len(enc1))
	}
	if !bytes.Equal(batch1.Records[0].Value, []byte("first")) {
		t.Errorf("first batch value = %q, want \"first\"", batch1.Records[0].Value)
	}

	batch2, pos2, err := DecodeNextBatch(data, pos1)
	if err != nil {
		t.Fatalf("second DecodeNextBatch: %v", err)
	}
	if pos2 != len(data) {
		t.Errorf("pos after second batch = %d, want %d", pos2, len(data))
	}
	if !bytes.Equal(batch2.Records[0].Value, []byte("second")) {
		t.Errorf("second batch value = %q, want \"second\"", batch2.Records[0].Value)
	}

	// Third call must return error (no more data).
	_, _, err = DecodeNextBatch(data, pos2)
	if err == nil {
		t.Error("expected error past end of data, got nil")
	}
}
