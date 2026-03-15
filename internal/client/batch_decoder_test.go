package client

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/bunnymq/bunnymq/internal/storage"
)

func TestBatchDecoder_SingleRecord(t *testing.T) {
	rec := storage.Record{TimestampMs: 5000, Key: []byte("k"), Value: []byte("hello")}
	data, err := storage.EncodeBatch([]storage.Record{rec})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	// Override base_offset to 42 (CRC covers only records[], so header is safe to patch).
	binary.BigEndian.PutUint64(data[0:8], 42)

	dec := NewBatchDecoder()
	got, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if got[0].Offset != 42 {
		t.Errorf("Offset: got %d, want 42", got[0].Offset)
	}
	if got[0].TimestampMs != 5000 {
		t.Errorf("TimestampMs: got %d, want 5000", got[0].TimestampMs)
	}
	if string(got[0].Key) != "k" {
		t.Errorf("Key: got %q, want %q", got[0].Key, "k")
	}
	if string(got[0].Value) != "hello" {
		t.Errorf("Value: got %q, want %q", got[0].Value, "hello")
	}
}

func TestBatchDecoder_MultiBatch(t *testing.T) {
	batch1, err := storage.EncodeBatch([]storage.Record{
		{TimestampMs: 100, Value: []byte("a")},
		{TimestampMs: 200, Value: []byte("b")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch batch1: %v", err)
	}
	// Set base_offset for batch1 to 0.
	binary.BigEndian.PutUint64(batch1[0:8], 0)

	batch2, err := storage.EncodeBatch([]storage.Record{
		{TimestampMs: 300, Value: []byte("c")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch batch2: %v", err)
	}
	// Set base_offset for batch2 to 2 (follows batch1).
	binary.BigEndian.PutUint64(batch2[0:8], 2)

	data := append(batch1, batch2...)

	dec := NewBatchDecoder()
	got, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 records, got %d", len(got))
	}
	wantOffsets := []int64{0, 1, 2}
	for i, want := range wantOffsets {
		if got[i].Offset != want {
			t.Errorf("record %d: Offset got %d want %d", i, got[i].Offset, want)
		}
	}
	wantValues := []string{"a", "b", "c"}
	for i, want := range wantValues {
		if string(got[i].Value) != want {
			t.Errorf("record %d: Value got %q want %q", i, got[i].Value, want)
		}
	}
}

func TestBatchDecoder_CRCMismatch(t *testing.T) {
	batch1, err := storage.EncodeBatch([]storage.Record{
		{TimestampMs: 100, Value: []byte("good")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch batch1: %v", err)
	}

	batch2, err := storage.EncodeBatch([]storage.Record{
		{TimestampMs: 200, Value: []byte("bad")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch batch2: %v", err)
	}
	// Corrupt the CRC of batch2 (bytes 16–19 within batch2).
	batch2[16] ^= 0xFF

	data := append(batch1, batch2...)

	dec := NewBatchDecoder()
	got, decErr := dec.Decode(data)
	if decErr == nil {
		t.Fatal("expected error on CRC mismatch, got nil")
	}
	if !errors.Is(decErr, storage.ErrCRCMismatch) {
		t.Errorf("expected ErrCRCMismatch in error chain, got: %v", decErr)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 record from first batch, got %d", len(got))
	}
	if string(got[0].Value) != "good" {
		t.Errorf("expected first batch record value %q, got %q", "good", got[0].Value)
	}
}
