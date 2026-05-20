package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/bunnymq/bunnymq/pkg/client"
)

func TestFormatRecord_UTF8(t *testing.T) {
	r := client.Record{
		Topic:       "test-topic",
		PartitionID: 2,
		Offset:      42,
		Key:         []byte("my-key"),
		Value:       []byte("hello world"),
		TimestampMs: 1000,
	}
	data, err := formatRecord(r)
	if err != nil {
		t.Fatalf("formatRecord error: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if m["topic"] != "test-topic" {
		t.Errorf("topic = %v, want test-topic", m["topic"])
	}
	if m["partition"].(float64) != 2 {
		t.Errorf("partition = %v, want 2", m["partition"])
	}
	if m["offset"].(float64) != 42 {
		t.Errorf("offset = %v, want 42", m["offset"])
	}
	// encoding/json encodes []byte as base64; for plain ASCII the base64 decodes to the original string
	keyB64, ok := m["key"].(string)
	if !ok {
		t.Fatalf("key is not a string")
	}
	keyDecoded, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil {
		t.Fatalf("key base64 decode error: %v", err)
	}
	if string(keyDecoded) != "my-key" {
		t.Errorf("key = %q, want my-key", string(keyDecoded))
	}
	valB64, ok := m["value"].(string)
	if !ok {
		t.Fatalf("value is not a string")
	}
	valDecoded, err := base64.StdEncoding.DecodeString(valB64)
	if err != nil {
		t.Fatalf("value base64 decode error: %v", err)
	}
	if string(valDecoded) != "hello world" {
		t.Errorf("value = %q, want hello world", string(valDecoded))
	}
}

func TestFormatRecord_BinaryValue(t *testing.T) {
	binary := []byte{0xFF, 0xFE, 0x00, 0x01}
	r := client.Record{
		Topic:       "bin-topic",
		PartitionID: 0,
		Offset:      0,
		Value:       binary,
	}
	data, err := formatRecord(r)
	if err != nil {
		t.Fatalf("formatRecord error: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	valStr, ok := m["value"].(string)
	if !ok {
		t.Fatalf("value is not a string in JSON")
	}
	decoded, err := base64.StdEncoding.DecodeString(valStr)
	if err != nil {
		t.Fatalf("value is not valid base64: %v", err)
	}
	if string(decoded) != string(binary) {
		t.Errorf("decoded value = %v, want %v", decoded, binary)
	}
}

func TestParseOffsetReset(t *testing.T) {
	tests := []struct {
		input string
		want  client.OffsetResetPolicy
	}{
		{"earliest", client.OffsetResetEarliest},
		{"latest", client.OffsetResetLatest},
		{"", client.OffsetResetLatest},
		{"EARLIEST", client.OffsetResetLatest},
	}
	for _, tt := range tests {
		got := parseOffsetReset(tt.input)
		if got != tt.want {
			t.Errorf("parseOffsetReset(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

