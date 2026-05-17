package main

import (
	"testing"

	"github.com/bunnymq/bunnymq/pkg/client"
)

func TestParseAcks(t *testing.T) {
	tests := []struct {
		input   string
		want    client.AcksMode
		wantErr bool
	}{
		{"all", client.AcksAll, false},
		{"zero", client.AcksZero, false},
		{"", 0, true},
		{"1", 0, true},
		{"ALL", 0, true},
	}
	for _, tt := range tests {
		got, err := parseAcks(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseAcks(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("parseAcks(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
