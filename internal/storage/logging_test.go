package storage

import (
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bunnymq/bunnymq/internal/config"
)

func TestStorage_LogsSegmentRoll(t *testing.T) {
	dir := t.TempDir()
	core, logs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	// Use a tiny segment so the first Append triggers a roll.
	cfg := &config.StorageConfig{
		SegmentMaxBytes:  100,
		IndexSampleBytes: 4096,
	}
	s, err := Open(dir, cfg, WithLogger(logger))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close() //nolint:errcheck

	batch1 := makeBatch(t, "first", 1000)
	if _, err := s.Append(batch1); err != nil {
		t.Fatalf("Append batch1: %v", err)
	}
	batch2 := makeBatch(t, "second", 2000)
	if _, err := s.Append(batch2); err != nil {
		t.Fatalf("Append batch2: %v", err)
	}

	var rollEntries []observer.LoggedEntry
	for _, e := range logs.All() {
		if e.Level == zapcore.InfoLevel && e.Message == "segment rolled" {
			rollEntries = append(rollEntries, e)
		}
	}
	if len(rollEntries) == 0 {
		t.Fatal("expected at least one 'segment rolled' info log")
	}
	entry := rollEntries[0]
	for _, field := range []string{"old_base_offset", "new_base_offset", "bytes_written"} {
		if entry.ContextMap()[field] == nil {
			t.Errorf("expected field %q in segment rolled log entry", field)
		}
	}
}

func TestStorage_LogsCRCError(t *testing.T) {
	dir := t.TempDir()

	// Write a valid batch to the segment log, then corrupt it.
	cfg := &config.StorageConfig{
		SegmentMaxBytes:  128 * 1024 * 1024,
		IndexSampleBytes: 4096,
	}
	s, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open initial: %v", err)
	}
	batch := makeBatch(t, "hello", 1000)
	if _, err := s.Append(batch); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the CRC field (bytes 16-20) in the log file.
	logPath := filepath.Join(dir, "00000000000000000000.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) > 17 {
		data[17] ^= 0xFF
	}
	if err := os.WriteFile(logPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Reopen with an observer logger and expect a CRC warn.
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	s2, err := Open(dir, cfg, WithLogger(logger))
	if err != nil {
		t.Fatalf("Open after corruption: %v", err)
	}
	defer s2.Close() //nolint:errcheck

	var crcEntries []observer.LoggedEntry
	for _, e := range logs.All() {
		if e.Level == zapcore.WarnLevel && e.Message == "CRC mismatch during crash recovery scan" {
			crcEntries = append(crcEntries, e)
		}
	}
	if len(crcEntries) == 0 {
		t.Fatal("expected 'CRC mismatch during crash recovery scan' warn log")
	}
	if crcEntries[0].ContextMap()["byte_position"] == nil {
		t.Error("expected 'byte_position' field in CRC warn log entry")
	}
}
