package partition

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/config"
	"github.com/bunnymq/bunnymq/internal/storage"
)

func testStorageCfg() *config.StorageConfig {
	return &config.StorageConfig{
		SegmentMaxBytes:  128 * 1024 * 1024,
		IndexSampleBytes: 4096,
	}
}

func makeBatch(t *testing.T, value string, timestampMs int64) []byte {
	t.Helper()
	b, err := storage.EncodeBatch([]storage.Record{{TimestampMs: timestampMs, Value: []byte(value)}})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	return b
}

func openFSM(t *testing.T) *PartitionFSM {
	t.Helper()
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, "applied.idx")
	fsm := NewPartitionFSM(dir, sidecarPath, testStorageCfg())
	t.Cleanup(func() { _ = fsm.Close() })
	return fsm
}

// TestPartitionFSM_InterfaceConformance verifies the compile-time assertion.
func TestPartitionFSM_InterfaceConformance(t *testing.T) {
	var _ sm.IOnDiskStateMachine = (*PartitionFSM)(nil)
}

// TestPartitionFSM_OpenFresh opens on an empty directory; expects (0, nil) and LatestOffset=0.
func TestPartitionFSM_OpenFresh(t *testing.T) {
	fsm := openFSM(t)
	idx, err := fsm.Open(nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if idx != 0 {
		t.Fatalf("Open returned index %d, want 0", idx)
	}
	if got := fsm.storage.LatestOffset(); got != 0 {
		t.Fatalf("LatestOffset = %d, want 0", got)
	}
}

// TestPartitionFSM_UpdateAppend proposes an AppendBatch entry and verifies
// the batch is written to storage and the sidecar is persisted.
func TestPartitionFSM_UpdateAppend(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	batch := makeBatch(t, "hello", 1000)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	entries := []sm.Entry{{Index: 1, Cmd: cmd}}

	out, err := fsm.Update(entries)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("Update returned %d entries, want 1", len(out))
	}
	// Result.Value is the base offset returned by storage.Append.
	_ = out[0].Result.Value

	if got := fsm.storage.LatestOffset(); got == 0 {
		t.Fatal("LatestOffset still 0 after Append")
	}

	// Sidecar must exist after Update.
	if _, err := os.Stat(fsm.sidecarPath); err != nil {
		t.Fatalf("sidecar missing after Update: %v", err)
	}
}

// TestPartitionFSM_UpdateRetentionConfig proposes a RetentionConfig entry; no panic; sidecar written.
func TestPartitionFSM_UpdateRetentionConfig(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// First append a real batch so LatestOffset > 0 (needed for non-trivial sidecar).
	batch := makeBatch(t, "seed", 0)
	appendCmd := append([]byte{CmdAppendBatch}, batch...)
	_, err := fsm.Update([]sm.Entry{{Index: 1, Cmd: appendCmd}})
	if err != nil {
		t.Fatalf("Update(append): %v", err)
	}

	payload, _ := json.Marshal(RetentionConfigPayload{RetentionMs: 3600_000, RetentionBytes: 1 << 30})
	cmd := append([]byte{CmdRetentionConfig}, payload...)
	entries := []sm.Entry{{Index: 2, Cmd: cmd}}

	if _, err := fsm.Update(entries); err != nil {
		t.Fatalf("Update(RetentionConfig): %v", err)
	}

	if _, err := os.Stat(fsm.sidecarPath); err != nil {
		t.Fatalf("sidecar missing after RetentionConfig Update: %v", err)
	}
}

// TestPartitionFSM_PersistApplied_Sidecar verifies that after Update the sidecar
// contains the correct index and offset.
func TestPartitionFSM_PersistApplied_Sidecar(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	batch := makeBatch(t, "data", 500)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	const raftIdx = uint64(7)
	if _, err := fsm.Update([]sm.Entry{{Index: raftIdx, Cmd: cmd}}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	raw, err := os.ReadFile(fsm.sidecarPath)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if len(raw) != 16 {
		t.Fatalf("sidecar size %d, want 16", len(raw))
	}
	gotIdx := binary.BigEndian.Uint64(raw[0:8])
	gotOff := int64(binary.BigEndian.Uint64(raw[8:16]))

	if gotIdx != raftIdx {
		t.Fatalf("sidecar index %d, want %d", gotIdx, raftIdx)
	}
	if gotOff != fsm.storage.LatestOffset() {
		t.Fatalf("sidecar offset %d, want %d", gotOff, fsm.storage.LatestOffset())
	}
}

// TestPartitionFSM_OpenReconcile simulates a crash where storage is ahead of the sidecar.
// After reopening, TruncateTo should be called and LatestOffset should match the sidecar value.
func TestPartitionFSM_OpenReconcile(t *testing.T) {
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, "applied.idx")
	cfg := testStorageCfg()

	// Step 1: open and append one batch via Update; this writes the sidecar.
	fsm1 := NewPartitionFSM(dir, sidecarPath, cfg)
	if _, err := fsm1.Open(nil); err != nil {
		t.Fatalf("first Open: %v", err)
	}
	batch := makeBatch(t, "committed", 100)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	if _, err := fsm1.Update([]sm.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	committedOffset := fsm1.storage.LatestOffset()

	// Step 2: append one more batch directly to storage (simulates data written after sidecar).
	extraBatch := makeBatch(t, "uncommitted", 200)
	if _, err := fsm1.storage.Append(extraBatch); err != nil {
		t.Fatalf("direct Append: %v", err)
	}
	// Do NOT update the sidecar; storage is now ahead.
	_ = fsm1.storage.Close()

	// Step 3: reopen; Open() must truncate storage back to committedOffset.
	fsm2 := NewPartitionFSM(dir, sidecarPath, cfg)
	t.Cleanup(func() { _ = fsm2.Close() })
	idx, err := fsm2.Open(nil)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	if idx != 1 {
		t.Fatalf("second Open returned index %d, want 1", idx)
	}
	if got := fsm2.storage.LatestOffset(); got != committedOffset {
		t.Fatalf("LatestOffset after reconcile = %d, want %d", got, committedOffset)
	}
}

// TestPartitionFSM_Lookup_Read appends a batch via Update; Lookup QueryRead at offset 0
// returns the batch bytes.
func TestPartitionFSM_Lookup_Read(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	batch := makeBatch(t, "hello", 1000)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	if _, err := fsm.Update([]sm.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	res, err := fsm.Lookup(PartitionQuery{Type: QueryRead, Offset: 0, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	lr, ok := res.(PartitionLookupResult)
	if !ok {
		t.Fatalf("expected PartitionLookupResult, got %T", res)
	}
	if len(lr.Batches) == 0 {
		t.Fatal("Lookup returned empty Batches")
	}
}

// TestPartitionFSM_Lookup_EarliestLatest verifies QueryEarliestOffset and QueryLatestOffset
// after 3 appends.
func TestPartitionFSM_Lookup_EarliestLatest(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 1; i <= 3; i++ {
		batch := makeBatch(t, "msg", int64(i*100))
		cmd := append([]byte{CmdAppendBatch}, batch...)
		if _, err := fsm.Update([]sm.Entry{{Index: uint64(i), Cmd: cmd}}); err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
	}

	earliest, err := fsm.Lookup(PartitionQuery{Type: QueryEarliestOffset})
	if err != nil {
		t.Fatalf("Lookup EarliestOffset: %v", err)
	}
	if earliest.(int64) != 0 {
		t.Fatalf("EarliestOffset = %d, want 0", earliest)
	}

	latest, err := fsm.Lookup(PartitionQuery{Type: QueryLatestOffset})
	if err != nil {
		t.Fatalf("Lookup LatestOffset: %v", err)
	}
	if latest.(int64) != 3 {
		t.Fatalf("LatestOffset = %d, want 3", latest)
	}
}

// TestPartitionFSM_Lookup_ReadNoData verifies that QueryRead at offset == LatestOffset
// returns (nil, offset, nil).
func TestPartitionFSM_Lookup_ReadNoData(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	batch := makeBatch(t, "msg", 100)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	if _, err := fsm.Update([]sm.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	latestOffset := fsm.storage.LatestOffset()
	res, err := fsm.Lookup(PartitionQuery{Type: QueryRead, Offset: latestOffset, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	lr, ok := res.(PartitionLookupResult)
	if !ok {
		t.Fatalf("expected PartitionLookupResult, got %T", res)
	}
	if lr.Batches != nil {
		t.Fatalf("expected nil Batches at latest offset, got %d bytes", len(lr.Batches))
	}
	if lr.NextOffset != latestOffset {
		t.Fatalf("NextOffset = %d, want %d", lr.NextOffset, latestOffset)
	}
}

// TestPartitionFSM_SnapshotNoOp calls PrepareSnapshot, SaveSnapshot, RecoverFromSnapshot;
// verifies no error and state is unchanged.
func TestPartitionFSM_SnapshotNoOp(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	batch := makeBatch(t, "data", 42)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	if _, err := fsm.Update([]sm.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	offsetBefore := fsm.storage.LatestOffset()

	ctx, err := fsm.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot: %v", err)
	}

	var buf bytes.Buffer
	if err := fsm.SaveSnapshot(ctx, &buf, nil); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if buf.String() != "strategy-a-noop" {
		t.Fatalf("SaveSnapshot wrote %q, want %q", buf.String(), "strategy-a-noop")
	}

	if err := fsm.RecoverFromSnapshot(&buf, nil); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}

	if fsm.storage.LatestOffset() != offsetBefore {
		t.Fatalf("LatestOffset changed after snapshot round-trip")
	}
}

// TestPartitionFSM_Sync_NoOp verifies that Sync returns nil.
func TestPartitionFSM_Sync_NoOp(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := fsm.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// TestPartitionFSM_Close verifies that after Close, subsequent Lookup returns an error.
func TestPartitionFSM_Close(t *testing.T) {
	fsm := openFSM(t)
	if _, err := fsm.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write data so nextOffset > 0, forcing Read to access segment files.
	batch := makeBatch(t, "msg", 100)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	if _, err := fsm.Update([]sm.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := fsm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := fsm.Lookup(PartitionQuery{Type: QueryRead, Offset: 0, MaxBytes: 1 << 20})
	if err == nil {
		t.Fatal("Lookup after Close should return error, got nil")
	}
}

// TestPartitionFSM_OpenClean closes gracefully and reopens; the index from the sidecar
// is returned and no truncation occurs.
func TestPartitionFSM_OpenClean(t *testing.T) {
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, "applied.idx")
	cfg := testStorageCfg()

	fsm1 := NewPartitionFSM(dir, sidecarPath, cfg)
	if _, err := fsm1.Open(nil); err != nil {
		t.Fatalf("first Open: %v", err)
	}
	batch := makeBatch(t, "msg", 42)
	cmd := append([]byte{CmdAppendBatch}, batch...)
	const wantIdx = uint64(3)
	if _, err := fsm1.Update([]sm.Entry{{Index: wantIdx, Cmd: cmd}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	wantOffset := fsm1.storage.LatestOffset()
	// Graceful close — sidecar matches storage.
	if err := fsm1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fsm2 := NewPartitionFSM(dir, sidecarPath, cfg)
	t.Cleanup(func() { _ = fsm2.Close() })
	idx, err := fsm2.Open(nil)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	if idx != wantIdx {
		t.Fatalf("second Open returned index %d, want %d", idx, wantIdx)
	}
	if got := fsm2.storage.LatestOffset(); got != wantOffset {
		t.Fatalf("LatestOffset = %d, want %d", got, wantOffset)
	}
}
