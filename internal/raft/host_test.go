package raft

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dbconfig "github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
)

// --- fake nodeHostIface for serialization tests ---

type fakeNodeHost struct {
	lastSyncData []byte
	lastShardID  uint64
}

func (f *fakeNodeHost) SyncPropose(_ context.Context, _ *client.Session, cmd []byte) (sm.Result, error) {
	f.lastSyncData = cmd
	return sm.Result{}, nil
}

func (f *fakeNodeHost) Propose(_ *client.Session, _ []byte, _ time.Duration) (*dragonboat.RequestState, error) {
	return nil, nil
}

func (f *fakeNodeHost) StaleRead(shardID uint64, _ any) (any, error) {
	f.lastShardID = shardID
	return nil, nil
}

func (f *fakeNodeHost) GetNoOPSession(_ uint64) *client.Session { return nil }

func (f *fakeNodeHost) StartReplica(_ map[uint64]string, _ bool, _ sm.CreateStateMachineFunc, _ dbconfig.Config) error {
	return nil
}

func (f *fakeNodeHost) StartOnDiskReplica(_ map[uint64]string, _ bool, _ sm.CreateOnDiskStateMachineFunc, _ dbconfig.Config) error {
	return nil
}

func (f *fakeNodeHost) StopShard(_ uint64) error                           { return nil }
func (f *fakeNodeHost) GetLeaderID(_ uint64) (uint64, uint64, bool, error) { return 0, 0, false, nil }
func (f *fakeNodeHost) Close()                                             {}

func fakeHost(nh *fakeNodeHost) *Host {
	return &Host{
		nh: nh,
		config: &Config{
			NodeID: 1,
		},
	}
}

// TestHost_MetadataPropose_Serialize verifies that SyncProposeMetadata sends
// a valid JSON-encoded MetadataCommand to the underlying nodeHost.
func TestHost_MetadataPropose_Serialize(t *testing.T) {
	fake := &fakeNodeHost{}
	h := fakeHost(fake)

	cmd := metadata.MetadataCommand{Type: metadata.CmdCreateTopic}
	_, err := h.SyncProposeMetadata(context.Background(), cmd)
	if err != nil {
		t.Fatalf("SyncProposeMetadata: %v", err)
	}

	if len(fake.lastSyncData) == 0 {
		t.Fatal("expected non-empty bytes sent to SyncPropose")
	}

	// Verify it is valid JSON containing the type field.
	want := `"type":"create_topic"`
	if !containsSubstring(fake.lastSyncData, want) {
		t.Fatalf("expected JSON to contain %q, got: %s", want, fake.lastSyncData)
	}
}

// TestHost_PartitionPropose_Serialize verifies that SyncProposePartition
// serialises a PartitionCommand as [type_byte, payload...].
func TestHost_PartitionPropose_Serialize(t *testing.T) {
	fake := &fakeNodeHost{}
	h := fakeHost(fake)

	cmd := partition.PartitionCommand{Type: partition.CmdAppendBatch, Payload: []byte("data")}
	_, err := h.SyncProposePartition(context.Background(), 1, cmd)
	if err != nil {
		t.Fatalf("SyncProposePartition: %v", err)
	}

	got := fake.lastSyncData
	if len(got) != 5 {
		t.Fatalf("expected 5 bytes, got %d", len(got))
	}
	if got[0] != 0x01 {
		t.Fatalf("expected first byte 0x01, got 0x%02x", got[0])
	}
	if string(got[1:]) != "data" {
		t.Fatalf("expected payload %q, got %q", "data", got[1:])
	}
}

// TestHost_StartStop starts a single-node in-process dragonboat cluster
// (metadata shard only) and verifies StartMetadataShard and Close succeed.
func TestHost_StartStop(t *testing.T) {
	addr, err := freeTCPAddr()
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}

	dir := t.TempDir()
	cfg := &Config{
		DataDir:     dir,
		RaftAddress: addr,
		NodeID:      1,
		RaftRTTMs:   10,
		Peers:       map[uint64]string{1: addr},
	}

	h, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}

	members := map[uint64]string{1: addr}
	err = h.StartMetadataShard(members, false, noopSMFactory)
	if err != nil {
		t.Fatalf("StartMetadataShard: %v", err)
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// freeTCPAddr returns a free TCP address on localhost.
func freeTCPAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr, nil
}

func containsSubstring(data []byte, sub string) bool {
	return len(data) >= len(sub) && findBytes(data, []byte(sub))
}

func findBytes(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// --- minimal no-op IStateMachine for TestHost_StartStop ---

type noopSM struct{}

func noopSMFactory(_ uint64, _ uint64) sm.IStateMachine { return &noopSM{} }

func (s *noopSM) Update(e sm.Entry) (sm.Result, error) { return sm.Result{}, nil }
func (s *noopSM) Lookup(_ any) (any, error)            { return nil, nil }
func (s *noopSM) SaveSnapshot(w io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	return nil
}
func (s *noopSM) RecoverFromSnapshot(_ io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
	return nil
}
func (s *noopSM) Close() error { return nil }
