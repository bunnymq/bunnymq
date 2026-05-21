package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/config"
	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
	"github.com/bunnymq/bunnymq/internal/raft"
	"github.com/bunnymq/bunnymq/internal/storage"
)

// storageCfg is a minimal storage configuration used across all integration tests.
var storageCfg = &config.StorageConfig{
	SegmentMaxBytes:  128 * 1024 * 1024,
	IndexSampleBytes: 4096,
}

// freeTCPAddr finds an available TCP address on localhost.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPAddr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// makePartitionFactory returns a dragonboat factory that creates PartitionFSMs
// rooted at baseDir/shard-{shardID}/.
func makePartitionFactory(baseDir string) sm.CreateOnDiskStateMachineFunc {
	return func(shardID uint64, _ uint64) sm.IOnDiskStateMachine {
		dir := filepath.Join(baseDir, fmt.Sprintf("shard-%d", shardID))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			panic(fmt.Sprintf("MkdirAll shard dir: %v", err))
		}
		sidecarPath := filepath.Join(dir, "applied.idx")
		return partition.NewPartitionFSM(dir, sidecarPath, storageCfg, nil, "", "")
	}
}

// startSingleNodeCluster creates a single-node dragonboat cluster (metadata shard
// only) at dataDir, bound to addr, and waits until the metadata shard is ready.
func startSingleNodeCluster(t *testing.T, dataDir string, addr string) *raft.Host {
	t.Helper()
	cfg := &raft.Config{
		DataDir:     dataDir,
		RaftAddress: addr,
		NodeID:      1,
		RaftRTTMs:   10,
		Peers:       map[uint64]string{1: addr},
	}
	h, err := raft.NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}

	metaFactory := func(_ uint64, _ uint64) sm.IStateMachine {
		return metadata.NewMetadataFSM()
	}
	members := map[uint64]string{1: addr}
	if err := h.StartMetadataShard(members, false, metaFactory); err != nil {
		_ = h.Close()
		t.Fatalf("StartMetadataShard: %v", err)
	}
	return h
}

// waitForMetadataReady polls SyncProposeMetadata with a benign RegisterNode(0) probe
// until the shard accepts a propose (i.e., has elected itself leader).
func waitForMetadataReady(t *testing.T, h *raft.Host, timeout time.Duration) {
	t.Helper()
	probe := metadata.MetadataCommand{
		Type:         metadata.CmdRegisterNode,
		RegisterNode: &metadata.RegisterNodeCmd{NodeID: 0, Address: ""},
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := h.SyncProposeMetadata(ctx, probe)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for metadata shard leader election")
}

// waitForPartitionReady polls SyncProposePartition with a RetentionConfig probe
// (sets retention to 0/0, a no-op) until the partition shard accepts a propose.
// RetentionConfig does not advance storage offsets, so it doesn't affect test data.
func waitForPartitionReady(t *testing.T, h *raft.Host, shardID uint64, timeout time.Duration) {
	t.Helper()
	payload, err := json.Marshal(partition.RetentionConfigPayload{RetentionMs: 0, RetentionBytes: 0})
	if err != nil {
		t.Fatalf("marshal probe payload: %v", err)
	}
	probe := partition.PartitionCommand{Type: partition.CmdRetentionConfig, Payload: payload}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := h.SyncProposePartition(ctx, shardID, probe)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for partition shard %d leader election", shardID)
}

// makeBatch encodes a single-record batch for use in propose commands.
func makeBatch(t *testing.T, value string) []byte {
	t.Helper()
	b, err := storage.EncodeBatch([]storage.Record{{TimestampMs: 1000, Value: []byte(value)}})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	return b
}

// proposeAndCheck proposes cmd and fatals if the result is not OK.
func proposeAndCheck(t *testing.T, h *raft.Host, cmd metadata.MetadataCommand) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := h.SyncProposeMetadata(ctx, cmd)
	if err != nil {
		t.Fatalf("SyncProposeMetadata(%s): %v", cmd.Type, err)
	}
	if result.Value != metadata.ResultOK {
		t.Fatalf("SyncProposeMetadata(%s): code=%d msg=%s", cmd.Type, result.Value, result.Data)
	}
}

// appendBatch proposes an AppendBatch command to the given partition shard.
func appendBatch(t *testing.T, h *raft.Host, shardID uint64, batchData []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := h.SyncProposePartition(ctx, shardID, partition.PartitionCommand{
		Type:    partition.CmdAppendBatch,
		Payload: batchData,
	})
	if err != nil {
		t.Fatalf("SyncProposePartition(AppendBatch): %v", err)
	}
}

// latestOffset queries and returns the latest offset for the given partition shard.
func latestOffset(t *testing.T, h *raft.Host, shardID uint64) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := h.LookupPartition(ctx, shardID, partition.PartitionQuery{Type: partition.QueryLatestOffset})
	if err != nil {
		t.Fatalf("LookupPartition(LatestOffset): %v", err)
	}
	off, ok := raw.(int64)
	if !ok {
		t.Fatalf("LatestOffset result type %T, want int64", raw)
	}
	return off
}

// getShardID queries the metadata FSM and returns the shard ID for partition 0 of topicName.
func getShardID(t *testing.T, h *raft.Host, topicName string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := h.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:        metadata.QueryGetPartition,
		TopicName:   topicName,
		PartitionID: 0,
	})
	if err != nil {
		t.Fatalf("LookupMetadata(GetPartition): %v", err)
	}
	pm, ok := raw.(*metadata.PartitionMeta)
	if !ok {
		t.Fatalf("GetPartition result type %T, want *metadata.PartitionMeta", raw)
	}
	return pm.ShardID
}

// TestSingleNode_CreateTopicAndAppend starts a single-node cluster, creates a topic,
// appends 3 batches through the Raft path, and verifies all 3 are readable.
func TestSingleNode_CreateTopicAndAppend(t *testing.T) {
	dataDir := t.TempDir()
	addr := freeTCPAddr(t)
	partDir := filepath.Join(dataDir, "partitions")

	h := startSingleNodeCluster(t, dataDir, addr)
	defer func() { _ = h.Close() }()

	waitForMetadataReady(t, h, 5*time.Second)

	proposeAndCheck(t, h, metadata.MetadataCommand{
		Type: metadata.CmdRegisterNode,
		RegisterNode: &metadata.RegisterNodeCmd{
			NodeID:  1,
			Address: addr,
		},
	})
	proposeAndCheck(t, h, metadata.MetadataCommand{
		Type: metadata.CmdCreateTopic,
		CreateTopic: &metadata.CreateTopicCmd{
			Name:              "test-topic",
			PartitionCount:    1,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}},
		},
	})

	shardID := getShardID(t, h, "test-topic")
	members := map[uint64]string{1: addr}
	if err := h.StartPartitionShard(shardID, members, false, makePartitionFactory(partDir)); err != nil {
		t.Fatalf("StartPartitionShard: %v", err)
	}
	waitForPartitionReady(t, h, shardID, 5*time.Second)

	for i := 0; i < 3; i++ {
		appendBatch(t, h, shardID, makeBatch(t, fmt.Sprintf("msg-%d", i)))
	}

	if off := latestOffset(t, h, shardID); off != 3 {
		t.Fatalf("LatestOffset = %d, want 3", off)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := h.LookupPartition(ctx, shardID, partition.PartitionQuery{
		Type:     partition.QueryRead,
		Offset:   0,
		MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("LookupPartition(Read): %v", err)
	}
	result, ok := raw.(partition.PartitionLookupResult)
	if !ok {
		t.Fatalf("Read result type %T, want partition.PartitionLookupResult", raw)
	}
	if result.NextOffset != 3 {
		t.Fatalf("Read NextOffset = %d, want 3 (all 3 batches)", result.NextOffset)
	}
}

// TestSingleNode_RestartRecovery verifies that metadata and partition state survive
// a graceful close and restart of the single-node cluster.
func TestSingleNode_RestartRecovery(t *testing.T) {
	dataDir := t.TempDir()
	addr := freeTCPAddr(t)
	partDir := filepath.Join(dataDir, "partitions")

	// --- first lifetime ---
	h := startSingleNodeCluster(t, dataDir, addr)
	waitForMetadataReady(t, h, 5*time.Second)

	proposeAndCheck(t, h, metadata.MetadataCommand{
		Type: metadata.CmdRegisterNode,
		RegisterNode: &metadata.RegisterNodeCmd{NodeID: 1, Address: addr},
	})
	proposeAndCheck(t, h, metadata.MetadataCommand{
		Type: metadata.CmdCreateTopic,
		CreateTopic: &metadata.CreateTopicCmd{
			Name:              "restart-topic",
			PartitionCount:    1,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}},
		},
	})

	shardID := getShardID(t, h, "restart-topic")
	members := map[uint64]string{1: addr}
	if err := h.StartPartitionShard(shardID, members, false, makePartitionFactory(partDir)); err != nil {
		t.Fatalf("StartPartitionShard: %v", err)
	}
	waitForPartitionReady(t, h, shardID, 5*time.Second)

	for i := 0; i < 5; i++ {
		appendBatch(t, h, shardID, makeBatch(t, fmt.Sprintf("msg-%d", i)))
	}
	if off := latestOffset(t, h, shardID); off != 5 {
		t.Fatalf("pre-restart LatestOffset = %d, want 5", off)
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// --- restart ---
	h2 := startSingleNodeCluster(t, dataDir, addr)
	defer func() { _ = h2.Close() }()

	waitForMetadataReady(t, h2, 5*time.Second)

	// Metadata must survive Raft log replay.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := h2.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:      metadata.QueryGetTopic,
		TopicName: "restart-topic",
	})
	if err != nil {
		t.Fatalf("post-restart LookupMetadata(GetTopic): %v", err)
	}
	if _, ok := raw.(*metadata.TopicMeta); !ok {
		t.Fatalf("GetTopic result type %T, want *metadata.TopicMeta", raw)
	}

	shardID2 := getShardID(t, h2, "restart-topic")
	if shardID2 != shardID {
		t.Fatalf("shardID changed after restart: got %d, want %d", shardID2, shardID)
	}

	if err := h2.StartPartitionShard(shardID2, members, false, makePartitionFactory(partDir)); err != nil {
		t.Fatalf("post-restart StartPartitionShard: %v", err)
	}
	waitForPartitionReady(t, h2, shardID2, 5*time.Second)

	// Partition data must survive.
	if off := latestOffset(t, h2, shardID2); off != 5 {
		t.Fatalf("post-restart LatestOffset = %d, want 5", off)
	}

	// Append 2 more and read all 7.
	for i := 5; i < 7; i++ {
		appendBatch(t, h2, shardID2, makeBatch(t, fmt.Sprintf("msg-%d", i)))
	}
	if off := latestOffset(t, h2, shardID2); off != 7 {
		t.Fatalf("LatestOffset after 2 more appends = %d, want 7", off)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	raw2, err := h2.LookupPartition(ctx2, shardID2, partition.PartitionQuery{
		Type:     partition.QueryRead,
		Offset:   0,
		MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("LookupPartition(Read all 7): %v", err)
	}
	result, ok := raw2.(partition.PartitionLookupResult)
	if !ok {
		t.Fatalf("Read result type %T, want partition.PartitionLookupResult", raw2)
	}
	if result.NextOffset != 7 {
		t.Fatalf("Read NextOffset = %d, want 7", result.NextOffset)
	}
}

// TestSingleNode_PartitionFSM_CrashRecovery simulates an unclean shutdown by
// truncating the last batch in the log file, then verifies that at least 2 of
// 3 appended batches are recoverable after restart.
func TestSingleNode_PartitionFSM_CrashRecovery(t *testing.T) {
	dataDir := t.TempDir()
	addr := freeTCPAddr(t)
	partDir := filepath.Join(dataDir, "partitions")

	h := startSingleNodeCluster(t, dataDir, addr)
	waitForMetadataReady(t, h, 5*time.Second)

	proposeAndCheck(t, h, metadata.MetadataCommand{
		Type: metadata.CmdRegisterNode,
		RegisterNode: &metadata.RegisterNodeCmd{NodeID: 1, Address: addr},
	})
	proposeAndCheck(t, h, metadata.MetadataCommand{
		Type: metadata.CmdCreateTopic,
		CreateTopic: &metadata.CreateTopicCmd{
			Name:              "crash-topic",
			PartitionCount:    1,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}},
		},
	})

	shardID := getShardID(t, h, "crash-topic")
	members := map[uint64]string{1: addr}
	if err := h.StartPartitionShard(shardID, members, false, makePartitionFactory(partDir)); err != nil {
		t.Fatalf("StartPartitionShard: %v", err)
	}
	waitForPartitionReady(t, h, shardID, 5*time.Second)

	for i := 0; i < 3; i++ {
		appendBatch(t, h, shardID, makeBatch(t, fmt.Sprintf("crash-msg-%d", i)))
	}
	if off := latestOffset(t, h, shardID); off != 3 {
		t.Fatalf("pre-crash LatestOffset = %d, want 3", off)
	}

	// Graceful close — sidecar is written (LastAppliedIndex=3, LatestOffset=3).
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate crash: truncate the active .log file by 1 byte to corrupt the last batch.
	shardDir := filepath.Join(partDir, fmt.Sprintf("shard-%d", shardID))
	logPath := filepath.Join(shardDir, "00000000000000000000.log")
	fi, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if fi.Size() < 2 {
		t.Fatalf("log file too small to truncate: %d bytes", fi.Size())
	}
	if err := os.Truncate(logPath, fi.Size()-1); err != nil {
		t.Fatalf("truncate log: %v", err)
	}

	// Restart cluster.
	h2 := startSingleNodeCluster(t, dataDir, addr)
	defer func() { _ = h2.Close() }()
	waitForMetadataReady(t, h2, 5*time.Second)

	if err := h2.StartPartitionShard(shardID, members, false, makePartitionFactory(partDir)); err != nil {
		t.Fatalf("post-crash StartPartitionShard: %v", err)
	}
	waitForPartitionReady(t, h2, shardID, 5*time.Second)

	off := latestOffset(t, h2, shardID)
	if off < 2 {
		t.Fatalf("post-crash LatestOffset = %d, want >= 2", off)
	}
}
