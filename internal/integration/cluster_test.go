//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bunnymq/bunnymq/internal/storage"
	"github.com/bunnymq/bunnymq/pkg/client"
)

// brokerBinary is set by TestMain to the compiled cmd/bunnymq binary.
var brokerBinary string

func TestMain(m *testing.M) {
	bin, err := buildBrokerBinary()
	if err != nil {
		fmt.Printf("WARN: failed to build broker binary (cluster tests will be skipped): %v\n", err)
	}
	brokerBinary = bin
	os.Exit(m.Run())
}

// buildBrokerBinary compiles cmd/bunnymq into a temp file and returns its path.
func buildBrokerBinary() (string, error) {
	dir, err := os.MkdirTemp("", "bunnymq-bin-*")
	if err != nil {
		return "", err
	}
	binPath := filepath.Join(dir, "bunnymq")
	cmd := exec.Command("go", "build", "-o", binPath, "github.com/bunnymq/bunnymq/cmd/bunnymq")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("go build: %w", err)
	}
	return binPath, nil
}

// producedBatch holds the offset and value of a produced batch.
type producedBatch struct {
	offset int64
	value  string
}

// brokerCfgFile is the JSON config written for each broker process.
type brokerCfgFile struct {
	NodeID         uint64            `json:"nodeid"`
	RaftAddress    string            `json:"raftaddress"`
	ManagementAddr string            `json:"managementaddr"`
	DataAddr       string            `json:"dataaddr"`
	DataDir        string            `json:"datadir"`
	RaftRTTMs      uint64            `json:"raftrttms"`
	Peers          map[string]string `json:"peers"`
	Storage        brokerStorage     `json:"storage"`
	Coordinator    brokerCoord       `json:"coordinator"`
}

type brokerStorage struct {
	SegmentMaxBytes  int64 `json:"segmentmaxbytes"`
	IndexSampleBytes int   `json:"indexsamplebytes"`
}

type brokerCoord struct {
	ReconcileIntervalMs    int64 `json:"reconcileintervalms"`
	LeaderCheckIntervalMs  int64 `json:"leadercheckintervalms"`
	BootstrapTimeoutMs     int64 `json:"bootstraptimeoutms"`
	EagerReconcileOnCreate bool  `json:"eagerreconcileoncreate"`
	GroupSweepIntervalMs   int64 `json:"groupsweepintervalms"`
}

// startBroker writes a config file and launches cmd/bunnymq as a subprocess.
// The returned *exec.Cmd is running; t.Cleanup terminates it.
func startBroker(t *testing.T, nodeID uint64, raftPort, mgmtPort, dataPort int, dataDir string, peers map[uint64]string) *exec.Cmd {
	t.Helper()

	peerMap := make(map[string]string, len(peers))
	for id, addr := range peers {
		peerMap[fmt.Sprintf("%d", id)] = addr
	}

	cfg := brokerCfgFile{
		NodeID:         nodeID,
		RaftAddress:    fmt.Sprintf("localhost:%d", raftPort),
		ManagementAddr: fmt.Sprintf("localhost:%d", mgmtPort),
		DataAddr:       fmt.Sprintf("localhost:%d", dataPort),
		DataDir:        dataDir,
		RaftRTTMs:      10,
		Peers:          peerMap,
		Storage: brokerStorage{
			SegmentMaxBytes:  128 * 1024 * 1024,
			IndexSampleBytes: 4096,
		},
		Coordinator: brokerCoord{
			ReconcileIntervalMs:    500,
			LeaderCheckIntervalMs:  1000,
			BootstrapTimeoutMs:     30000,
			EagerReconcileOnCreate: true,
		},
	}

	cfgData, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal broker config: %v", err)
	}
	cfgPath := filepath.Join(dataDir, "config.json")
	if err := os.WriteFile(cfgPath, cfgData, 0o644); err != nil {
		t.Fatalf("write broker config: %v", err)
	}

	cmd := exec.Command(brokerBinary, cfgPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker node %d: %v", nodeID, err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	return cmd
}

// startBrokerSweep starts a broker with a custom GroupSweepIntervalMs.
func startBrokerSweep(t *testing.T, nodeID uint64, raftPort, mgmtPort, dataPort int, dataDir string, peers map[uint64]string, groupSweepIntervalMs int64) *exec.Cmd {
	t.Helper()

	peerMap := make(map[string]string, len(peers))
	for id, addr := range peers {
		peerMap[fmt.Sprintf("%d", id)] = addr
	}

	cfg := brokerCfgFile{
		NodeID:         nodeID,
		RaftAddress:    fmt.Sprintf("localhost:%d", raftPort),
		ManagementAddr: fmt.Sprintf("localhost:%d", mgmtPort),
		DataAddr:       fmt.Sprintf("localhost:%d", dataPort),
		DataDir:        dataDir,
		RaftRTTMs:      10,
		Peers:          peerMap,
		Storage: brokerStorage{
			SegmentMaxBytes:  128 * 1024 * 1024,
			IndexSampleBytes: 4096,
		},
		Coordinator: brokerCoord{
			ReconcileIntervalMs:    500,
			LeaderCheckIntervalMs:  1000,
			BootstrapTimeoutMs:     30000,
			EagerReconcileOnCreate: true,
			GroupSweepIntervalMs:   groupSweepIntervalMs,
		},
	}

	cfgData, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal broker config: %v", err)
	}
	cfgPath := filepath.Join(dataDir, "config.json")
	if err := os.WriteFile(cfgPath, cfgData, 0o644); err != nil {
		t.Fatalf("write broker config: %v", err)
	}

	cmd := exec.Command(brokerBinary, cfgPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker node %d: %v", nodeID, err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	return cmd
}

// waitClusterReady polls AdminClient.DescribeCluster until expectedNodes nodes
// are registered or timeout is reached.
func waitClusterReady(t *testing.T, adminAddr string, expectedNodes int, timeout time.Duration) {
	t.Helper()
	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		desc, err := ac.DescribeCluster(ctx)
		cancel()
		if err == nil && len(desc.Nodes) >= expectedNodes {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("cluster did not reach %d nodes within %s", expectedNodes, timeout)
}

// waitLeaderChanged polls DescribeTopic for partition 0 until its LeaderNodeID is
// non-zero AND different from killedNodeID. This ensures the ClusterCoordinator has
// run its leader sweep and the metadata FSM reflects the NEW leader.
func waitLeaderChanged(t *testing.T, adminAddr string, topic string, killedNodeID uint64, timeout time.Duration) {
	t.Helper()
	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client for waitLeaderChanged: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		desc, err := ac.DescribeTopic(ctx, topic)
		cancel()
		if err == nil {
			for _, p := range desc.Partitions {
				if p.PartitionID == 0 && p.LeaderNodeID != 0 && p.LeaderNodeID != killedNodeID {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("partition 0 leader did not change away from node %d within %s", killedNodeID, timeout)
}

// waitPartitionsLeaders polls DescribeTopic until all partitions have a non-zero
// LeaderNodeID or timeout is reached.
func waitPartitionsLeaders(t *testing.T, adminAddr string, topic string, partitionCount int, timeout time.Duration) {
	t.Helper()
	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client for waitPartitionsLeaders: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		desc, err := ac.DescribeTopic(ctx, topic)
		cancel()
		if err == nil && allLeadersElected(desc.Partitions, partitionCount) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("not all %d partitions of %q had leaders within %s", partitionCount, topic, timeout)
}

func allLeadersElected(parts []client.PartitionInfo, want int) bool {
	elected := 0
	for _, p := range parts {
		if p.LeaderNodeID != 0 {
			elected++
		}
	}
	return elected >= want
}

// consumeAtLeast polls the consumer until each of nPartitions partitions has at
// least wantPerPart records, or timeout elapses.
func consumeAtLeast(t *testing.T, cons *client.Consumer, nPartitions, wantPerPart int, timeout time.Duration) [][]client.Record {
	t.Helper()
	received := make([][]client.Record, nPartitions)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if allPartitionsHave(received, wantPerPart) {
			break
		}
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 5*time.Second)
		recs, pollErr := cons.Poll(pollCtx, 5000)
		pollCancel()
		if pollErr != nil {
			t.Fatalf("Poll: %v", pollErr)
		}
		for _, r := range recs {
			p := int(r.PartitionID)
			if p >= 0 && p < nPartitions {
				received[p] = append(received[p], r)
			}
		}
	}
	return received
}

func allPartitionsHave(received [][]client.Record, want int) bool {
	for _, recs := range received {
		if len(recs) < want {
			return false
		}
	}
	return true
}

// TestCluster_ProduceFetch starts a 3-node cluster, creates a topic with RF=3,
// produces 10 batches to each of 3 partitions, and verifies all are readable.
func TestCluster_ProduceFetch(t *testing.T) {
	if brokerBinary == "" {
		t.Skip("broker binary not available; skipping cluster test")
	}

	const (
		partitionCount = 3
		batchesPerPart = 10
	)

	type nodeSpec struct {
		id       uint64
		raftPort int
		mgmtPort int
		dataPort int
	}
	nodes := []nodeSpec{
		{1, 19093, 19091, 19092},
		{2, 29093, 29091, 29092},
		{3, 39093, 39091, 39092},
	}

	peers := map[uint64]string{}
	for _, n := range nodes {
		peers[n.id] = fmt.Sprintf("localhost:%d", n.raftPort)
	}

	for _, n := range nodes {
		dataDir := t.TempDir()
		startBroker(t, n.id, n.raftPort, n.mgmtPort, n.dataPort, dataDir, peers)
	}

	adminAddr := fmt.Sprintf("localhost:%d", nodes[0].mgmtPort)
	waitClusterReady(t, adminAddr, 3, 30*time.Second)

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()
	if _, err = ac.CreateTopic(ctx, client.CreateTopicRequest{
		Name:              "smoke-topic",
		PartitionCount:    partitionCount,
		ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	waitPartitionsLeaders(t, adminAddr, "smoke-topic", partitionCount, 15*time.Second)

	bootstrapAddrs := make([]string, len(nodes))
	for i, n := range nodes {
		bootstrapAddrs[i] = fmt.Sprintf("localhost:%d", n.mgmtPort)
	}

	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	produced := produceAllBatches(t, ctx, prod, partitionCount, batchesPerPart)
	checkMonotonicOffsets(t, produced)

	cons, err := client.NewConsumer(client.ConsumerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		MaxFetchBytes:  1 << 20,
		MaxFetchWaitMs: 5000,
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Close() //nolint:errcheck

	for p := 0; p < partitionCount; p++ {
		cons.Seek("smoke-topic", int32(p), 0)
	}

	received := consumeAtLeast(t, cons, partitionCount, batchesPerPart, 30*time.Second)
	checkConsumedContent(t, produced, received, partitionCount, batchesPerPart)
}

// produceAllBatches sends batchesPerPart batches to each of partitionCount partitions.
func produceAllBatches(t *testing.T, ctx context.Context, prod *client.Producer, partitionCount, batchesPerPart int) [][]producedBatch {
	t.Helper()
	produced := make([][]producedBatch, partitionCount)
	for p := 0; p < partitionCount; p++ {
		produced[p] = make([]producedBatch, 0, batchesPerPart)
		for b := 0; b < batchesPerPart; b++ {
			val := fmt.Sprintf("part-%d-batch-%d", p, b)
			batchData, encErr := storage.EncodeBatch([]storage.Record{
				{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
			})
			if encErr != nil {
				t.Fatalf("encode batch p=%d b=%d: %v", p, b, encErr)
			}
			sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
			offset, sendErr := prod.SendBatch(sendCtx, "smoke-topic", int32(p), batchData, client.AcksAll)
			sendCancel()
			if sendErr != nil {
				t.Fatalf("SendBatch p=%d b=%d: %v", p, b, sendErr)
			}
			produced[p] = append(produced[p], producedBatch{offset: offset, value: val})
		}
	}
	return produced
}

// checkMonotonicOffsets verifies each partition's base offsets are strictly increasing.
func checkMonotonicOffsets(t *testing.T, produced [][]producedBatch) {
	t.Helper()
	for p, batches := range produced {
		for i := 1; i < len(batches); i++ {
			if batches[i].offset <= batches[i-1].offset {
				t.Errorf("partition %d offsets not monotonically increasing: [%d]=%d <= [%d]=%d",
					p, i, batches[i].offset, i-1, batches[i-1].offset)
			}
		}
	}
}

// checkConsumedContent verifies consumed record values match what was produced.
func checkConsumedContent(t *testing.T, produced [][]producedBatch, received [][]client.Record, partitionCount, batchesPerPart int) {
	t.Helper()
	for p := 0; p < partitionCount; p++ {
		if len(received[p]) < batchesPerPart {
			t.Errorf("partition %d: got %d records, want >= %d", p, len(received[p]), batchesPerPart)
			continue
		}
		for i, rec := range received[p][:batchesPerPart] {
			want := produced[p][i].value
			if got := string(rec.Value); got != want {
				t.Errorf("partition %d record %d: got %q, want %q", p, i, got, want)
			}
		}
	}
}

// clusterNode describes one broker in a multi-node test cluster.
type clusterNode struct {
	id       uint64
	raftPort int
	mgmtPort int
	dataPort int
}

// clusterBootstrapAddrs returns the management addresses of all nodes.
func clusterBootstrapAddrs(nodes []clusterNode) []string {
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = fmt.Sprintf("localhost:%d", n.mgmtPort)
	}
	return addrs
}

// clusterPeers returns the raft peer map for all nodes.
func clusterPeers(nodes []clusterNode) map[uint64]string {
	peers := make(map[uint64]string, len(nodes))
	for _, n := range nodes {
		peers[n.id] = fmt.Sprintf("localhost:%d", n.raftPort)
	}
	return peers
}

// failoverProducer creates a Producer with aggressive retries suited for a leader-failover window.
func failoverProducer(t *testing.T, bootstrapAddrs []string) *client.Producer {
	t.Helper()
	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
			RetryPolicy: client.RetryPolicy{
				MaxRetries:     10,
				InitialBackoff: 200 * time.Millisecond,
				MaxBackoff:     3 * time.Second,
				BackoffFactor:  2.0,
			},
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new failover producer: %v", err)
	}
	return prod
}

// sendOneBatch encodes a single-record batch and sends it, returning the produced offset and value.
func sendOneBatch(t *testing.T, ctx context.Context, prod *client.Producer, topic string, partition int32, batchIdx int, prefix string, timeout time.Duration) producedBatch {
	t.Helper()
	val := fmt.Sprintf("%s-%d", prefix, batchIdx)
	batchData, err := storage.EncodeBatch([]storage.Record{
		{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
	})
	if err != nil {
		t.Fatalf("encode batch %s-%d: %v", prefix, batchIdx, err)
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	offset, err := prod.SendBatch(sendCtx, topic, partition, batchData, client.AcksAll)
	cancel()
	if err != nil {
		t.Fatalf("SendBatch %s-%d: %v", prefix, batchIdx, err)
	}
	return producedBatch{offset: offset, value: val}
}

// checkSequentialOffsets verifies that produced batches have no gaps or duplicates.
func checkSequentialOffsets(t *testing.T, produced []producedBatch) {
	t.Helper()
	for i := 1; i < len(produced); i++ {
		if produced[i].offset != produced[i-1].offset+1 {
			t.Errorf("offset gap at batch %d: %d → %d", i, produced[i-1].offset, produced[i].offset)
		}
	}
}

// findLeaderIdx returns the index into nodes of the current leader for partition 0,
// plus the mgmt address of one surviving (non-leader) node.
func findLeaderIdx(t *testing.T, ac *client.AdminClient, topic string, nodes []clusterNode) (leaderIdx int, survivorMgmtAddr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	desc, err := ac.DescribeTopic(ctx, topic)
	cancel()
	if err != nil {
		t.Fatalf("DescribeTopic: %v", err)
	}
	var leaderNodeID uint64
	for _, p := range desc.Partitions {
		if p.PartitionID == 0 {
			leaderNodeID = p.LeaderNodeID
		}
	}
	if leaderNodeID == 0 {
		t.Fatalf("partition 0 has no leader in DescribeTopic response")
	}
	leaderIdx = -1
	for i, n := range nodes {
		if n.id == leaderNodeID {
			leaderIdx = i
		} else if survivorMgmtAddr == "" {
			survivorMgmtAddr = fmt.Sprintf("localhost:%d", n.mgmtPort)
		}
	}
	if leaderIdx < 0 {
		t.Fatalf("leader nodeID %d not found in node list", leaderNodeID)
	}
	return leaderIdx, survivorMgmtAddr
}

// TestCluster_LeaderFailover starts a 3-node cluster, produces 5 batches, kills the
// partition leader, verifies produce and fetch resume on the new leader with sequential
// offsets, and confirms the killed broker can rejoin.
func TestCluster_LeaderFailover(t *testing.T) {
	if brokerBinary == "" {
		t.Skip("broker binary not available; skipping cluster test")
	}

	nodes := []clusterNode{
		{1, 49093, 49091, 49092},
		{2, 50093, 50091, 50092},
		{3, 51093, 51091, 51092},
	}
	peers := clusterPeers(nodes)

	cmds := make([]*exec.Cmd, len(nodes))
	dataDirs := make([]string, len(nodes))
	for i, n := range nodes {
		dataDirs[i] = t.TempDir()
		cmds[i] = startBroker(t, n.id, n.raftPort, n.mgmtPort, n.dataPort, dataDirs[i], peers)
	}

	adminAddr := fmt.Sprintf("localhost:%d", nodes[0].mgmtPort)
	waitClusterReady(t, adminAddr, 3, 30*time.Second)

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()
	if _, err = ac.CreateTopic(ctx, client.CreateTopicRequest{
		Name:              "failover-topic",
		PartitionCount:    1,
		ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	waitPartitionsLeaders(t, adminAddr, "failover-topic", 1, 15*time.Second)

	leaderIdx, survivorMgmtAddr := findLeaderIdx(t, ac, "failover-topic", nodes)
	bootstrapAddrs := clusterBootstrapAddrs(nodes)

	prod := failoverProducer(t, bootstrapAddrs)
	defer prod.Close() //nolint:errcheck

	produced := make([]producedBatch, 0, 10)
	for b := 0; b < 5; b++ {
		produced = append(produced, sendOneBatch(t, ctx, prod, "failover-topic", 0, b, "failover-batch", 15*time.Second))
	}

	// SIGKILL the leader — simulates a crash (no graceful dragonboat shutdown).
	t.Logf("killing leader node %d (index %d)", nodes[leaderIdx].id, leaderIdx)
	if err = cmds[leaderIdx].Process.Kill(); err != nil {
		t.Fatalf("kill leader: %v", err)
	}
	_ = cmds[leaderIdx].Wait()

	// Produce batch 5 (expected offset 5); producer retries through the election window.
	b5 := sendOneBatch(t, ctx, prod, "failover-topic", 0, 5, "failover-batch", 30*time.Second)
	if b5.offset != 5 {
		t.Errorf("post-failover batch 5: expected offset 5, got %d", b5.offset)
	}
	produced = append(produced, b5)

	for b := 6; b < 10; b++ {
		produced = append(produced, sendOneBatch(t, ctx, prod, "failover-topic", 0, b, "failover-batch", 15*time.Second))
	}

	checkSequentialOffsets(t, produced)

	cons, err := client.NewConsumer(client.ConsumerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		MaxFetchBytes:  1 << 20,
		MaxFetchWaitMs: 5000,
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Close() //nolint:errcheck

	cons.Seek("failover-topic", 0, 0)
	received := consumeAtLeast(t, cons, 1, 10, 30*time.Second)
	for i, rec := range received[0][:10] {
		if string(rec.Value) != produced[i].value {
			t.Errorf("record %d: got %q, want %q", i, rec.Value, produced[i].value)
		}
	}

	// Restart the killed broker with the same dataDir so dragonboat replays the log.
	startBroker(t, nodes[leaderIdx].id, nodes[leaderIdx].raftPort,
		nodes[leaderIdx].mgmtPort, nodes[leaderIdx].dataPort,
		dataDirs[leaderIdx], peers)
	waitClusterReady(t, survivorMgmtAddr, 3, 30*time.Second)
}

// TestCluster_LeaderFailover_FetchDuringElection verifies that a long-poll consumer
// targeting the new leader receives the next batch in a single Poll call after a
// leader change — confirming server-side newDataCh notification works on the new leader.
func TestCluster_LeaderFailover_FetchDuringElection(t *testing.T) {
	if brokerBinary == "" {
		t.Skip("broker binary not available; skipping cluster test")
	}

	nodes := []clusterNode{
		{1, 52093, 52091, 52092},
		{2, 53093, 53091, 53092},
		{3, 54093, 54091, 54092},
	}
	peers := clusterPeers(nodes)

	cmds := make([]*exec.Cmd, len(nodes))
	for i, n := range nodes {
		cmds[i] = startBroker(t, n.id, n.raftPort, n.mgmtPort, n.dataPort, t.TempDir(), peers)
	}

	adminAddr := fmt.Sprintf("localhost:%d", nodes[0].mgmtPort)
	waitClusterReady(t, adminAddr, 3, 30*time.Second)

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()
	if _, err = ac.CreateTopic(ctx, client.CreateTopicRequest{
		Name:              "failover-fetch-topic",
		PartitionCount:    1,
		ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	waitPartitionsLeaders(t, adminAddr, "failover-fetch-topic", 1, 15*time.Second)

	leaderIdx, survivorMgmtAddr := findLeaderIdx(t, ac, "failover-fetch-topic", nodes)
	bootstrapAddrs := clusterBootstrapAddrs(nodes)

	prod := failoverProducer(t, bootstrapAddrs)
	defer prod.Close() //nolint:errcheck

	for b := 0; b < 5; b++ {
		sendOneBatch(t, ctx, prod, "failover-fetch-topic", 0, b, "fetch-election-batch", 15*time.Second)
	}

	// SIGKILL the leader.
	killedNodeID := nodes[leaderIdx].id
	t.Logf("killing leader node %d (index %d) for FetchDuringElection", killedNodeID, leaderIdx)
	if err = cmds[leaderIdx].Process.Kill(); err != nil {
		t.Fatalf("kill leader: %v", err)
	}
	_ = cmds[leaderIdx].Wait()

	// Wait until the metadata FSM reflects the NEW leader (not the dead node).
	waitLeaderChanged(t, survivorMgmtAddr, "failover-fetch-topic", killedNodeID, 15*time.Second)

	cons, err := client.NewConsumer(client.ConsumerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		MaxFetchBytes:  1 << 20,
		MaxFetchWaitMs: 5000,
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Close() //nolint:errcheck
	cons.Seek("failover-fetch-topic", 0, 5)

	type pollResult struct {
		recs []client.Record
		err  error
	}
	pollCh := make(chan pollResult, 1)
	go func() {
		pollCtx, pollCancel := context.WithTimeout(ctx, 15*time.Second)
		defer pollCancel()
		recs, fetchErr := cons.Poll(pollCtx, 5000)
		pollCh <- pollResult{recs, fetchErr}
	}()

	// Allow time for the Fetch RPC to reach the new leader and enter its long-poll wait.
	time.Sleep(time.Second)

	trigger := sendOneBatch(t, ctx, prod, "failover-fetch-topic", 0, 5, "fetch-election-batch", 15*time.Second)
	if trigger.offset != 5 {
		t.Errorf("trigger batch offset: got %d, want 5", trigger.offset)
	}

	select {
	case result := <-pollCh:
		if result.err != nil {
			t.Fatalf("long-poll Poll: %v", result.err)
		}
		if len(result.recs) == 0 {
			t.Error("long-poll returned no records; expected batch at offset 5")
		} else if string(result.recs[0].Value) != trigger.value {
			t.Errorf("long-poll record value: got %q, want %q", result.recs[0].Value, trigger.value)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("long-poll did not return within 15s after producing batch 5")
	}
}

// groupCollect polls a slice of consumers in a round-robin loop, accumulating records
// keyed by partition ID per consumer, until nPartitions distinct partition IDs have been
// seen across all consumers or timeout elapses.
func groupCollect(t *testing.T, consumers []*client.Consumer, nPartitions int, timeout time.Duration) []map[int32][]client.Record {
	t.Helper()
	results := make([]map[int32][]client.Record, len(consumers))
	for i := range results {
		results[i] = make(map[int32][]client.Record)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		seen := make(map[int32]bool)
		for _, m := range results {
			for pid := range m {
				seen[pid] = true
			}
		}
		if len(seen) >= nPartitions {
			break
		}
		for i, c := range consumers {
			pollCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			recs, err := c.Poll(pollCtx, 1000)
			cancel()
			if err != nil {
				t.Logf("groupCollect consumer %d poll: %v", i, err)
				continue
			}
			for _, r := range recs {
				results[i][r.PartitionID] = append(results[i][r.PartitionID], r)
			}
		}
	}
	return results
}

// produceRoundRobin sends nBatches batches distributed round-robin across nPartitions.
func produceRoundRobin(t *testing.T, ctx context.Context, prod *client.Producer, topic string, nPartitions, nBatches int, prefix string) {
	t.Helper()
	for b := 0; b < nBatches; b++ {
		part := int32(b % nPartitions)
		val := fmt.Sprintf("%s-p%d-b%d", prefix, part, b/nPartitions)
		batchData, encErr := storage.EncodeBatch([]storage.Record{
			{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
		})
		if encErr != nil {
			t.Fatalf("encode batch: %v", encErr)
		}
		sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, sendErr := prod.SendBatch(sendCtx, topic, part, batchData, client.AcksAll)
		cancel()
		if sendErr != nil {
			t.Fatalf("SendBatch %s p=%d: %v", prefix, part, sendErr)
		}
	}
}

// TestGroup_TwoConsumers_RangeAssignment starts a 3-broker cluster, creates a topic
// with 4 partitions, produces 40 batches, and verifies that two consumers in the same
// group each receive exactly 2 partitions with no overlap and full coverage.
func TestGroup_TwoConsumers_RangeAssignment(t *testing.T) {
	if brokerBinary == "" {
		t.Skip("broker binary not available; skipping cluster test")
	}

	nodes := []clusterNode{
		{1, 55093, 55091, 55092},
		{2, 56093, 56091, 56092},
		{3, 57093, 57091, 57092},
	}
	peers := clusterPeers(nodes)
	for _, n := range nodes {
		startBroker(t, n.id, n.raftPort, n.mgmtPort, n.dataPort, t.TempDir(), peers)
	}

	adminAddr := fmt.Sprintf("localhost:%d", nodes[0].mgmtPort)
	waitClusterReady(t, adminAddr, 3, 30*time.Second)

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()
	if _, err = ac.CreateTopic(ctx, client.CreateTopicRequest{
		Name:              "group-topic",
		PartitionCount:    4,
		ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	waitPartitionsLeaders(t, adminAddr, "group-topic", 4, 15*time.Second)

	bootstrapAddrs := clusterBootstrapAddrs(nodes)
	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	const (
		nPartitions    = 4
		batchesPerPart = 10
	)
	produceRoundRobin(t, ctx, prod, "group-topic", nPartitions, nPartitions*batchesPerPart, "grp")

	newGroupConsumer := func() *client.Consumer {
		c, cerr := client.NewConsumer(client.ConsumerConfig{
			Config: client.Config{
				BootstrapServers: bootstrapAddrs,
				RequestTimeout:   10 * time.Second,
			},
			GroupID:             "test-group",
			MaxFetchBytes:       1 << 20,
			MaxFetchWaitMs:      1000,
			AutoOffsetReset:     client.OffsetResetEarliest,
			HeartbeatIntervalMs: 1000,
		})
		if cerr != nil {
			t.Fatalf("new consumer: %v", cerr)
		}
		return c
	}

	cons1 := newGroupConsumer()
	defer cons1.Close() //nolint:errcheck
	if err = cons1.Subscribe([]string{"group-topic"}); err != nil {
		t.Fatalf("cons1.Subscribe: %v", err)
	}

	cons2 := newGroupConsumer()
	defer cons2.Close() //nolint:errcheck
	if err = cons2.Subscribe([]string{"group-topic"}); err != nil {
		t.Fatalf("cons2.Subscribe: %v", err)
	}

	// Wait for cons1's heartbeat to detect rebalance_required and re-join.
	// With HeartbeatIntervalMs=1000ms, 5 seconds covers multiple heartbeat cycles.
	time.Sleep(5 * time.Second)

	// Collect records from both consumers in their stable assignments.
	results := groupCollect(t, []*client.Consumer{cons1, cons2}, nPartitions, 30*time.Second)
	r1, r2 := results[0], results[1]

	for pid := range r1 {
		if _, ok := r2[pid]; ok {
			t.Errorf("partition %d appears in both consumers (overlap)", pid)
		}
	}
	for p := int32(0); p < nPartitions; p++ {
		_, in1 := r1[p]
		_, in2 := r2[p]
		if !in1 && !in2 {
			t.Errorf("partition %d not covered by either consumer", p)
		}
	}
	if len(r1) != 2 {
		t.Errorf("consumer1 covers %d partitions, want 2", len(r1))
	}
	if len(r2) != 2 {
		t.Errorf("consumer2 covers %d partitions, want 2", len(r2))
	}
	total := 0
	for _, recs := range r1 {
		total += len(recs)
	}
	for _, recs := range r2 {
		total += len(recs)
	}
	if total < nPartitions*batchesPerPart {
		t.Errorf("total records: got %d, want >= %d", total, nPartitions*batchesPerPart)
	}
}

// TestGroup_VoluntaryLeave_TriggersRebalance verifies that Consumer3.Close() sends
// LeaveGroup, which causes the group coordinator to rebalance so that Consumer1 and
// Consumer2 together cover all 3 partitions. New batches produced after the rebalance
// are fully received by the remaining two consumers.
func TestGroup_VoluntaryLeave_TriggersRebalance(t *testing.T) {
	if brokerBinary == "" {
		t.Skip("broker binary not available; skipping cluster test")
	}

	nodes := []clusterNode{
		{1, 58093, 58091, 58092},
		{2, 59093, 59091, 59092},
		{3, 60093, 60091, 60092},
	}
	peers := clusterPeers(nodes)
	for _, n := range nodes {
		startBroker(t, n.id, n.raftPort, n.mgmtPort, n.dataPort, t.TempDir(), peers)
	}

	adminAddr := fmt.Sprintf("localhost:%d", nodes[0].mgmtPort)
	waitClusterReady(t, adminAddr, 3, 30*time.Second)

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()
	if _, err = ac.CreateTopic(ctx, client.CreateTopicRequest{
		Name:              "leave-topic",
		PartitionCount:    3,
		ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	waitPartitionsLeaders(t, adminAddr, "leave-topic", 3, 15*time.Second)

	bootstrapAddrs := clusterBootstrapAddrs(nodes)
	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	const (
		nPartitions    = 3
		initialBatches = 10 // per partition; total = 30
	)
	produceRoundRobin(t, ctx, prod, "leave-topic", nPartitions, nPartitions*initialBatches, "leave-pre")

	newLeaveConsumer := func() *client.Consumer {
		c, cerr := client.NewConsumer(client.ConsumerConfig{
			Config: client.Config{
				BootstrapServers: bootstrapAddrs,
				RequestTimeout:   10 * time.Second,
			},
			GroupID:             "leave-group",
			MaxFetchBytes:       1 << 20,
			MaxFetchWaitMs:      1000,
			AutoOffsetReset:     client.OffsetResetEarliest,
			HeartbeatIntervalMs: 1000,
		})
		if cerr != nil {
			t.Fatalf("new consumer: %v", cerr)
		}
		return c
	}

	cons1 := newLeaveConsumer()
	defer cons1.Close() //nolint:errcheck
	if err = cons1.Subscribe([]string{"leave-topic"}); err != nil {
		t.Fatalf("cons1.Subscribe: %v", err)
	}

	cons2 := newLeaveConsumer()
	defer cons2.Close() //nolint:errcheck
	if err = cons2.Subscribe([]string{"leave-topic"}); err != nil {
		t.Fatalf("cons2.Subscribe: %v", err)
	}

	cons3 := newLeaveConsumer()
	if err = cons3.Subscribe([]string{"leave-topic"}); err != nil {
		// cons3 may fail to close on cleanup, so we still try.
		_ = cons3.Close()
		t.Fatalf("cons3.Subscribe: %v", err)
	}

	// Wait for all three rebalances to settle (one per join).
	time.Sleep(5 * time.Second)

	// Verify initial stable assignment: all 3 partitions covered across 3 consumers.
	initResults := groupCollect(t, []*client.Consumer{cons1, cons2, cons3}, nPartitions, 20*time.Second)
	for p := int32(0); p < nPartitions; p++ {
		covered := false
		for _, m := range initResults {
			if _, ok := m[p]; ok {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("initial assignment: partition %d not covered", p)
		}
	}

	// cons3 leaves voluntarily; its Close() sends LeaveGroup to the coordinator.
	if err = cons3.Close(); err != nil {
		t.Logf("cons3.Close: %v", err)
	}

	// Wait for cons1 and cons2 to detect the rebalance via heartbeat and re-join.
	time.Sleep(5 * time.Second)

	// After rebalance, cons1+cons2 together must cover all 3 partitions.
	postResults := groupCollect(t, []*client.Consumer{cons1, cons2}, nPartitions, 15*time.Second)
	for p := int32(0); p < nPartitions; p++ {
		_, in1 := postResults[0][p]
		_, in2 := postResults[1][p]
		if !in1 && !in2 {
			t.Errorf("after rebalance: partition %d not covered by cons1 or cons2", p)
		}
	}

	// Produce 10 more batches and verify both remaining consumers receive them.
	produceRoundRobin(t, ctx, prod, "leave-topic", nPartitions, 10, "leave-post")

	// Poll until records with offset >= initialBatches appear from all 3 partitions.
	deadline := time.Now().Add(30 * time.Second)
	newRecordsSeen := make(map[int32]bool)
	for time.Now().Before(deadline) && len(newRecordsSeen) < nPartitions {
		for i, c := range []*client.Consumer{cons1, cons2} {
			pollCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			recs, pollErr := c.Poll(pollCtx, 1000)
			cancel()
			if pollErr != nil {
				t.Logf("post-rebalance poll consumer %d: %v", i+1, pollErr)
				continue
			}
			for _, r := range recs {
				if r.Offset >= int64(initialBatches) {
					newRecordsSeen[r.PartitionID] = true
				}
			}
		}
	}
	if len(newRecordsSeen) < nPartitions {
		t.Errorf("after rebalance: new batches seen on %d/%d partitions, want all %d",
			len(newRecordsSeen), nPartitions, nPartitions)
	}
}

// TestGroup_SessionTimeout_EvictsMember verifies that a consumer which stops
// heartbeating is evicted after session_timeout_ms and Consumer1 acquires both
// partitions without Consumer2 sending LeaveGroup.
func TestGroup_SessionTimeout_EvictsMember(t *testing.T) {
	if brokerBinary == "" {
		t.Skip("broker binary not available; skipping cluster test")
	}

	const (
		sessionTimeoutMs = 3000
		sweepIntervalMs  = 5000
		nPartitions      = 2
		batchesPerPart   = 10
	)

	nodes := []clusterNode{
		{1, 61093, 61091, 61092},
		{2, 62093, 62091, 62092},
		{3, 63093, 63091, 63092},
	}
	peers := clusterPeers(nodes)
	for _, n := range nodes {
		startBrokerSweep(t, n.id, n.raftPort, n.mgmtPort, n.dataPort, t.TempDir(), peers, sweepIntervalMs)
	}

	adminAddr := fmt.Sprintf("localhost:%d", nodes[0].mgmtPort)
	waitClusterReady(t, adminAddr, 3, 30*time.Second)

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()
	if _, err = ac.CreateTopic(ctx, client.CreateTopicRequest{
		Name:              "timeout-topic",
		PartitionCount:    nPartitions,
		ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	waitPartitionsLeaders(t, adminAddr, "timeout-topic", nPartitions, 15*time.Second)

	bootstrapAddrs := clusterBootstrapAddrs(nodes)
	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	produceRoundRobin(t, ctx, prod, "timeout-topic", nPartitions, nPartitions*batchesPerPart, "timeout")

	newTimeoutConsumer := func() *client.Consumer {
		c, cerr := client.NewConsumer(client.ConsumerConfig{
			Config: client.Config{
				BootstrapServers: bootstrapAddrs,
				RequestTimeout:   10 * time.Second,
			},
			GroupID:             "timeout-group",
			MaxFetchBytes:       1 << 20,
			MaxFetchWaitMs:      1000,
			AutoOffsetReset:     client.OffsetResetEarliest,
			HeartbeatIntervalMs: 1000,
			SessionTimeoutMs:    sessionTimeoutMs,
		})
		if cerr != nil {
			t.Fatalf("new consumer: %v", cerr)
		}
		return c
	}

	cons1 := newTimeoutConsumer()
	defer cons1.Close() //nolint:errcheck
	if err = cons1.Subscribe([]string{"timeout-topic"}); err != nil {
		t.Fatalf("cons1.Subscribe: %v", err)
	}

	cons2 := newTimeoutConsumer()
	if err = cons2.Subscribe([]string{"timeout-topic"}); err != nil {
		_ = cons2.SimulateCrash
		t.Fatalf("cons2.Subscribe: %v", err)
	}

	// Wait for the initial rebalance to settle.
	time.Sleep(5 * time.Second)

	// Simulate a crash: stop heartbeats without sending LeaveGroup.
	cons2.SimulateCrash()

	// Poll until cons1 accumulates records from all partitions.
	// Budget covers: sessionTimeoutMs (3s) + sweepIntervalMs (5s) + heartbeat (1s) + drain + margin.
	// Use a 5s poll context so sequential partition fetches (2 partitions × up to 1s wait each)
	// never race against the context deadline.
	allRecs := make(map[int32][]client.Record)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, recs := range allRecs {
			total += len(recs)
		}
		if total >= nPartitions*batchesPerPart && len(allRecs) >= nPartitions {
			break
		}
		pollCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		recs, pollErr := cons1.Poll(pollCtx, 5000)
		cancel()
		if pollErr != nil {
			t.Logf("cons1.Poll: %v", pollErr)
			continue
		}
		for _, r := range recs {
			allRecs[r.PartitionID] = append(allRecs[r.PartitionID], r)
		}
	}

	total := 0
	for _, recs := range allRecs {
		total += len(recs)
	}
	if total < nPartitions*batchesPerPart {
		t.Errorf("cons1 received %d total records, want >= %d (both partitions after eviction)",
			total, nPartitions*batchesPerPart)
	}
	if len(allRecs) < nPartitions {
		t.Errorf("cons1 received records from %d partitions, want %d (all partitions after eviction)",
			len(allRecs), nPartitions)
	}
}

// TestGroup_OffsetCommit_SurvivesRestart verifies that committed offsets persist
// across a consumer restart so Consumer2 resumes from offset 10, not 0.
func TestGroup_OffsetCommit_SurvivesRestart(t *testing.T) {
	if brokerBinary == "" {
		t.Skip("broker binary not available; skipping cluster test")
	}

	const (
		totalBatches    = 20
		firstHalf       = 10
		commitOffset    = int64(10) // next-to-read after consuming offsets 0–9
	)

	nodes := []clusterNode{
		{1, 64093, 64091, 64092},
		{2, 65093, 65091, 65092},
		{3, 63193, 63191, 63192},
	}
	peers := clusterPeers(nodes)
	for _, n := range nodes {
		startBroker(t, n.id, n.raftPort, n.mgmtPort, n.dataPort, t.TempDir(), peers)
	}

	adminAddr := fmt.Sprintf("localhost:%d", nodes[0].mgmtPort)
	waitClusterReady(t, adminAddr, 3, 30*time.Second)

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{adminAddr},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()
	if _, err = ac.CreateTopic(ctx, client.CreateTopicRequest{
		Name:              "offset-topic",
		PartitionCount:    1,
		ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	waitPartitionsLeaders(t, adminAddr, "offset-topic", 1, 15*time.Second)

	bootstrapAddrs := clusterBootstrapAddrs(nodes)
	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   10 * time.Second,
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	for b := 0; b < totalBatches; b++ {
		sendOneBatch(t, ctx, prod, "offset-topic", 0, b, "offset-batch", 15*time.Second)
	}

	newOffsetConsumer := func() *client.Consumer {
		c, cerr := client.NewConsumer(client.ConsumerConfig{
			Config: client.Config{
				BootstrapServers: bootstrapAddrs,
				RequestTimeout:   10 * time.Second,
			},
			GroupID:             "offset-group",
			MaxFetchBytes:       1 << 20,
			MaxFetchWaitMs:      1000,
			AutoOffsetReset:     client.OffsetResetEarliest,
			HeartbeatIntervalMs: 1000,
		})
		if cerr != nil {
			t.Fatalf("new consumer: %v", cerr)
		}
		return c
	}

	// Consumer1: join, poll first 10 records, commit offset 10, then close.
	cons1 := newOffsetConsumer()
	if err = cons1.Subscribe([]string{"offset-topic"}); err != nil {
		_ = cons1.Close()
		t.Fatalf("cons1.Subscribe: %v", err)
	}

	received1 := make([]client.Record, 0, firstHalf)
	deadline1 := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline1) && len(received1) < firstHalf {
		pollCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		recs, pollErr := cons1.Poll(pollCtx, 3000)
		cancel()
		if pollErr != nil {
			t.Fatalf("cons1.Poll: %v", pollErr)
		}
		received1 = append(received1, recs...)
	}
	if len(received1) < firstHalf {
		_ = cons1.Close()
		t.Fatalf("cons1 received %d records, want >= %d", len(received1), firstHalf)
	}

	commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
	if err = cons1.CommitOffsets(commitCtx, map[client.TP]int64{
		{Topic: "offset-topic", PartitionID: 0}: commitOffset,
	}); err != nil {
		commitCancel()
		_ = cons1.Close()
		t.Fatalf("CommitOffsets: %v", err)
	}
	commitCancel()

	if err = cons1.Close(); err != nil {
		t.Logf("cons1.Close: %v", err)
	}

	// Consumer2: same group, re-joins and must start at committed offset 10.
	cons2 := newOffsetConsumer()
	defer cons2.Close() //nolint:errcheck
	if err = cons2.Subscribe([]string{"offset-topic"}); err != nil {
		t.Fatalf("cons2.Subscribe: %v", err)
	}

	received2 := make([]client.Record, 0, totalBatches-firstHalf)
	deadline2 := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline2) && len(received2) < totalBatches-firstHalf {
		pollCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		recs, pollErr := cons2.Poll(pollCtx, 3000)
		cancel()
		if pollErr != nil {
			t.Fatalf("cons2.Poll: %v", pollErr)
		}
		received2 = append(received2, recs...)
	}

	if len(received2) < totalBatches-firstHalf {
		t.Errorf("cons2 received %d records, want >= %d", len(received2), totalBatches-firstHalf)
	}

	for _, r := range received2 {
		if r.Offset < commitOffset {
			t.Errorf("cons2 re-delivered offset %d (< committed offset %d)", r.Offset, commitOffset)
		}
	}

	lowestOffset := int64(-1)
	for _, r := range received2 {
		if lowestOffset < 0 || r.Offset < lowestOffset {
			lowestOffset = r.Offset
		}
	}
	if lowestOffset >= 0 && lowestOffset != commitOffset {
		t.Errorf("cons2 first offset: got %d, want %d", lowestOffset, commitOffset)
	}
}
