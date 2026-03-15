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
		{2, 59093, 59091, 59092},
		{3, 69093, 69091, 69092},
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
		{1, 79093, 79091, 79092},
		{2, 89093, 89091, 89092},
		{3, 99093, 99091, 99092},
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
	t.Logf("killing leader node %d (index %d) for FetchDuringElection", nodes[leaderIdx].id, leaderIdx)
	if err = cmds[leaderIdx].Process.Kill(); err != nil {
		t.Fatalf("kill leader: %v", err)
	}
	_ = cmds[leaderIdx].Wait()

	waitPartitionsLeaders(t, survivorMgmtAddr, "failover-fetch-topic", 1, 15*time.Second)

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
