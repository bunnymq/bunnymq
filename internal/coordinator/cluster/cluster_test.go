package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"

	cmetrics "github.com/bunnymq/bunnymq/internal/cluster"
	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
)

// --- stub raft host ---

type stubRaftHost struct {
	mu                    sync.Mutex
	lookupFn              func(q metadata.MetadataQuery) (any, error)
	syncProposeFn         func(cmd metadata.MetadataCommand) (sm.Result, error)
	proposePartFn         func(shardID uint64, cmd partition.PartitionCommand) error
	startPartitionShardFn func(shardID uint64, peers map[uint64]string, join bool, topic string, partitionID int32) error
	stopPartitionShardFn  func(shardID uint64) error
	getLeaderIDFn         func(shardID uint64) (uint64, uint64, bool, error)
	proposePartCalls      []uint64 // shard IDs called
	syncProposeCalls      []metadata.MetadataCommand
}

func (s *stubRaftHost) StartMetadataShard(_ map[uint64]string, _ bool, _ sm.CreateStateMachineFunc) error {
	return nil
}

func (s *stubRaftHost) StartPartitionShard(shardID uint64, peers map[uint64]string, join bool, topic string, partitionID int32) error {
	if s.startPartitionShardFn != nil {
		return s.startPartitionShardFn(shardID, peers, join, topic, partitionID)
	}
	return nil
}

func (s *stubRaftHost) StopPartitionShard(shardID uint64) error {
	if s.stopPartitionShardFn != nil {
		return s.stopPartitionShardFn(shardID)
	}
	return nil
}

func (s *stubRaftHost) GetLeaderID(shardID uint64) (uint64, uint64, bool, error) {
	if s.getLeaderIDFn != nil {
		return s.getLeaderIDFn(shardID)
	}
	return 0, 0, false, nil
}

func (s *stubRaftHost) SyncProposeMetadata(_ context.Context, cmd metadata.MetadataCommand) (sm.Result, error) {
	s.mu.Lock()
	s.syncProposeCalls = append(s.syncProposeCalls, cmd)
	s.mu.Unlock()
	if s.syncProposeFn != nil {
		return s.syncProposeFn(cmd)
	}
	return metadata.OKResult(), nil
}

func (s *stubRaftHost) LookupMetadata(_ context.Context, q metadata.MetadataQuery) (any, error) {
	if s.lookupFn != nil {
		return s.lookupFn(q)
	}
	return nil, errors.New("not configured")
}

func (s *stubRaftHost) ProposePartition(_ context.Context, shardID uint64, cmd partition.PartitionCommand) error {
	s.mu.Lock()
	s.proposePartCalls = append(s.proposePartCalls, shardID)
	s.mu.Unlock()
	if s.proposePartFn != nil {
		return s.proposePartFn(shardID, cmd)
	}
	return nil
}

func (s *stubRaftHost) proposedShards() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, len(s.proposePartCalls))
	copy(out, s.proposePartCalls)
	return out
}

func (s *stubRaftHost) syncProposeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.syncProposeCalls)
}

// --- stub data coordinator ---

type stubDataCoord struct {
	mu                         sync.Mutex
	startPartitionReplicaCalls []partitionKey
	stopPartitionReplicaCalls  []partitionKey
}

type partitionKey struct {
	topic       string
	partitionID int32
	shardID     uint64
}

func (s *stubDataCoord) StartPartitionReplica(topic string, partitionID int32, shardID uint64) {
	s.mu.Lock()
	s.startPartitionReplicaCalls = append(s.startPartitionReplicaCalls, partitionKey{topic, partitionID, shardID})
	s.mu.Unlock()
}

func (s *stubDataCoord) StopPartitionReplica(topic string, partitionID int32, shardID uint64) {
	s.mu.Lock()
	s.stopPartitionReplicaCalls = append(s.stopPartitionReplicaCalls, partitionKey{topic, partitionID, shardID})
	s.mu.Unlock()
}

func (s *stubDataCoord) startCalls() []partitionKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]partitionKey, len(s.startPartitionReplicaCalls))
	copy(out, s.startPartitionReplicaCalls)
	return out
}

// --- helpers ---

func makeNodes(n int) []*metadata.NodeInfo {
	nodes := make([]*metadata.NodeInfo, n)
	for i := range n {
		nodes[i] = &metadata.NodeInfo{NodeID: uint64(i + 1), Address: "addr"}
	}
	return nodes
}

func newCoord(host raftHostIface) *ClusterCoordinator {
	return NewClusterCoordinator(CoordinatorConfig{
		NodeID:             1,
		RaftAddress:        "localhost:1",
		Peers:              map[uint64]string{1: "localhost:1"},
		BootstrapTimeoutMs: 5000,
	}, host, &stubDataCoord{}, cmetrics.NoopRaftMetrics(), zap.NewNop())
}

func newCoordWithData(host raftHostIface, dc DataCoordinatorIface) *ClusterCoordinator {
	return NewClusterCoordinator(CoordinatorConfig{
		NodeID:                1,
		RaftAddress:           "localhost:1",
		DataDir:               "/tmp/test",
		Peers:                 map[uint64]string{1: "localhost:1"},
		BootstrapTimeoutMs:    5000,
		ReconcileIntervalMs:   3000,
		LeaderCheckIntervalMs: 3000,
	}, host, dc, cmetrics.NoopRaftMetrics(), zap.NewNop())
}

// --- assignReplicas tests ---

func TestAssignReplicas_Distribution(t *testing.T) {
	nodes := makeNodes(5)
	counts := make(map[uint64]int)
	for p := int32(0); p < 5; p++ {
		replicas := assignReplicas(nodes, "my-topic", p, 1)
		counts[replicas[0]]++
	}
	for nodeID, c := range counts {
		if c != 1 {
			t.Errorf("node %d got %d partitions, want 1", nodeID, c)
		}
	}
}

func TestAssignReplicas_RF3(t *testing.T) {
	nodes := makeNodes(3)
	replicas := assignReplicas(nodes, "my-topic", 0, 3)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}
	seen := make(map[uint64]bool)
	for _, id := range replicas {
		if seen[id] {
			t.Errorf("duplicate node %d in replica list", id)
		}
		seen[id] = true
	}
}

func TestAssignReplicas_Deterministic(t *testing.T) {
	nodes := makeNodes(5)
	r1 := assignReplicas(nodes, "topic-x", 2, 2)
	r2 := assignReplicas(nodes, "topic-x", 2, 2)
	if len(r1) != len(r2) {
		t.Fatal("lengths differ")
	}
	for i := range r1 {
		if r1[i] != r2[i] {
			t.Errorf("replica[%d]: %d != %d", i, r1[i], r2[i])
		}
	}
}

// --- CreateTopic tests ---

func TestCreateTopic_ValidatesName(t *testing.T) {
	host := &stubRaftHost{}
	cc := newCoord(host)
	_, err := cc.CreateTopic(context.Background(), "invalid name!", 1, 1, TopicConfigOverrides{})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestCreateTopic_ValidatesRF(t *testing.T) {
	nodes := makeNodes(2)
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			if q.Type == metadata.QueryListNodes {
				return nodes, nil
			}
			return nil, errors.New("unexpected query")
		},
	}
	cc := newCoord(host)
	_, err := cc.CreateTopic(context.Background(), "topic", 1, 3, TopicConfigOverrides{})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestCreateTopic_AlreadyExists(t *testing.T) {
	nodes := makeNodes(3)
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			if q.Type == metadata.QueryListNodes {
				return nodes, nil
			}
			return nil, errors.New("unexpected query")
		},
		syncProposeFn: func(_ metadata.MetadataCommand) (sm.Result, error) {
			return metadata.ErrorResult(metadata.ResultErrAlreadyExists, "topic already exists"), nil
		},
	}
	cc := newCoord(host)
	_, err := cc.CreateTopic(context.Background(), "topic", 1, 1, TopicConfigOverrides{})
	if !errors.Is(err, ErrTopicAlreadyExists) {
		t.Fatalf("expected ErrTopicAlreadyExists, got %v", err)
	}
}

// --- DeleteTopic tests ---

func TestDeleteTopic_NotFound(t *testing.T) {
	var proposed atomic.Bool
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			return nil, metadata.ErrNotFound
		},
		syncProposeFn: func(_ metadata.MetadataCommand) (sm.Result, error) {
			proposed.Store(true)
			return metadata.OKResult(), nil
		},
	}
	cc := newCoord(host)
	err := cc.DeleteTopic(context.Background(), "missing")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("expected ErrTopicNotFound, got %v", err)
	}
	if proposed.Load() {
		t.Fatal("SyncProposeMetadata must not be called when topic is not found")
	}
}

// --- Bootstrap tests ---

func TestBootstrap_Timeout(t *testing.T) {
	host := &stubRaftHost{
		lookupFn: func(_ metadata.MetadataQuery) (any, error) {
			return nil, errors.New("no leader")
		},
	}
	cc := NewClusterCoordinator(CoordinatorConfig{
		NodeID:             1,
		Peers:              map[uint64]string{1: "localhost:1"},
		BootstrapTimeoutMs: 100, // very short timeout
	}, host, &stubDataCoord{}, cmetrics.NoopRaftMetrics(), zap.NewNop())

	ctx := context.Background()
	err := cc.Bootstrap(ctx)
	if err == nil {
		t.Fatal("expected timeout error from Bootstrap, got nil")
	}
}

// --- AlterTopicRetention tests ---

func TestAlterTopicRetention_PropagatesShards(t *testing.T) {
	partitions := []*metadata.PartitionMeta{
		{Topic: "t", PartitionID: 0, ShardID: 10},
		{Topic: "t", PartitionID: 1, ShardID: 11},
		{Topic: "t", PartitionID: 2, ShardID: 12},
	}
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			switch q.Type {
			case metadata.QueryGetTopic:
				return &metadata.TopicMeta{Name: "t"}, nil
			case metadata.QueryGetPartitions:
				return partitions, nil
			}
			return nil, errors.New("unexpected query")
		},
	}
	cc := newCoord(host)
	err := cc.AlterTopicRetention(context.Background(), "t", 1000, 2000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for goroutines to fire.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(host.proposedShards()) == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	shards := host.proposedShards()
	if len(shards) != 3 {
		t.Fatalf("expected 3 ProposePartition calls, got %d", len(shards))
	}
	shardSet := make(map[uint64]bool)
	for _, id := range shards {
		shardSet[id] = true
	}
	for _, pm := range partitions {
		if !shardSet[pm.ShardID] {
			t.Errorf("missing ProposePartition for shard %d", pm.ShardID)
		}
	}
}

// --- reconcileOnce tests ---

func makePartitionMeta(topic string, partitionID int32, shardID uint64, replicaIDs []uint64) *metadata.PartitionMeta {
	return &metadata.PartitionMeta{
		Topic:          topic,
		PartitionID:    partitionID,
		ShardID:        shardID,
		ReplicaNodeIDs: replicaIDs,
	}
}

func TestReconcileOnce_StartsExpectedShards(t *testing.T) {
	nodes := []*metadata.NodeInfo{
		{NodeID: 1, Address: "addr1"},
		{NodeID: 2, Address: "addr2"},
	}
	topics := []*metadata.TopicMeta{
		{Name: "topic-a"},
	}
	parts := []*metadata.PartitionMeta{
		makePartitionMeta("topic-a", 0, 10, []uint64{1, 2}),
		makePartitionMeta("topic-a", 1, 11, []uint64{1, 2}),
	}

	var startCalls atomic.Int32
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			switch q.Type {
			case metadata.QueryListTopics:
				return topics, nil
			case metadata.QueryListNodes:
				return nodes, nil
			case metadata.QueryGetPartitions:
				return parts, nil
			}
			return nil, errors.New("unexpected query")
		},
		startPartitionShardFn: func(_ uint64, _ map[uint64]string, _ bool, _ string, _ int32) error {
			startCalls.Add(1)
			return nil
		},
	}
	dc := &stubDataCoord{}
	cc := newCoordWithData(host, dc)

	cc.reconcileOnce(context.Background())

	// Wait for goroutines spawned by reconcileOnce.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if int(startCalls.Load()) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := int(startCalls.Load()); got != 2 {
		t.Fatalf("expected 2 StartPartitionShard calls, got %d", got)
	}
}

func TestReconcileOnce_StopsDeletedShards(t *testing.T) {
	nodes := []*metadata.NodeInfo{{NodeID: 1, Address: "addr1"}}
	topics := []*metadata.TopicMeta{}

	var stopCalls atomic.Int32
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			switch q.Type {
			case metadata.QueryListTopics:
				return topics, nil
			case metadata.QueryListNodes:
				return nodes, nil
			}
			return nil, errors.New("unexpected query")
		},
		stopPartitionShardFn: func(_ uint64) error {
			stopCalls.Add(1)
			return nil
		},
	}
	dc := &stubDataCoord{}
	cc := newCoordWithData(host, dc)

	// Pre-populate a running shard that is no longer in metadata.
	cc.shardMu.Lock()
	cc.runningShards[99] = shardInfo{Topic: "gone", PartitionID: 0, ShardID: 99, Peers: map[uint64]string{1: "addr1"}}
	cc.shardMu.Unlock()

	cc.reconcileOnce(context.Background())

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if int(stopCalls.Load()) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := int(stopCalls.Load()); got != 1 {
		t.Fatalf("expected 1 StopPartitionShard call, got %d", got)
	}
}

func TestReconcileOnce_IdempotentIfMatch(t *testing.T) {
	nodes := []*metadata.NodeInfo{{NodeID: 1, Address: "addr1"}}
	topics := []*metadata.TopicMeta{{Name: "topic-a"}}
	parts := []*metadata.PartitionMeta{
		makePartitionMeta("topic-a", 0, 10, []uint64{1}),
	}

	var startCalls, stopCalls atomic.Int32
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			switch q.Type {
			case metadata.QueryListTopics:
				return topics, nil
			case metadata.QueryListNodes:
				return nodes, nil
			case metadata.QueryGetPartitions:
				return parts, nil
			}
			return nil, errors.New("unexpected query")
		},
		startPartitionShardFn: func(_ uint64, _ map[uint64]string, _ bool, _ string, _ int32) error {
			startCalls.Add(1)
			return nil
		},
		stopPartitionShardFn: func(_ uint64) error {
			stopCalls.Add(1)
			return nil
		},
	}
	dc := &stubDataCoord{}
	cc := newCoordWithData(host, dc)

	// Pre-populate runningShards to match metadata exactly.
	cc.shardMu.Lock()
	cc.runningShards[10] = shardInfo{Topic: "topic-a", PartitionID: 0, ShardID: 10, Peers: map[uint64]string{1: "addr1"}}
	cc.shardMu.Unlock()

	cc.reconcileOnce(context.Background())
	time.Sleep(50 * time.Millisecond)

	if got := int(startCalls.Load()); got != 0 {
		t.Fatalf("expected 0 StartPartitionShard calls, got %d", got)
	}
	if got := int(stopCalls.Load()); got != 0 {
		t.Fatalf("expected 0 StopPartitionShard calls, got %d", got)
	}
}

// --- startShard / stopShard tests ---

func TestStartShard_RegistersWithDataCoord(t *testing.T) {
	host := &stubRaftHost{}
	dc := &stubDataCoord{}
	cc := newCoordWithData(host, dc)

	info := shardInfo{
		Topic:       "my-topic",
		PartitionID: 2,
		ShardID:     20,
		Peers:       map[uint64]string{1: "addr1"},
	}

	cc.startShard(20, info)

	calls := dc.startCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 StartPartitionReplica call, got %d", len(calls))
	}
	if calls[0].topic != "my-topic" || calls[0].partitionID != 2 || calls[0].shardID != 20 {
		t.Errorf("unexpected call args: %+v", calls[0])
	}
}

func TestStopShard_UnregistersBeforeStop(t *testing.T) {
	var stopOrder []string
	var mu sync.Mutex

	host := &stubRaftHost{
		stopPartitionShardFn: func(_ uint64) error {
			mu.Lock()
			stopOrder = append(stopOrder, "raft")
			mu.Unlock()
			return nil
		},
	}

	// Wrap the stub data coord to record call order.
	dc := &orderTrackingDataCoord{
		order: &stopOrder,
		mu:    &mu,
	}

	cc := newCoordWithData(host, dc)
	cc.shardMu.Lock()
	cc.runningShards[5] = shardInfo{Topic: "t", PartitionID: 0, ShardID: 5, Peers: map[uint64]string{1: "x"}}
	cc.shardMu.Unlock()

	cc.stopShard(5, cc.runningShards[5])

	mu.Lock()
	order := make([]string, len(stopOrder))
	copy(order, stopOrder)
	mu.Unlock()

	if len(order) != 2 {
		t.Fatalf("expected 2 calls, got %d: %v", len(order), order)
	}
	if order[0] != "data" || order[1] != "raft" {
		t.Errorf("expected data before raft, got: %v", order)
	}
}

// orderTrackingDataCoord records that StopPartitionReplica was called before
// raft.StopPartitionShard.
type orderTrackingDataCoord struct {
	order *[]string
	mu    *sync.Mutex
}

func (o *orderTrackingDataCoord) StartPartitionReplica(_ string, _ int32, _ uint64) {}
func (o *orderTrackingDataCoord) StopPartitionReplica(_ string, _ int32, _ uint64) {
	o.mu.Lock()
	*o.order = append(*o.order, "data")
	o.mu.Unlock()
}

// --- sweepLeaders tests ---

func TestSweepLeaders_FiresOnChange(t *testing.T) {
	host := &stubRaftHost{
		getLeaderIDFn: func(_ uint64) (uint64, uint64, bool, error) {
			return 2, 5, true, nil // leader=2, term=5
		},
	}
	dc := &stubDataCoord{}
	cc := newCoordWithData(host, dc)
	cc.shardMu.Lock()
	cc.runningShards[10] = shardInfo{Topic: "t", PartitionID: 0, ShardID: 10, Peers: map[uint64]string{1: "a"}}
	cc.shardMu.Unlock()

	cc.sweepLeaders(context.Background())

	if got := host.syncProposeCount(); got != 1 {
		t.Fatalf("expected 1 SyncProposeMetadata call, got %d", got)
	}
	host.mu.Lock()
	cmd := host.syncProposeCalls[0]
	host.mu.Unlock()
	if cmd.Type != metadata.CmdAssignPartitionLeader {
		t.Errorf("unexpected command type: %v", cmd.Type)
	}
	if cmd.AssignPartitionLeader.LeaderNodeID != 2 || cmd.AssignPartitionLeader.LeaderEpoch != 5 {
		t.Errorf("unexpected leader info: %+v", cmd.AssignPartitionLeader)
	}
}

func TestSweepLeaders_SkipsIfUnchanged(t *testing.T) {
	host := &stubRaftHost{
		getLeaderIDFn: func(_ uint64) (uint64, uint64, bool, error) {
			return 1, 3, true, nil
		},
	}
	dc := &stubDataCoord{}
	cc := newCoordWithData(host, dc)
	cc.shardMu.Lock()
	cc.runningShards[10] = shardInfo{Topic: "t", PartitionID: 0, ShardID: 10, Peers: map[uint64]string{1: "a"}}
	cc.shardMu.Unlock()
	// Pre-set lastKnownLeader to match what GetLeaderID will return.
	cc.leaderMu.Lock()
	cc.lastKnownLeader[10] = leaderRecord{nodeID: 1, term: 3}
	cc.leaderMu.Unlock()

	cc.sweepLeaders(context.Background())

	if got := host.syncProposeCount(); got != 0 {
		t.Fatalf("expected 0 SyncProposeMetadata calls, got %d", got)
	}
}

func TestSweepLeaders_ContinuesOnProposeError(t *testing.T) {
	callCount := atomic.Int32{}
	host := &stubRaftHost{
		getLeaderIDFn: func(shardID uint64) (uint64, uint64, bool, error) {
			return shardID, 1, true, nil // different leader per shard to trigger propose
		},
		syncProposeFn: func(_ metadata.MetadataCommand) (sm.Result, error) {
			callCount.Add(1)
			return sm.Result{}, errors.New("propose failed")
		},
	}
	dc := &stubDataCoord{}
	cc := newCoordWithData(host, dc)
	cc.shardMu.Lock()
	cc.runningShards[10] = shardInfo{Topic: "t", PartitionID: 0, ShardID: 10, Peers: map[uint64]string{1: "a"}}
	cc.runningShards[11] = shardInfo{Topic: "t", PartitionID: 1, ShardID: 11, Peers: map[uint64]string{1: "a"}}
	cc.shardMu.Unlock()

	cc.sweepLeaders(context.Background())

	if got := int(callCount.Load()); got != 2 {
		t.Fatalf("expected 2 SyncProposeMetadata calls (one per shard), got %d", got)
	}
}
