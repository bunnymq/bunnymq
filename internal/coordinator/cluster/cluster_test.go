package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
	"go.uber.org/zap"
)

// --- stub raft host ---

type stubRaftHost struct {
	mu               sync.Mutex
	lookupFn         func(q metadata.MetadataQuery) (interface{}, error)
	syncProposeFn    func(cmd metadata.MetadataCommand) (sm.Result, error)
	proposePartFn    func(shardID uint64, cmd partition.PartitionCommand) error
	proposePartCalls []uint64 // shard IDs called
}

func (s *stubRaftHost) StartMetadataShard(_ map[uint64]string, _ bool, _ sm.CreateStateMachineFunc) error {
	return nil
}

func (s *stubRaftHost) SyncProposeMetadata(_ context.Context, cmd metadata.MetadataCommand) (sm.Result, error) {
	if s.syncProposeFn != nil {
		return s.syncProposeFn(cmd)
	}
	return metadata.OKResult(), nil
}

func (s *stubRaftHost) LookupMetadata(_ context.Context, q metadata.MetadataQuery) (interface{}, error) {
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
	}, host, zap.NewNop())
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
		lookupFn: func(q metadata.MetadataQuery) (interface{}, error) {
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
		lookupFn: func(q metadata.MetadataQuery) (interface{}, error) {
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
		lookupFn: func(q metadata.MetadataQuery) (interface{}, error) {
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
		lookupFn: func(_ metadata.MetadataQuery) (interface{}, error) {
			return nil, errors.New("no leader")
		},
	}
	cc := NewClusterCoordinator(CoordinatorConfig{
		NodeID:             1,
		Peers:              map[uint64]string{1: "localhost:1"},
		BootstrapTimeoutMs: 100, // very short timeout
	}, host, zap.NewNop())

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
		lookupFn: func(q metadata.MetadataQuery) (interface{}, error) {
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
