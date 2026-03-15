package data

import (
	"context"
	"errors"
	"sync"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
	stor "github.com/bunnymq/bunnymq/internal/storage"
)

// stubRaftHost is a minimal raftHostIface for unit tests.
type stubRaftHost struct {
	lookupMetadataFn       func(ctx context.Context, q metadata.MetadataQuery) (any, error)
	lookupPartitionFn      func(ctx context.Context, shardID uint64, q partition.PartitionQuery) (any, error)
	syncProposePartitionFn func(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) (sm.Result, error)
	proposePartitionFn     func(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) error

	syncProposeCalled  bool
	proposeAsyncCalled bool
}

func (s *stubRaftHost) LookupMetadata(ctx context.Context, q metadata.MetadataQuery) (any, error) {
	return s.lookupMetadataFn(ctx, q)
}

func (s *stubRaftHost) LookupPartition(ctx context.Context, shardID uint64, q partition.PartitionQuery) (any, error) {
	return s.lookupPartitionFn(ctx, shardID, q)
}

func (s *stubRaftHost) SyncProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) (sm.Result, error) {
	s.syncProposeCalled = true
	return s.syncProposePartitionFn(ctx, shardID, cmd)
}

func (s *stubRaftHost) ProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) error {
	s.proposeAsyncCalled = true
	return s.proposePartitionFn(ctx, shardID, cmd)
}

const (
	testNodeID  = uint64(1)
	testShardID = uint64(10)
	testTopic   = "test-topic"
	testPartID  = int32(0)
)

// leaderMeta returns a PartitionMeta indicating this node (testNodeID) is leader.
func leaderMeta() *metadata.PartitionMeta {
	return &metadata.PartitionMeta{
		Topic:        testTopic,
		PartitionID:  testPartID,
		ShardID:      testShardID,
		LeaderNodeID: testNodeID,
	}
}

func newCoord(host raftHostIface) *DataCoordinator {
	return NewDataCoordinator(
		DataCoordinatorConfig{NodeID: testNodeID, NodeAddressCacheTTLMs: 0},
		host,
		zap.NewNop(),
	)
}

func TestDataCoordinator_ProduceAll_Success(t *testing.T) {
	stub := &stubRaftHost{
		lookupMetadataFn: func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		syncProposePartitionFn: func(_ context.Context, _ uint64, _ partition.PartitionCommand) (sm.Result, error) {
			return sm.Result{Value: 42}, nil
		},
	}
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	offset, err := dc.Produce(context.Background(), testTopic, testPartID, []byte("batch"), AcksAll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if offset != 42 {
		t.Fatalf("expected offset 42, got %d", offset)
	}
	if !stub.syncProposeCalled {
		t.Fatal("expected SyncProposePartition to be called")
	}
	if stub.proposeAsyncCalled {
		t.Fatal("expected ProposePartition NOT to be called")
	}
}

func TestDataCoordinator_ProduceZero_AsyncFire(t *testing.T) {
	stub := &stubRaftHost{
		lookupMetadataFn: func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		proposePartitionFn: func(_ context.Context, _ uint64, _ partition.PartitionCommand) error {
			return nil
		},
	}
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	offset, err := dc.Produce(context.Background(), testTopic, testPartID, []byte("batch"), AcksZero)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if offset != -1 {
		t.Fatalf("expected offset -1, got %d", offset)
	}
	if stub.syncProposeCalled {
		t.Fatal("expected SyncProposePartition NOT to be called")
	}
	if !stub.proposeAsyncCalled {
		t.Fatal("expected ProposePartition to be called")
	}
}

func TestDataCoordinator_LeaderCheck_NotLeader(t *testing.T) {
	stub := &stubRaftHost{
		lookupMetadataFn: func(_ context.Context, q metadata.MetadataQuery) (any, error) {
			if q.Type == metadata.QueryGetPartition {
				return &metadata.PartitionMeta{
					Topic:        testTopic,
					PartitionID:  testPartID,
					ShardID:      testShardID,
					LeaderNodeID: 2, // different node
				}, nil
			}
			// QueryListNodes
			return []*metadata.NodeInfo{
				{NodeID: 2, Address: "other-node:8080"},
			}, nil
		},
	}
	dc := newCoord(stub)

	_, err := dc.Produce(context.Background(), testTopic, testPartID, []byte("batch"), AcksAll)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("expected *NotLeaderError, got %T: %v", err, err)
	}
	if notLeader.LeaderNodeID != 2 {
		t.Fatalf("expected LeaderNodeID=2, got %d", notLeader.LeaderNodeID)
	}
	if notLeader.LeaderAddress != "other-node:8080" {
		t.Fatalf("expected LeaderAddress=other-node:8080, got %q", notLeader.LeaderAddress)
	}
}

func TestDataCoordinator_LeaderCheck_ShardNotRegistered(t *testing.T) {
	stub := &stubRaftHost{
		lookupMetadataFn: func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
	}
	dc := newCoord(stub) // no StartPartitionReplica call

	_, err := dc.Produce(context.Background(), testTopic, testPartID, []byte("batch"), AcksAll)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestDataCoordinator_RegistryConcurrency(t *testing.T) {
	dc := NewDataCoordinator(
		DataCoordinatorConfig{NodeID: testNodeID},
		nil, // raftHost not used in this test
		zap.NewNop(),
	)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := range goroutines {
		partID := int32(i)
		shardID := uint64(100 + i)
		go func() {
			defer wg.Done()
			dc.StartPartitionReplica(testTopic, partID, shardID)
		}()
		go func() {
			defer wg.Done()
			dc.StopPartitionReplica(testTopic, partID, shardID)
		}()
	}
	wg.Wait()
}

func TestDataCoordinator_GetLatestOffset(t *testing.T) {
	stub := &stubRaftHost{
		lookupMetadataFn: func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		lookupPartitionFn: func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			if q.Type != partition.QueryLatestOffset {
				t.Errorf("expected QueryLatestOffset, got %q", q.Type)
			}
			return int64(42), nil
		},
	}
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	offset, err := dc.GetLatestOffset(context.Background(), testTopic, testPartID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if offset != 42 {
		t.Fatalf("expected 42, got %d", offset)
	}
}

func TestDataCoordinator_GetOffsetByTimestamp_NotFound(t *testing.T) {
	stub := &stubRaftHost{
		lookupMetadataFn: func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		lookupPartitionFn: func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			if q.Type != partition.QueryReadByTime {
				t.Errorf("expected QueryReadByTime, got %q", q.Type)
			}
			return nil, stor.ErrTimestampNotFound
		},
	}
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	_, err := dc.GetOffsetByTimestamp(context.Background(), testTopic, testPartID, 9999999)
	if !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("expected ErrOffsetNotFound, got %v", err)
	}
}
