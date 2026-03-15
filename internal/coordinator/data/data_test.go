package data

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

// makeFetchStub builds a stubRaftHost configured for Fetch tests.
// metaFn is the LookupMetadata handler; partFn handles LookupPartition calls.
func makeFetchStub(metaFn func(context.Context, metadata.MetadataQuery) (any, error),
	partFn func(context.Context, uint64, partition.PartitionQuery) (any, error)) *stubRaftHost {
	return &stubRaftHost{
		lookupMetadataFn:  metaFn,
		lookupPartitionFn: partFn,
	}
}

func TestFetch_ImmediateData(t *testing.T) {
	batch := []byte("batch-data")
	stub := makeFetchStub(
		func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			if q.Type != partition.QueryRead {
				t.Errorf("expected QueryRead, got %q", q.Type)
			}
			return partition.PartitionLookupResult{Batches: batch, NextOffset: 10}, nil
		},
	)
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	records, next, err := dc.Fetch(context.Background(), testTopic, testPartID, 0, 1024, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(records) != string(batch) {
		t.Fatalf("expected %q, got %q", batch, records)
	}
	if next != 10 {
		t.Fatalf("expected nextOffset=10, got %d", next)
	}
}

func TestFetch_NoDataNoWait(t *testing.T) {
	stub := makeFetchStub(
		func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			return partition.PartitionLookupResult{Batches: nil, NextOffset: 0}, nil
		},
	)
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	records, next, err := dc.Fetch(context.Background(), testTopic, testPartID, 0, 1024, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if records != nil || next != 0 {
		t.Fatalf("expected (nil, 0, nil), got (%v, %d, %v)", records, next, err)
	}
}

func TestFetch_LongPollWakesOnData(t *testing.T) {
	ch := make(chan struct{})
	callCount := 0

	stub := makeFetchStub(
		func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			switch q.Type {
			case partition.QueryGetNewDataCh:
				return (<-chan struct{})(ch), nil
			case partition.QueryRead:
				callCount++
				if callCount >= 2 {
					return partition.PartitionLookupResult{Batches: []byte("data"), NextOffset: 1}, nil
				}
				return partition.PartitionLookupResult{Batches: nil, NextOffset: 0}, nil
			}
			return nil, nil
		},
	)
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(ch)
	}()

	records, _, err := dc.Fetch(context.Background(), testTopic, testPartID, 0, 1024, 5000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(records) != "data" {
		t.Fatalf("expected data, got %q", records)
	}
}

func TestFetch_LongPollTimeout(t *testing.T) {
	ch := make(chan struct{}) // never closed

	stub := makeFetchStub(
		func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			switch q.Type {
			case partition.QueryGetNewDataCh:
				return (<-chan struct{})(ch), nil
			case partition.QueryRead:
				return partition.PartitionLookupResult{Batches: nil, NextOffset: 0}, nil
			}
			return nil, nil
		},
	)
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	start := time.Now()
	records, next, err := dc.Fetch(context.Background(), testTopic, testPartID, 0, 1024, 50)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if records != nil || next != 0 {
		t.Fatalf("expected (nil, 0, nil) on timeout, got (%v, %d)", records, next)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("returned too early: %v", elapsed)
	}
}

func TestFetch_LongPollCtxCancel(t *testing.T) {
	ch := make(chan struct{}) // never closed

	stub := makeFetchStub(
		func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
			return leaderMeta(), nil
		},
		func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			switch q.Type {
			case partition.QueryGetNewDataCh:
				return (<-chan struct{})(ch), nil
			case partition.QueryRead:
				return partition.PartitionLookupResult{Batches: nil, NextOffset: 0}, nil
			}
			return nil, nil
		},
	)
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, _, err := dc.Fetch(ctx, testTopic, testPartID, 0, 1024, 10000)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestFetch_LongPollLeaderChange(t *testing.T) {
	ch := make(chan struct{})
	metaCallCount := 0

	stub := makeFetchStub(
		func(_ context.Context, q metadata.MetadataQuery) (any, error) {
			if q.Type == metadata.QueryGetPartition {
				metaCallCount++
				if metaCallCount == 1 {
					// First call: leaderCheck passes (this node is leader).
					return leaderMeta(), nil
				}
				// Subsequent calls (inside long-poll loop): different leader.
				return &metadata.PartitionMeta{
					Topic:        testTopic,
					PartitionID:  testPartID,
					ShardID:      testShardID,
					LeaderNodeID: 2,
				}, nil
			}
			// QueryListNodes for nodeAddress lookup.
			return []*metadata.NodeInfo{{NodeID: 2, Address: "other:9090"}}, nil
		},
		func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
			switch q.Type {
			case partition.QueryGetNewDataCh:
				return (<-chan struct{})(ch), nil
			case partition.QueryRead:
				return partition.PartitionLookupResult{Batches: nil, NextOffset: 0}, nil
			}
			return nil, nil
		},
	)
	dc := newCoord(stub)
	dc.StartPartitionReplica(testTopic, testPartID, testShardID)

	_, _, err := dc.Fetch(context.Background(), testTopic, testPartID, 0, 1024, 5000)
	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("expected *NotLeaderError, got %T: %v", err, err)
	}
	if notLeader.LeaderNodeID != 2 {
		t.Fatalf("expected LeaderNodeID=2, got %d", notLeader.LeaderNodeID)
	}
}

func TestFetch_NewDataCh_RaceElimination(t *testing.T) {
	const iters = 200

	for range iters {
		batch := []byte("race-batch")
		ch := make(chan struct{})

		var readMu sync.Mutex
		dataReady := false

		stub := makeFetchStub(
			func(_ context.Context, _ metadata.MetadataQuery) (any, error) {
				return leaderMeta(), nil
			},
			func(_ context.Context, _ uint64, q partition.PartitionQuery) (any, error) {
				switch q.Type {
				case partition.QueryGetNewDataCh:
					return (<-chan struct{})(ch), nil
				case partition.QueryRead:
					readMu.Lock()
					ready := dataReady
					readMu.Unlock()
					if ready {
						return partition.PartitionLookupResult{Batches: batch, NextOffset: 1}, nil
					}
					return partition.PartitionLookupResult{Batches: nil, NextOffset: 0}, nil
				}
				return nil, nil
			},
		)
		dc := newCoord(stub)
		dc.StartPartitionReplica(testTopic, testPartID, testShardID)

		// Simulate a concurrent append: set dataReady and close the channel.
		go func() {
			readMu.Lock()
			dataReady = true
			readMu.Unlock()
			close(ch)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		records, _, err := dc.Fetch(ctx, testTopic, testPartID, 0, 1024, 1000)
		cancel()

		if err != nil {
			t.Fatalf("iter: unexpected error: %v", err)
		}
		if string(records) != string(batch) {
			t.Fatalf("iter: expected batch data, got %q", records)
		}
	}
}
