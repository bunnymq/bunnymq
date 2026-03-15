package client

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---- test server helpers ----

type testServer struct {
	grpc    *grpc.Server
	addr    string
	mgmtSvc *fakeMgmtSvc
	dataSvc *fakeDataSvc
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	ts := &testServer{
		grpc:    s,
		addr:    lis.Addr().String(),
		mgmtSvc: &fakeMgmtSvc{},
		dataSvc: &fakeDataSvc{},
	}
	pb.RegisterManagementServiceServer(s, ts.mgmtSvc)
	pb.RegisterDataServiceServer(s, ts.dataSvc)
	go s.Serve(lis) //nolint:errcheck
	t.Cleanup(s.Stop)
	return ts
}

// fakeMgmtSvc is a configurable ManagementService stub.
type fakeMgmtSvc struct {
	pb.UnimplementedManagementServiceServer
	mu             sync.Mutex
	topicResp      *pb.DescribeTopicResponse
	topicErr       error
	clusterResp    *pb.DescribeClusterResponse
	clusterErr     error
}

func (f *fakeMgmtSvc) DescribeTopic(_ context.Context, _ *pb.DescribeTopicRequest) (*pb.DescribeTopicResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.topicErr != nil {
		return nil, f.topicErr
	}
	return f.topicResp, nil
}

func (f *fakeMgmtSvc) DescribeCluster(_ context.Context, _ *pb.DescribeClusterRequest) (*pb.DescribeClusterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.clusterErr != nil {
		return nil, f.clusterErr
	}
	return f.clusterResp, nil
}

// fakeDataSvc is a configurable DataService stub.
type fakeDataSvc struct {
	pb.UnimplementedDataServiceServer
	mu        sync.Mutex
	responses []produceResult
	calls     []produceCall
}

type produceResult struct {
	offset int64
	err    error
}

type produceCall struct {
	topic       string
	partitionID int32
	batchData   []byte
}

func (f *fakeDataSvc) addResult(offset int64, err error) {
	f.mu.Lock()
	f.responses = append(f.responses, produceResult{offset, err})
	f.mu.Unlock()
}

func (f *fakeDataSvc) Produce(_ context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, produceCall{req.Topic, req.PartitionId, req.BatchData})
	if len(f.responses) == 0 {
		return &pb.ProduceResponse{Offset: 0}, nil
	}
	r := f.responses[0]
	f.responses = f.responses[1:]
	if r.err != nil {
		return nil, r.err
	}
	return &pb.ProduceResponse{Offset: r.offset}, nil
}

// notLeaderErr creates a gRPC error carrying BunnyErrorDetail(NOT_LEADER) and
// NotLeaderDetail with the given leader address.
func notLeaderErr(leaderAddr string) error {
	st, _ := status.New(codes.FailedPrecondition, "not leader").WithDetails(
		&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_NOT_LEADER},
		&pb.NotLeaderDetail{LeaderAddress: leaderAddr},
	)
	return st.Err()
}

// unavailableErr creates a gRPC error carrying BunnyErrorDetail(UNAVAILABLE).
func unavailableErr() error {
	st, _ := status.New(codes.Unavailable, "unavailable").WithDetails(
		&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_UNAVAILABLE},
	)
	return st.Err()
}

// newProducerForTest creates a Producer pointing at the given bootstrap address(es)
// with a short request timeout and the given retry policy.
func newProducerForTest(t *testing.T, bootstrapAddrs []string, retryPolicy RetryPolicy) *Producer {
	t.Helper()
	p, err := NewProducer(ProducerConfig{
		Config: Config{
			BootstrapServers: bootstrapAddrs,
			RequestTimeout:   2 * time.Second,
			RetryPolicy:      retryPolicy,
		},
		DefaultAcks:      AcksAll,
		MetadataCacheTTL: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() { p.Close() }) //nolint:errcheck
	return p
}

// setTopicMeta configures the fake management server to return 3-partition topic
// metadata with each partition leader at the given Data API address.
func setTopicMeta(ts *testServer, topic string, partitionCount int, leaderAddr string) {
	parts := make([]*pb.PartitionInfo, partitionCount)
	for i := range parts {
		parts[i] = &pb.PartitionInfo{PartitionId: int32(i), LeaderNodeId: 1}
	}
	ts.mgmtSvc.mu.Lock()
	ts.mgmtSvc.topicResp = &pb.DescribeTopicResponse{
		Topic:      &pb.TopicInfo{Name: topic, PartitionCount: int32(partitionCount)},
		Partitions: parts,
	}
	ts.mgmtSvc.clusterResp = &pb.DescribeClusterResponse{
		Nodes: []*pb.NodeInfo{{NodeId: 1, Address: leaderAddr}},
	}
	ts.mgmtSvc.mu.Unlock()
}

// ---- tests ----

func TestProducer_Send_RoundRobin(t *testing.T) {
	ts := newTestServer(t)
	setTopicMeta(ts, "my-topic", 3, ts.addr)

	p := newProducerForTest(t, []string{ts.addr}, RetryPolicy{MaxRetries: 3})

	for i := range 6 {
		_, err := p.Send(context.Background(), "my-topic", nil, []byte("v"), nil, AcksAll)
		if err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.calls
	ts.dataSvc.mu.Unlock()

	if len(calls) != 6 {
		t.Fatalf("expected 6 Produce calls, got %d", len(calls))
	}

	counts := make(map[int32]int)
	for _, c := range calls {
		counts[c.partitionID]++
	}
	for pid, cnt := range counts {
		if cnt != 2 {
			t.Errorf("partition %d: want 2 calls, got %d", pid, cnt)
		}
	}
	if len(counts) != 3 {
		t.Errorf("expected 3 distinct partitions, got %d", len(counts))
	}
}

func TestProducer_Send_KeyHash(t *testing.T) {
	ts := newTestServer(t)
	setTopicMeta(ts, "my-topic", 3, ts.addr)

	p := newProducerForTest(t, []string{ts.addr}, RetryPolicy{MaxRetries: 3})

	key := []byte("foo")
	for i := range 3 {
		_, err := p.Send(context.Background(), "my-topic", key, []byte("v"), nil, AcksAll)
		if err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.calls
	ts.dataSvc.mu.Unlock()

	if len(calls) != 3 {
		t.Fatalf("expected 3 Produce calls, got %d", len(calls))
	}

	first := calls[0].partitionID
	for _, c := range calls[1:] {
		if c.partitionID != first {
			t.Errorf("partition changed across calls: want %d, got %d", first, c.partitionID)
		}
	}
}

func TestProducer_Send_NotLeader_Retry(t *testing.T) {
	tsA := newTestServer(t)
	tsB := newTestServer(t)

	// tsA: Management provides metadata pointing to tsA as leader; first Produce returns NOT_LEADER pointing to tsB.
	setTopicMeta(tsA, "my-topic", 1, tsA.addr)
	tsA.dataSvc.addResult(0, notLeaderErr(tsB.addr))

	// tsB: only needs to serve Produce and return success.
	tsB.dataSvc.addResult(42, nil)

	p := newProducerForTest(t, []string{tsA.addr}, RetryPolicy{MaxRetries: 3})

	offset, err := p.Send(context.Background(), "my-topic", []byte("k"), []byte("v"), nil, AcksAll)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if offset != 42 {
		t.Errorf("offset = %d, want 42", offset)
	}

	tsA.dataSvc.mu.Lock()
	callsA := len(tsA.dataSvc.calls)
	tsA.dataSvc.mu.Unlock()
	tsB.dataSvc.mu.Lock()
	callsB := len(tsB.dataSvc.calls)
	tsB.dataSvc.mu.Unlock()

	if callsA != 1 {
		t.Errorf("server A Produce calls = %d, want 1", callsA)
	}
	if callsB != 1 {
		t.Errorf("server B Produce calls = %d, want 1", callsB)
	}
}

func TestProducer_Send_MaxRetriesExceeded(t *testing.T) {
	ts := newTestServer(t)
	setTopicMeta(ts, "my-topic", 1, ts.addr)

	// Server always returns NOT_LEADER pointing to itself (so we keep retrying).
	for range 10 {
		ts.dataSvc.addResult(0, notLeaderErr(ts.addr))
	}

	maxRetries := 2
	p := newProducerForTest(t, []string{ts.addr}, RetryPolicy{
		MaxRetries:     maxRetries,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})

	_, err := p.Send(context.Background(), "my-topic", []byte("k"), []byte("v"), nil, AcksAll)
	if err == nil {
		t.Fatal("expected error after MaxRetries exceeded, got nil")
	}

	ts.dataSvc.mu.Lock()
	totalCalls := len(ts.dataSvc.calls)
	ts.dataSvc.mu.Unlock()

	wantCalls := maxRetries + 1
	if totalCalls != wantCalls {
		t.Errorf("Produce calls = %d, want %d (MaxRetries=%d)", totalCalls, wantCalls, maxRetries)
	}
}

func TestProducer_Send_Unavailable_Backoff(t *testing.T) {
	ts := newTestServer(t)
	setTopicMeta(ts, "my-topic", 1, ts.addr)

	ts.dataSvc.addResult(0, unavailableErr())
	ts.dataSvc.addResult(0, unavailableErr())
	ts.dataSvc.addResult(99, nil)

	initialBackoff := 20 * time.Millisecond
	p := newProducerForTest(t, []string{ts.addr}, RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: initialBackoff,
		MaxBackoff:     1 * time.Second,
		BackoffFactor:  2.0,
	})

	start := time.Now()
	_, err := p.Send(context.Background(), "my-topic", []byte("k"), []byte("v"), nil, AcksAll)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if elapsed < initialBackoff {
		t.Errorf("elapsed %v < InitialBackoff %v; expected backoff between retries", elapsed, initialBackoff)
	}
}

func TestProducer_SendBatch_Success(t *testing.T) {
	ts := newTestServer(t)
	setTopicMeta(ts, "my-topic", 1, ts.addr)
	ts.dataSvc.addResult(77, nil)

	p := newProducerForTest(t, []string{ts.addr}, RetryPolicy{MaxRetries: 3})

	batchData := []byte{0x01, 0x02, 0x03, 0x04}
	offset, err := p.SendBatch(context.Background(), "my-topic", 0, batchData, AcksAll)
	if err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
	if offset != 77 {
		t.Errorf("offset = %d, want 77", offset)
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.calls
	ts.dataSvc.mu.Unlock()

	if len(calls) != 1 {
		t.Fatalf("expected 1 Produce call, got %d", len(calls))
	}
	if string(calls[0].batchData) != string(batchData) {
		t.Errorf("batchData mismatch: got %v, want %v", calls[0].batchData, batchData)
	}
}

func TestProducer_RefreshMeta_TriesAllBootstrap(t *testing.T) {
	// Server A: DescribeTopic fails.
	tsA := newTestServer(t)
	tsA.mgmtSvc.mu.Lock()
	tsA.mgmtSvc.topicErr = status.Error(codes.Unavailable, "down")
	tsA.mgmtSvc.mu.Unlock()

	// Server B: DescribeTopic + DescribeCluster succeed.
	tsB := newTestServer(t)
	setTopicMeta(tsB, "my-topic", 2, tsB.addr)
	tsB.dataSvc.addResult(0, nil)

	// Bootstrap with A first, then B.
	p, err := NewProducer(ProducerConfig{
		Config: Config{
			BootstrapServers: []string{tsA.addr, tsB.addr},
			RequestTimeout:   2 * time.Second,
			RetryPolicy:      RetryPolicy{MaxRetries: 3},
		},
		DefaultAcks:      AcksAll,
		MetadataCacheTTL: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close() //nolint:errcheck

	// metaFor should skip A and succeed via B.
	meta, err := p.metaFor(context.Background(), "my-topic")
	if err != nil {
		t.Fatalf("metaFor: %v", err)
	}
	if meta.PartitionCount != 2 {
		t.Errorf("PartitionCount = %d, want 2", meta.PartitionCount)
	}
}

// ---- unit test for selectPartition (no network) ----

func TestSelectPartition_RoundRobin(t *testing.T) {
	var counter atomic.Int64
	partitions := []int32{
		selectPartition(nil, 3, &counter),
		selectPartition(nil, 3, &counter),
		selectPartition(nil, 3, &counter),
		selectPartition(nil, 3, &counter),
		selectPartition(nil, 3, &counter),
		selectPartition(nil, 3, &counter),
	}
	counts := make(map[int32]int)
	for _, p := range partitions {
		counts[p]++
	}
	for pid, cnt := range counts {
		if cnt != 2 {
			t.Errorf("partition %d: want 2, got %d", pid, cnt)
		}
	}
}

func TestSelectPartition_KeyHash(t *testing.T) {
	var counter atomic.Int64
	p0 := selectPartition([]byte("foo"), 3, &counter)
	p1 := selectPartition([]byte("foo"), 3, &counter)
	p2 := selectPartition([]byte("foo"), 3, &counter)
	if p0 != p1 || p1 != p2 {
		t.Errorf("key hash not stable: %d %d %d", p0, p1, p2)
	}
}

// Verify that NewProducer uses insecure credentials by default (no TLS).
func TestNewProducer_InsecureDefault(t *testing.T) {
	ts := newTestServer(t)
	setTopicMeta(ts, "t", 1, ts.addr)

	// NewProducer with insecure conn (default, no TLS field set).
	p, err := NewProducer(ProducerConfig{
		Config: Config{
			BootstrapServers: []string{ts.addr},
			RequestTimeout:   2 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close() //nolint:errcheck

	// Verify connectivity by calling metaFor.
	_, err = p.metaFor(context.Background(), "t")
	if err != nil {
		t.Fatalf("metaFor: %v", err)
	}
}

// Verify Flush is a no-op.
func TestProducer_Flush_Noop(t *testing.T) {
	p := &Producer{config: ProducerConfig{}}
	if err := p.Flush(context.Background()); err != nil {
		t.Errorf("Flush() = %v, want nil", err)
	}
}

// Unused import guard — grpc used via ts.grpc field.
var _ = grpc.NewServer
var _ = insecure.NewCredentials
