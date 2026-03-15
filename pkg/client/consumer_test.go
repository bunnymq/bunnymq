package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bunnymq/bunnymq/internal/storage"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc"
)

// ---- consumer-specific fake helpers ----

type consumerTestServer struct {
	grpc    *grpc.Server
	addr    string
	mgmtSvc *fakeMgmtSvc
	dataSvc *fakeConsumerDataSvc
}

type fetchResultEntry struct {
	records []byte
	nextOff int64
	err     error
}

type fetchCallEntry struct {
	topic       string
	partitionID int32
	offset      int64
}

type fakeConsumerDataSvc struct {
	pb.UnimplementedDataServiceServer
	mu      sync.Mutex
	results []fetchResultEntry
	calls   []fetchCallEntry
}

func (f *fakeConsumerDataSvc) addResult(records []byte, nextOff int64, err error) {
	f.mu.Lock()
	f.results = append(f.results, fetchResultEntry{records, nextOff, err})
	f.mu.Unlock()
}

func (f *fakeConsumerDataSvc) Fetch(_ context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fetchCallEntry{req.Topic, req.PartitionId, req.Offset})
	if len(f.results) == 0 {
		return &pb.FetchResponse{}, nil
	}
	r := f.results[0]
	f.results = f.results[1:]
	if r.err != nil {
		return nil, r.err
	}
	return &pb.FetchResponse{Records: r.records, NextOffset: r.nextOff}, nil
}

func newConsumerTestServer(t *testing.T) *consumerTestServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	ts := &consumerTestServer{
		grpc:    s,
		addr:    lis.Addr().String(),
		mgmtSvc: &fakeMgmtSvc{},
		dataSvc: &fakeConsumerDataSvc{},
	}
	pb.RegisterManagementServiceServer(s, ts.mgmtSvc)
	pb.RegisterDataServiceServer(s, ts.dataSvc)
	go s.Serve(lis) //nolint:errcheck
	t.Cleanup(s.Stop)
	return ts
}

func setConsumerTopicMeta(ts *consumerTestServer, topic string, partitionCount int, leaderAddr string) {
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

func newConsumerForTest(t *testing.T, bootstrapAddr string) *Consumer {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig{
		Config: Config{
			BootstrapServers: []string{bootstrapAddr},
			RequestTimeout:   2 * time.Second,
		},
		MaxFetchBytes:  1 << 20,
		MaxFetchWaitMs: 100,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// encodeBatch creates an on-wire batch for the given storage.Record values.
func encodeBatch(t *testing.T, recs []storage.Record) []byte {
	t.Helper()
	data, err := storage.EncodeBatch(recs)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	return data
}

// ---- consumer tests ----

func TestConsumer_Seek_Poll_ReturnsRecords(t *testing.T) {
	ts := newConsumerTestServer(t)
	setConsumerTopicMeta(ts, "my-topic", 1, ts.addr)

	batch := encodeBatch(t, []storage.Record{
		{TimestampMs: 1000, Key: []byte("k1"), Value: []byte("v1")},
		{TimestampMs: 2000, Key: []byte("k2"), Value: []byte("v2")},
	})
	ts.dataSvc.addResult(batch, 2, nil)

	c := newConsumerForTest(t, ts.addr)
	_ = c.Subscribe([]string{"my-topic"})
	c.Seek("my-topic", 0, 0)

	got, err := c.Poll(context.Background(), 1000)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 records, got %d", len(got))
	}
	if got[0].Offset != 0 || got[1].Offset != 1 {
		t.Errorf("offsets: got %d,%d want 0,1", got[0].Offset, got[1].Offset)
	}
	if got[0].TimestampMs != 1000 || got[1].TimestampMs != 2000 {
		t.Errorf("timestamps: got %d,%d want 1000,2000", got[0].TimestampMs, got[1].TimestampMs)
	}
	if string(got[0].Value) != "v1" || string(got[1].Value) != "v2" {
		t.Errorf("values: got %q,%q want v1,v2", got[0].Value, got[1].Value)
	}
	if got[0].Topic != "my-topic" || got[0].PartitionID != 0 {
		t.Errorf("topic/partition: got %q/%d want my-topic/0", got[0].Topic, got[0].PartitionID)
	}
}

func TestConsumer_Poll_AdvancesOffset(t *testing.T) {
	ts := newConsumerTestServer(t)
	setConsumerTopicMeta(ts, "my-topic", 1, ts.addr)

	// First fetch: 3 records (offsets 0,1,2), nextOffset=3.
	batch := encodeBatch(t, []storage.Record{
		{TimestampMs: 100, Value: []byte("a")},
		{TimestampMs: 200, Value: []byte("b")},
		{TimestampMs: 300, Value: []byte("c")},
	})
	ts.dataSvc.addResult(batch, 3, nil)
	// Second fetch: empty (no more records).
	ts.dataSvc.addResult(nil, 3, nil)

	c := newConsumerForTest(t, ts.addr)
	c.Seek("my-topic", 0, 0)

	got, err := c.Poll(context.Background(), 500)
	if err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("first Poll: expected 3 records, got %d", len(got))
	}

	_, err = c.Poll(context.Background(), 500)
	if err != nil {
		t.Fatalf("second Poll: %v", err)
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.calls
	ts.dataSvc.mu.Unlock()

	if len(calls) < 2 {
		t.Fatalf("expected at least 2 Fetch calls, got %d", len(calls))
	}
	if calls[1].offset != 3 {
		t.Errorf("second Poll fetched from offset %d, want 3", calls[1].offset)
	}
}

func TestConsumer_Poll_NotLeader_SkipsPartition(t *testing.T) {
	ts := newConsumerTestServer(t)
	setConsumerTopicMeta(ts, "my-topic", 1, ts.addr)

	newLeaderAddr := "new-leader:9092"
	ts.dataSvc.addResult(nil, 0, notLeaderErr(newLeaderAddr))

	c := newConsumerForTest(t, ts.addr)
	c.Seek("my-topic", 0, 0)

	got, err := c.Poll(context.Background(), 500)
	if err != nil {
		t.Fatalf("Poll returned unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice on NOT_LEADER, got %d records", len(got))
	}

	// Verify meta cache was updated with the new leader address.
	meta := c.meta.Get("my-topic")
	if meta == nil {
		t.Fatal("meta cache entry missing after NOT_LEADER")
	}
	if meta.Leaders[0] != newLeaderAddr {
		t.Errorf("leader cache: got %q, want %q", meta.Leaders[0], newLeaderAddr)
	}
}

func TestConsumer_Poll_MultiPartition(t *testing.T) {
	ts := newConsumerTestServer(t)
	setConsumerTopicMeta(ts, "my-topic", 2, ts.addr)

	batch0 := encodeBatch(t, []storage.Record{{TimestampMs: 100, Value: []byte("p0r0")}})
	batch1 := encodeBatch(t, []storage.Record{{TimestampMs: 200, Value: []byte("p1r0")}})
	// Poll fetches partition 0 first, then partition 1 (order of Seek calls).
	ts.dataSvc.addResult(batch0, 1, nil)
	ts.dataSvc.addResult(batch1, 1, nil)

	c := newConsumerForTest(t, ts.addr)
	c.Seek("my-topic", 0, 0)
	c.Seek("my-topic", 1, 0)

	got, err := c.Poll(context.Background(), 1000)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 records, got %d", len(got))
	}

	// Collect by partition.
	byPart := make(map[int32]Record)
	for _, r := range got {
		byPart[r.PartitionID] = r
	}
	if r, ok := byPart[0]; !ok || string(r.Value) != "p0r0" {
		t.Errorf("partition 0: value=%q", byPart[0].Value)
	}
	if r, ok := byPart[1]; !ok || string(r.Value) != "p1r0" {
		t.Errorf("partition 1: value=%q", r.Value)
	}

	// Verify Fetch was called with the correct partition IDs.
	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.calls
	ts.dataSvc.mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("expected 2 Fetch calls, got %d", len(calls))
	}
	if calls[0].partitionID != 0 || calls[1].partitionID != 1 {
		t.Errorf("fetch order: partitions %d,%d, want 0,1", calls[0].partitionID, calls[1].partitionID)
	}
}

