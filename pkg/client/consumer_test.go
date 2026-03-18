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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestConsumer_Poll_NoReachableServer_SkipsPartition(t *testing.T) {
	ts := newConsumerTestServer(t)
	setConsumerTopicMeta(ts, "my-topic", 1, ts.addr)

	// DescribeTopic fails for the first call → ErrNoReachableServer in fetchPartition.
	ts.mgmtSvc.mu.Lock()
	ts.mgmtSvc.topicFailCount = 1
	ts.mgmtSvc.mu.Unlock()

	c := newConsumerForTest(t, ts.addr)
	c.Seek("my-topic", 0, 0)

	// Poll must not return an error; ErrNoReachableServer is treated as transient.
	got, err := c.Poll(context.Background(), 500)
	if err != nil {
		t.Fatalf("Poll returned unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice on ErrNoReachableServer, got %d records", len(got))
	}

	// Metadata cache must have been invalidated so the next Poll re-discovers the leader.
	if meta := c.meta.Get("my-topic"); meta != nil {
		t.Error("meta cache entry should be invalidated after ErrNoReachableServer")
	}
}

// ---- group mode test helpers ----

type groupConsumerTestServer struct {
	grpc    *grpc.Server
	addr    string
	mgmtSvc *fakeMgmtSvc
	dataSvc *fakeGroupDataSvc
}

type joinGroupResult struct {
	resp *pb.JoinGroupResponse
	err  error
}

type fetchOffsetsResult struct {
	offsets []*pb.PartitionOffset
	err     error
}

type commitOffsetResult struct {
	err error
}

type heartbeatResult struct {
	status pb.HeartbeatStatus
	err    error
}

type leaveGroupCall struct {
	groupID  string
	memberID string
}

type fakeGroupDataSvc struct {
	pb.UnimplementedDataServiceServer
	mu                sync.Mutex
	joinResults       []joinGroupResult
	joinCalls         []*pb.JoinGroupRequest
	commitResults     []commitOffsetResult
	commitCalls       []*pb.CommitOffsetRequest
	fetchOffsResults  []fetchOffsetsResult
	fetchOfsCalls     []*pb.FetchCommittedOffsetsRequest
	getOffsetResult   int64
	getOffsetCalls    int
	heartbeatResults  []heartbeatResult
	heartbeatCalls    []*pb.HeartbeatRequest
	leaveGroupCalls  []leaveGroupCall
}

func (f *fakeGroupDataSvc) JoinGroup(_ context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joinCalls = append(f.joinCalls, req)
	if len(f.joinResults) == 0 {
		return &pb.JoinGroupResponse{}, nil
	}
	r := f.joinResults[0]
	f.joinResults = f.joinResults[1:]
	return r.resp, r.err
}

func (f *fakeGroupDataSvc) FetchCommittedOffsets(_ context.Context, req *pb.FetchCommittedOffsetsRequest) (*pb.FetchCommittedOffsetsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetchOfsCalls = append(f.fetchOfsCalls, req)
	if len(f.fetchOffsResults) == 0 {
		return &pb.FetchCommittedOffsetsResponse{}, nil
	}
	r := f.fetchOffsResults[0]
	f.fetchOffsResults = f.fetchOffsResults[1:]
	return &pb.FetchCommittedOffsetsResponse{Offsets: r.offsets}, r.err
}

func (f *fakeGroupDataSvc) CommitOffset(_ context.Context, req *pb.CommitOffsetRequest) (*pb.CommitOffsetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitCalls = append(f.commitCalls, req)
	if len(f.commitResults) == 0 {
		return &pb.CommitOffsetResponse{}, nil
	}
	r := f.commitResults[0]
	f.commitResults = f.commitResults[1:]
	return &pb.CommitOffsetResponse{}, r.err
}

func (f *fakeGroupDataSvc) GetOffsets(_ context.Context, _ *pb.GetOffsetsRequest) (*pb.GetOffsetsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getOffsetCalls++
	return &pb.GetOffsetsResponse{Offset: f.getOffsetResult}, nil
}

func (f *fakeGroupDataSvc) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeatCalls = append(f.heartbeatCalls, req)
	if len(f.heartbeatResults) == 0 {
		return &pb.HeartbeatResponse{Status: pb.HeartbeatStatus_HEARTBEAT_OK}, nil
	}
	r := f.heartbeatResults[0]
	f.heartbeatResults = f.heartbeatResults[1:]
	if r.err != nil {
		return nil, r.err
	}
	return &pb.HeartbeatResponse{Status: r.status}, nil
}

func (f *fakeGroupDataSvc) LeaveGroup(_ context.Context, req *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaveGroupCalls = append(f.leaveGroupCalls, leaveGroupCall{req.GroupId, req.MemberId})
	return &pb.LeaveGroupResponse{}, nil
}

func (f *fakeGroupDataSvc) Fetch(_ context.Context, _ *pb.FetchRequest) (*pb.FetchResponse, error) {
	return &pb.FetchResponse{}, nil
}

func newGroupConsumerTestServer(t *testing.T) *groupConsumerTestServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	ts := &groupConsumerTestServer{
		grpc:    s,
		addr:    lis.Addr().String(),
		mgmtSvc: &fakeMgmtSvc{},
		dataSvc: &fakeGroupDataSvc{},
	}
	pb.RegisterManagementServiceServer(s, ts.mgmtSvc)
	pb.RegisterDataServiceServer(s, ts.dataSvc)
	go s.Serve(lis) //nolint:errcheck
	t.Cleanup(s.Stop)
	return ts
}

// setGroupCoordMeta configures the management service so that findCoordinator
// resolves ts.addr as the coordinator.
func setGroupCoordMeta(ts *groupConsumerTestServer) {
	ts.mgmtSvc.mu.Lock()
	ts.mgmtSvc.clusterResp = &pb.DescribeClusterResponse{
		Nodes: []*pb.NodeInfo{{NodeId: 1, Address: ts.addr}},
	}
	ts.mgmtSvc.mu.Unlock()
}

// setGroupTopicMeta sets topic metadata so that getPartitionOffset can resolve
// the leader for the given single-partition topic to ts.addr.
func setGroupTopicMeta(ts *groupConsumerTestServer, topic string) {
	ts.mgmtSvc.mu.Lock()
	ts.mgmtSvc.topicResp = &pb.DescribeTopicResponse{
		Topic:      &pb.TopicInfo{Name: topic, PartitionCount: 1},
		Partitions: []*pb.PartitionInfo{{PartitionId: 0, LeaderNodeId: 1}},
	}
	ts.mgmtSvc.mu.Unlock()
}

func newGroupConsumerForTest(t *testing.T, bootstrapAddr, groupID string, policy OffsetResetPolicy) *Consumer {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig{
		Config: Config{
			BootstrapServers: []string{bootstrapAddr},
			RequestTimeout:   2 * time.Second,
		},
		GroupID:         groupID,
		MaxFetchBytes:   1 << 20,
		MaxFetchWaitMs:  100,
		AutoOffsetReset: policy,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func staleGenerationErr() error {
	st, _ := status.New(codes.FailedPrecondition, "stale generation").WithDetails(
		&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_STALE_GENERATION},
	)
	return st.Err()
}

func notGroupMemberErr() error {
	st, _ := status.New(codes.FailedPrecondition, "not group member").WithDetails(
		&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_NOT_GROUP_MEMBER},
	)
	return st.Err()
}

func notLeaderErrWithDetail(leaderAddr string) error {
	st, _ := status.New(codes.FailedPrecondition, "not leader").WithDetails(
		&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_NOT_LEADER},
		&pb.NotLeaderDetail{LeaderAddress: leaderAddr},
	)
	return st.Err()
}

// newGroupConsumerWithHB creates a group Consumer with a fast heartbeat interval.
func newGroupConsumerWithHB(t *testing.T, bootstrapAddr, groupID string, heartbeatMs int64) *Consumer {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig{
		Config: Config{
			BootstrapServers: []string{bootstrapAddr},
			RequestTimeout:   2 * time.Second,
		},
		GroupID:             groupID,
		MaxFetchBytes:       1 << 20,
		MaxFetchWaitMs:      100,
		AutoOffsetReset:     OffsetResetEarliest,
		HeartbeatIntervalMs: heartbeatMs,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ---- group mode consumer tests ----

func TestConsumer_GroupMode_Subscribe_CallsJoinGroup(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m1",
			GenerationId: 7,
			Assignments: []*pb.TopicPartition{
				{Topic: "t", PartitionId: 0},
				{Topic: "t", PartitionId: 1},
			},
		},
	}}
	ts.dataSvc.mu.Unlock()

	c := newGroupConsumerForTest(t, ts.addr, "grp1", OffsetResetEarliest)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if c.memberID != "m1" {
		t.Errorf("memberID = %q, want m1", c.memberID)
	}
	if c.generationID != 7 {
		t.Errorf("generationID = %d, want 7", c.generationID)
	}
	if len(c.soughtPartitions) != 2 {
		t.Errorf("soughtPartitions len = %d, want 2", len(c.soughtPartitions))
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.joinCalls
	ts.dataSvc.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("JoinGroup call count = %d, want 1", len(calls))
	}
	if calls[0].GroupId != "grp1" {
		t.Errorf("JoinGroup GroupId = %q, want grp1", calls[0].GroupId)
	}
}

func TestConsumer_GroupMode_InitFetchOffsets_Committed(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m1",
			GenerationId: 1,
			Assignments:  []*pb.TopicPartition{{Topic: "tp", PartitionId: 0}},
		},
	}}
	ts.dataSvc.fetchOffsResults = []fetchOffsetsResult{{
		offsets: []*pb.PartitionOffset{{Topic: "tp", PartitionId: 0, Offset: 10}},
	}}
	ts.dataSvc.mu.Unlock()

	c := newGroupConsumerForTest(t, ts.addr, "grp1", OffsetResetEarliest)
	if err := c.Subscribe([]string{"tp"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tp := TP{Topic: "tp", PartitionID: 0}
	if c.fetchOffsets[tp] != 10 {
		t.Errorf("fetchOffsets = %d, want 10", c.fetchOffsets[tp])
	}
}

func TestConsumer_GroupMode_InitFetchOffsets_Earliest(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m1",
			GenerationId: 1,
			Assignments:  []*pb.TopicPartition{{Topic: "tp", PartitionId: 0}},
		},
	}}
	// FetchCommittedOffsets returns -1 (offset absent).
	ts.dataSvc.fetchOffsResults = []fetchOffsetsResult{{
		offsets: []*pb.PartitionOffset{{Topic: "tp", PartitionId: 0, Offset: -1}},
	}}
	ts.dataSvc.mu.Unlock()

	c := newGroupConsumerForTest(t, ts.addr, "grp1", OffsetResetEarliest)
	if err := c.Subscribe([]string{"tp"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tp := TP{Topic: "tp", PartitionID: 0}
	if c.fetchOffsets[tp] != 0 {
		t.Errorf("fetchOffsets = %d, want 0", c.fetchOffsets[tp])
	}
}

func TestConsumer_GroupMode_InitFetchOffsets_Latest(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)
	setGroupTopicMeta(ts, "tp") // needed for getPartitionOffset → leaderFor

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m1",
			GenerationId: 1,
			Assignments:  []*pb.TopicPartition{{Topic: "tp", PartitionId: 0}},
		},
	}}
	// FetchCommittedOffsets returns -1 so AutoOffsetReset=LATEST applies.
	ts.dataSvc.fetchOffsResults = []fetchOffsetsResult{{
		offsets: []*pb.PartitionOffset{{Topic: "tp", PartitionId: 0, Offset: -1}},
	}}
	ts.dataSvc.getOffsetResult = 42
	ts.dataSvc.mu.Unlock()

	c := newGroupConsumerForTest(t, ts.addr, "grp1", OffsetResetLatest)
	if err := c.Subscribe([]string{"tp"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tp := TP{Topic: "tp", PartitionID: 0}
	if c.fetchOffsets[tp] != 42 {
		t.Errorf("fetchOffsets = %d, want 42", c.fetchOffsets[tp])
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.getOffsetCalls
	ts.dataSvc.mu.Unlock()
	if calls != 1 {
		t.Errorf("GetOffsets calls = %d, want 1", calls)
	}
}

func TestConsumer_GroupMode_CommitOffsets_Success(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m1",
			GenerationId: 3,
			Assignments:  []*pb.TopicPartition{{Topic: "tp", PartitionId: 0}},
		},
	}}
	ts.dataSvc.mu.Unlock()

	c := newGroupConsumerForTest(t, ts.addr, "grp1", OffsetResetEarliest)
	if err := c.Subscribe([]string{"tp"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	err := c.CommitOffsets(context.Background(), map[TP]int64{
		{Topic: "tp", PartitionID: 0}: 5,
	})
	if err != nil {
		t.Errorf("CommitOffsets returned unexpected error: %v", err)
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.commitCalls
	ts.dataSvc.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("CommitOffset call count = %d, want 1", len(calls))
	}
	if calls[0].GenerationId != 3 {
		t.Errorf("CommitOffset GenerationId = %d, want 3", calls[0].GenerationId)
	}
}

func TestConsumer_GroupMode_CommitOffsets_StaleGeneration(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m1",
			GenerationId: 1,
			Assignments:  []*pb.TopicPartition{{Topic: "tp", PartitionId: 0}},
		},
	}}
	ts.dataSvc.commitResults = []commitOffsetResult{{err: staleGenerationErr()}}
	ts.dataSvc.mu.Unlock()

	c := newGroupConsumerForTest(t, ts.addr, "grp1", OffsetResetEarliest)
	if err := c.Subscribe([]string{"tp"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	err := c.CommitOffsets(context.Background(), map[TP]int64{
		{Topic: "tp", PartitionID: 0}: 5,
	})
	if err != ErrStaleGeneration {
		t.Errorf("CommitOffsets error = %v, want ErrStaleGeneration", err)
	}
}

func TestConsumer_GroupMode_Subscribe_NotLeader_Retry(t *testing.T) {
	// serverA: returns NOT_LEADER on first JoinGroup; DescribeCluster returns its own address.
	serverA := newGroupConsumerTestServer(t)
	serverA.mgmtSvc.mu.Lock()
	serverA.mgmtSvc.clusterResp = &pb.DescribeClusterResponse{
		Nodes: []*pb.NodeInfo{{NodeId: 1, Address: serverA.addr}},
	}
	serverA.mgmtSvc.mu.Unlock()

	// serverB: returns success on JoinGroup; handles FetchCommittedOffsets.
	serverB := newGroupConsumerTestServer(t)
	serverB.dataSvc.mu.Lock()
	serverB.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m-b",
			GenerationId: 2,
			Assignments:  []*pb.TopicPartition{{Topic: "t", PartitionId: 0}},
		},
	}}
	serverB.dataSvc.mu.Unlock()

	// serverA's JoinGroup returns NOT_LEADER pointing at serverB.
	serverA.dataSvc.mu.Lock()
	serverA.dataSvc.joinResults = []joinGroupResult{{
		err: notLeaderErr(serverB.addr),
	}}
	serverA.dataSvc.mu.Unlock()

	c := newGroupConsumerForTest(t, serverA.addr, "grp1", OffsetResetEarliest)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if c.memberID != "m-b" {
		t.Errorf("memberID = %q, want m-b", c.memberID)
	}
	if c.coordAddr != serverB.addr {
		t.Errorf("coordAddr = %q, want %q", c.coordAddr, serverB.addr)
	}

	// Verify JoinGroup was called on both servers.
	serverA.dataSvc.mu.Lock()
	callsA := len(serverA.dataSvc.joinCalls)
	serverA.dataSvc.mu.Unlock()
	serverB.dataSvc.mu.Lock()
	callsB := len(serverB.dataSvc.joinCalls)
	serverB.dataSvc.mu.Unlock()
	if callsA != 1 {
		t.Errorf("serverA JoinGroup calls = %d, want 1", callsA)
	}
	if callsB != 1 {
		t.Errorf("serverB JoinGroup calls = %d, want 1", callsB)
	}
}

func TestConsumer_ManualMode_CommitOffsets_Noop(t *testing.T) {
	ts := newGroupConsumerTestServer(t)

	c, err := NewConsumer(ConsumerConfig{
		Config: Config{
			BootstrapServers: []string{ts.addr},
			RequestTimeout:   2 * time.Second,
		},
		MaxFetchBytes:  1 << 20,
		MaxFetchWaitMs: 100,
		// GroupID is empty → manual mode
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	err = c.CommitOffsets(context.Background(), map[TP]int64{
		{Topic: "t", PartitionID: 0}: 5,
	})
	if err != nil {
		t.Errorf("CommitOffsets (manual mode) returned error: %v", err)
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.commitCalls
	ts.dataSvc.mu.Unlock()
	if len(calls) != 0 {
		t.Errorf("CommitOffset was called %d times, want 0", len(calls))
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

// ---- heartbeat / rebalance tests ----

func TestConsumer_Heartbeat_SentPeriodically(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{
			MemberId:     "m1",
			GenerationId: 1,
			Assignments:  []*pb.TopicPartition{{Topic: "t", PartitionId: 0}},
		},
	}}
	ts.dataSvc.mu.Unlock()

	const intervalMs = 50
	c := newGroupConsumerWithHB(t, ts.addr, "grp1", intervalMs)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for at least 3 heartbeats: 3 × intervalMs + margin.
	time.Sleep(time.Duration(3*intervalMs+intervalMs/2) * time.Millisecond)

	ts.dataSvc.mu.Lock()
	n := len(ts.dataSvc.heartbeatCalls)
	ts.dataSvc.mu.Unlock()

	if n < 3 {
		t.Errorf("heartbeat calls = %d, want >= 3", n)
	}
}

func TestConsumer_Heartbeat_RebalanceRequired_TriggersRejoin(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{
		{resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 1, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}}},
		{resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 2, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}}},
	}
	ts.dataSvc.heartbeatResults = []heartbeatResult{
		{status: pb.HeartbeatStatus_HEARTBEAT_REBALANCE_REQUIRED},
	}
	ts.dataSvc.mu.Unlock()

	const intervalMs = 50
	c := newGroupConsumerWithHB(t, ts.addr, "grp1", intervalMs)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for the first heartbeat to fire and rebalance to complete.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		gen := c.generationID
		c.mu.Unlock()
		if gen == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.mu.Lock()
	gen := c.generationID
	c.mu.Unlock()
	if gen != 2 {
		t.Errorf("generationID = %d, want 2 after rebalance", gen)
	}

	ts.dataSvc.mu.Lock()
	joins := len(ts.dataSvc.joinCalls)
	ts.dataSvc.mu.Unlock()
	if joins < 2 {
		t.Errorf("JoinGroup calls = %d, want >= 2 (initial + rejoin)", joins)
	}
}

func TestConsumer_Heartbeat_NotLeader_RefreshesCoord(t *testing.T) {
	// serverA: coordinator for initial subscribe.
	serverA := newGroupConsumerTestServer(t)
	serverA.mgmtSvc.mu.Lock()
	serverA.mgmtSvc.clusterResp = &pb.DescribeClusterResponse{
		Nodes: []*pb.NodeInfo{{NodeId: 1, Address: serverA.addr}},
	}
	serverA.mgmtSvc.mu.Unlock()

	// serverB: the new coordinator after NOT_LEADER.
	serverB := newGroupConsumerTestServer(t)
	serverB.mgmtSvc.mu.Lock()
	serverB.mgmtSvc.clusterResp = &pb.DescribeClusterResponse{
		Nodes: []*pb.NodeInfo{{NodeId: 2, Address: serverB.addr}},
	}
	serverB.mgmtSvc.mu.Unlock()

	serverA.dataSvc.mu.Lock()
	serverA.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 1, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}},
	}}
	// First heartbeat on serverA returns NOT_LEADER pointing at serverB.
	serverA.dataSvc.heartbeatResults = []heartbeatResult{
		{err: notLeaderErrWithDetail(serverB.addr)},
	}
	serverA.dataSvc.mu.Unlock()

	const intervalMs = 50
	c := newGroupConsumerWithHB(t, serverA.addr, "grp1", intervalMs)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for first heartbeat + NOT_LEADER handling + second heartbeat.
	time.Sleep(time.Duration(3*intervalMs) * time.Millisecond)

	// coordAddr must now be serverB.
	c.mu.Lock()
	addr := c.coordAddr
	c.mu.Unlock()
	if addr != serverB.addr {
		t.Errorf("coordAddr = %q after NOT_LEADER, want %q", addr, serverB.addr)
	}

	// serverB should have received at least one heartbeat.
	serverB.dataSvc.mu.Lock()
	hbB := len(serverB.dataSvc.heartbeatCalls)
	serverB.dataSvc.mu.Unlock()
	if hbB < 1 {
		t.Errorf("heartbeat calls on serverB = %d, want >= 1", hbB)
	}
}

func TestConsumer_Heartbeat_NotGroupMember_Rebalance(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{
		{resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 1, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}}},
		{resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 2, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}}},
	}
	ts.dataSvc.heartbeatResults = []heartbeatResult{
		{err: notGroupMemberErr()},
	}
	ts.dataSvc.mu.Unlock()

	const intervalMs = 50
	c := newGroupConsumerWithHB(t, ts.addr, "grp1", intervalMs)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		gen := c.generationID
		c.mu.Unlock()
		if gen == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.mu.Lock()
	gen := c.generationID
	c.mu.Unlock()
	if gen != 2 {
		t.Errorf("generationID = %d, want 2 after NOT_GROUP_MEMBER rebalance", gen)
	}
}

func TestConsumer_Close_StopsHeartbeat(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 1, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}},
	}}
	ts.dataSvc.mu.Unlock()

	const intervalMs = 50
	c := newGroupConsumerWithHB(t, ts.addr, "grp1", intervalMs)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Let one heartbeat fire.
	time.Sleep(time.Duration(intervalMs+intervalMs/2) * time.Millisecond)

	// Close must complete well within 2× intervalMs after returning.
	closeStart := time.Now()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = closeStart // close is synchronous; goroutine exits after ctx cancel

	// No more heartbeats after Close + 2 intervals.
	time.Sleep(time.Duration(2*intervalMs) * time.Millisecond)
	ts.dataSvc.mu.Lock()
	countAfterClose := len(ts.dataSvc.heartbeatCalls)
	ts.dataSvc.mu.Unlock()

	// Wait a bit more and confirm the count doesn't grow.
	time.Sleep(time.Duration(2*intervalMs) * time.Millisecond)
	ts.dataSvc.mu.Lock()
	countLater := len(ts.dataSvc.heartbeatCalls)
	ts.dataSvc.mu.Unlock()

	if countLater != countAfterClose {
		t.Errorf("heartbeat kept firing after Close: %d -> %d calls", countAfterClose, countLater)
	}
}

func TestConsumer_Close_SendsLeaveGroup(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{{
		resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 1, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}},
	}}
	ts.dataSvc.mu.Unlock()

	c := newGroupConsumerWithHB(t, ts.addr, "grp1", 10000)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ts.dataSvc.mu.Lock()
	calls := ts.dataSvc.leaveGroupCalls
	ts.dataSvc.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("LeaveGroup calls = %d, want 1", len(calls))
	}
	if calls[0].groupID != "grp1" || calls[0].memberID != "m1" {
		t.Errorf("LeaveGroup req = {%q, %q}, want {grp1, m1}", calls[0].groupID, calls[0].memberID)
	}
}

func TestConsumer_Poll_WaitsForRebalance(t *testing.T) {
	ts := newGroupConsumerTestServer(t)
	setGroupCoordMeta(ts)
	setGroupTopicMeta(ts, "t")

	ts.dataSvc.mu.Lock()
	ts.dataSvc.joinResults = []joinGroupResult{
		{resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 1, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}}},
		{resp: &pb.JoinGroupResponse{MemberId: "m1", GenerationId: 2, Assignments: []*pb.TopicPartition{{Topic: "t", PartitionId: 0}}}},
	}
	ts.dataSvc.heartbeatResults = []heartbeatResult{
		{status: pb.HeartbeatStatus_HEARTBEAT_REBALANCE_REQUIRED},
	}
	ts.dataSvc.mu.Unlock()

	const intervalMs = 50
	c := newGroupConsumerWithHB(t, ts.addr, "grp1", intervalMs)
	if err := c.Subscribe([]string{"t"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Manually set the rebalancing flag to simulate a concurrent rebalance.
	c.rebalancing.Store(true)

	// Unblock Poll after a short delay by clearing the flag.
	go func() {
		time.Sleep(60 * time.Millisecond)
		c.rebalancing.Store(false)
	}()

	pollStart := time.Now()
	_, err := c.Poll(context.Background(), 500)
	elapsed := time.Since(pollStart)

	if err != nil {
		t.Fatalf("Poll returned unexpected error: %v", err)
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("Poll returned too quickly (%v), expected to wait for rebalance", elapsed)
	}
}
