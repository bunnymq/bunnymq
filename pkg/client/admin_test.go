package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeAdminMgmtSvc is a configurable ManagementService stub for AdminClient tests.
type fakeAdminMgmtSvc struct {
	pb.UnimplementedManagementServiceServer

	mu sync.Mutex

	createTopicResp *pb.CreateTopicResponse
	createTopicErr  error

	deleteTopicErr error

	clusterResp *pb.DescribeClusterResponse
	clusterErr  error

	// unavailableCount is the number of UNAVAILABLE errors to return from
	// CreateTopic before returning createTopicResp (used in retry test).
	unavailableCount int
	unavailableSeen  int
}

func (f *fakeAdminMgmtSvc) CreateTopic(_ context.Context, _ *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailableSeen < f.unavailableCount {
		f.unavailableSeen++
		return nil, unavailableErr()
	}
	if f.createTopicErr != nil {
		return nil, f.createTopicErr
	}
	return f.createTopicResp, nil
}

func (f *fakeAdminMgmtSvc) DeleteTopic(_ context.Context, _ *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteTopicErr != nil {
		return nil, f.deleteTopicErr
	}
	return &pb.DeleteTopicResponse{}, nil
}

func (f *fakeAdminMgmtSvc) DescribeCluster(_ context.Context, _ *pb.DescribeClusterRequest) (*pb.DescribeClusterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.clusterErr != nil {
		return nil, f.clusterErr
	}
	return f.clusterResp, nil
}

// alreadyExistsErr creates a gRPC error carrying BunnyErrorDetail(TOPIC_ALREADY_EXISTS).
func alreadyExistsErr() error {
	st, _ := status.New(codes.AlreadyExists, "topic already exists").WithDetails(
		&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_TOPIC_ALREADY_EXISTS},
	)
	return st.Err()
}

// topicNotFoundErr creates a gRPC error carrying BunnyErrorDetail(TOPIC_NOT_FOUND).
func topicNotFoundErr() error {
	st, _ := status.New(codes.NotFound, "topic not found").WithDetails(
		&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_TOPIC_NOT_FOUND},
	)
	return st.Err()
}

// newAdminServer starts a gRPC server with only the fakeAdminMgmtSvc registered
// and returns the server address and the service stub.
func newAdminServer(t *testing.T, svc *fakeAdminMgmtSvc) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterManagementServiceServer(s, svc)
	go s.Serve(lis) //nolint:errcheck
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// newAdminClientAt creates an AdminClient targeting addr with fast defaults for tests.
func newAdminClientAt(t *testing.T, addr string, retryPolicy RetryPolicy) *AdminClient {
	t.Helper()
	a, err := NewAdminClient(Config{
		BootstrapServers: []string{addr},
		RequestTimeout:   2 * time.Second,
		RetryPolicy:      retryPolicy,
	})
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}
	t.Cleanup(func() { a.Close() }) //nolint:errcheck
	return a
}

// ---- tests ----

func TestAdminClient_CreateTopic_Success(t *testing.T) {
	svc := &fakeAdminMgmtSvc{
		createTopicResp: &pb.CreateTopicResponse{
			Topic: &pb.TopicInfo{
				Name:              "my-topic",
				PartitionCount:    3,
				ReplicationFactor: 2,
				RetentionMs:       604800000,
				RetentionBytes:    -1,
				CreatedAtMs:       1000,
			},
		},
	}
	addr := newAdminServer(t, svc)
	a := newAdminClientAt(t, addr, RetryPolicy{MaxRetries: 3})

	info, err := a.CreateTopic(context.Background(), CreateTopicRequest{
		Name:              "my-topic",
		PartitionCount:    3,
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if info.Name != "my-topic" {
		t.Errorf("Name = %q, want %q", info.Name, "my-topic")
	}
	if info.PartitionCount != 3 {
		t.Errorf("PartitionCount = %d, want 3", info.PartitionCount)
	}
	if info.ReplicationFactor != 2 {
		t.Errorf("ReplicationFactor = %d, want 2", info.ReplicationFactor)
	}
	if info.RetentionBytes != -1 {
		t.Errorf("RetentionBytes = %d, want -1", info.RetentionBytes)
	}
}

func TestAdminClient_CreateTopic_AlreadyExists(t *testing.T) {
	svc := &fakeAdminMgmtSvc{createTopicErr: alreadyExistsErr()}
	addr := newAdminServer(t, svc)
	a := newAdminClientAt(t, addr, RetryPolicy{MaxRetries: 3})

	_, err := a.CreateTopic(context.Background(), CreateTopicRequest{Name: "dup"})
	if err != ErrTopicAlreadyExists {
		t.Errorf("err = %v, want ErrTopicAlreadyExists", err)
	}
}

func TestAdminClient_DeleteTopic_NotFound(t *testing.T) {
	svc := &fakeAdminMgmtSvc{deleteTopicErr: topicNotFoundErr()}
	addr := newAdminServer(t, svc)
	a := newAdminClientAt(t, addr, RetryPolicy{MaxRetries: 3})

	err := a.DeleteTopic(context.Background(), "missing")
	if err != ErrTopicNotFound {
		t.Errorf("err = %v, want ErrTopicNotFound", err)
	}
}

func TestAdminClient_DescribeCluster(t *testing.T) {
	svc := &fakeAdminMgmtSvc{
		clusterResp: &pb.DescribeClusterResponse{
			Nodes: []*pb.NodeInfo{
				{NodeId: 1, Address: "host1:9092"},
				{NodeId: 2, Address: "host2:9092"},
				{NodeId: 3, Address: "host3:9092"},
			},
		},
	}
	addr := newAdminServer(t, svc)
	a := newAdminClientAt(t, addr, RetryPolicy{MaxRetries: 3})

	desc, err := a.DescribeCluster(context.Background())
	if err != nil {
		t.Fatalf("DescribeCluster: %v", err)
	}
	if len(desc.Nodes) != 3 {
		t.Fatalf("Nodes count = %d, want 3", len(desc.Nodes))
	}
	for i, want := range []struct {
		id   uint64
		addr string
	}{
		{1, "host1:9092"},
		{2, "host2:9092"},
		{3, "host3:9092"},
	} {
		if desc.Nodes[i].NodeID != want.id {
			t.Errorf("Nodes[%d].NodeID = %d, want %d", i, desc.Nodes[i].NodeID, want.id)
		}
		if desc.Nodes[i].Address != want.addr {
			t.Errorf("Nodes[%d].Address = %q, want %q", i, desc.Nodes[i].Address, want.addr)
		}
	}
}

func TestAdminClient_Unavailable_Retry(t *testing.T) {
	// Server returns UNAVAILABLE twice, then succeeds on the third call.
	svc := &fakeAdminMgmtSvc{
		unavailableCount: 2,
		createTopicResp: &pb.CreateTopicResponse{
			Topic: &pb.TopicInfo{Name: "t", PartitionCount: 1},
		},
	}
	addr := newAdminServer(t, svc)
	a := newAdminClientAt(t, addr, RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
	})

	info, err := a.CreateTopic(context.Background(), CreateTopicRequest{Name: "t", PartitionCount: 1})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if info.Name != "t" {
		t.Errorf("Name = %q, want %q", info.Name, "t")
	}

	svc.mu.Lock()
	seen := svc.unavailableSeen
	svc.mu.Unlock()
	if seen != 2 {
		t.Errorf("unavailableSeen = %d, want 2", seen)
	}
}

// Suppress "imported and not used" for packages used only by test helpers.
var _ = grpc.NewServer
var _ = insecure.NewCredentials
var _ = status.New
