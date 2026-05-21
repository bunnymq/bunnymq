package management

import (
	"context"
	"testing"

	"github.com/bunnymq/bunnymq/internal/coordinator/cluster"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubDataQuery is a minimal DataQueryIface implementation for tests.
type stubDataQuery struct {
	partitionOffsetsFn func(ctx context.Context, shardID uint64) (int64, int64, error)
}

func (s *stubDataQuery) PartitionOffsets(ctx context.Context, shardID uint64) (int64, int64, error) {
	if s.partitionOffsetsFn != nil {
		return s.partitionOffsetsFn(ctx, shardID)
	}
	return -1, -1, nil
}

// stubCoordinator is a minimal ClusterCoordinatorIface implementation for tests.
type stubCoordinator struct {
	createTopicFn          func(ctx context.Context, name string, partitionCount int32, replicationFactor int32, overrides cluster.TopicConfigOverrides) (cluster.TopicInfo, error)
	deleteTopicFn          func(ctx context.Context, name string) error
	listTopicsFn           func(ctx context.Context) ([]cluster.TopicInfo, error)
	describeTopicFn        func(ctx context.Context, name string) (cluster.TopicDescription, error)
	alterPartitionCountFn  func(ctx context.Context, name string, newCount int32) error
	alterRetentionFn       func(ctx context.Context, name string, retentionMs int64, retentionBytes int64) error
	describeClusterFn      func(ctx context.Context) (cluster.ClusterDescription, error)
}

func (s *stubCoordinator) CreateTopic(ctx context.Context, name string, partitionCount int32, replicationFactor int32, overrides cluster.TopicConfigOverrides) (cluster.TopicInfo, error) {
	return s.createTopicFn(ctx, name, partitionCount, replicationFactor, overrides)
}

func (s *stubCoordinator) DeleteTopic(ctx context.Context, name string) error {
	return s.deleteTopicFn(ctx, name)
}

func (s *stubCoordinator) ListTopics(ctx context.Context) ([]cluster.TopicInfo, error) {
	return s.listTopicsFn(ctx)
}

func (s *stubCoordinator) DescribeTopic(ctx context.Context, name string) (cluster.TopicDescription, error) {
	return s.describeTopicFn(ctx, name)
}

func (s *stubCoordinator) AlterTopicPartitionCount(ctx context.Context, name string, newCount int32) error {
	return s.alterPartitionCountFn(ctx, name, newCount)
}

func (s *stubCoordinator) AlterTopicRetention(ctx context.Context, name string, retentionMs int64, retentionBytes int64) error {
	return s.alterRetentionFn(ctx, name, retentionMs, retentionBytes)
}

func (s *stubCoordinator) DescribeCluster(ctx context.Context) (cluster.ClusterDescription, error) {
	return s.describeClusterFn(ctx)
}

func TestManagementServer_CreateTopic_Success(t *testing.T) {
	want := cluster.TopicInfo{
		Name:              "my-topic",
		PartitionCount:    3,
		ReplicationFactor: 2,
		RetentionMs:       604800000,
		RetentionBytes:    -1,
		CreatedAtMs:       1000000,
	}
	srv := NewServer(&stubCoordinator{
		createTopicFn: func(_ context.Context, _ string, _ int32, _ int32, _ cluster.TopicConfigOverrides) (cluster.TopicInfo, error) {
			return want, nil
		},
	}, nil, nil)

	resp, err := srv.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Name:              "my-topic",
		PartitionCount:    3,
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Topic.Name != want.Name {
		t.Errorf("name: got %q, want %q", resp.Topic.Name, want.Name)
	}
	if resp.Topic.PartitionCount != want.PartitionCount {
		t.Errorf("partition_count: got %d, want %d", resp.Topic.PartitionCount, want.PartitionCount)
	}
	if resp.Topic.ReplicationFactor != want.ReplicationFactor {
		t.Errorf("replication_factor: got %d, want %d", resp.Topic.ReplicationFactor, want.ReplicationFactor)
	}
	if resp.Topic.RetentionBytes != want.RetentionBytes {
		t.Errorf("retention_bytes: got %d, want %d", resp.Topic.RetentionBytes, want.RetentionBytes)
	}
}

func TestManagementServer_CreateTopic_AlreadyExists(t *testing.T) {
	srv := NewServer(&stubCoordinator{
		createTopicFn: func(_ context.Context, _ string, _ int32, _ int32, _ cluster.TopicConfigOverrides) (cluster.TopicInfo, error) {
			return cluster.TopicInfo{}, ErrTopicAlreadyExists
		},
	}, nil, nil)

	_, err := srv.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "dup"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.AlreadyExists {
		t.Errorf("code: got %v, want %v", st.Code(), codes.AlreadyExists)
	}
	assertBunnyErrorCode(t, err, pb.BunnyErrorCode_TOPIC_ALREADY_EXISTS)
}

func TestManagementServer_DeleteTopic_NotFound(t *testing.T) {
	srv := NewServer(&stubCoordinator{
		deleteTopicFn: func(_ context.Context, _ string) error {
			return ErrTopicNotFound
		},
	}, nil, nil)

	_, err := srv.DeleteTopic(context.Background(), &pb.DeleteTopicRequest{Name: "missing"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.NotFound {
		t.Errorf("code: got %v, want %v", st.Code(), codes.NotFound)
	}
	assertBunnyErrorCode(t, err, pb.BunnyErrorCode_TOPIC_NOT_FOUND)
}

func TestManagementServer_DescribeTopic_Success(t *testing.T) {
	srv := NewServer(&stubCoordinator{
		describeTopicFn: func(_ context.Context, _ string) (cluster.TopicDescription, error) {
			return cluster.TopicDescription{
				TopicInfo: cluster.TopicInfo{Name: "t", PartitionCount: 3},
				Partitions: []cluster.PartitionInfo{
					{PartitionID: 0, ShardID: 1, LeaderNodeID: 1},
					{PartitionID: 1, ShardID: 2, LeaderNodeID: 2},
					{PartitionID: 2, ShardID: 3, LeaderNodeID: 3},
				},
			}, nil
		},
	}, nil, nil)

	resp, err := srv.DescribeTopic(context.Background(), &pb.DescribeTopicRequest{Name: "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Partitions) != 3 {
		t.Errorf("partitions: got %d, want 3", len(resp.Partitions))
	}
	for i, p := range resp.Partitions {
		if p.PartitionId != int32(i) {
			t.Errorf("partitions[%d].partition_id: got %d, want %d", i, p.PartitionId, i)
		}
	}
}

func TestManagementServer_AlterTopicPartitions_InvalidArg(t *testing.T) {
	srv := NewServer(&stubCoordinator{
		alterPartitionCountFn: func(_ context.Context, _ string, _ int32) error {
			return ErrInvalidArgument
		},
	}, nil, nil)

	_, err := srv.AlterTopicPartitions(context.Background(), &pb.AlterTopicPartitionsRequest{Name: "t", NewPartitionCount: 1})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code: got %v, want %v", st.Code(), codes.InvalidArgument)
	}
	assertBunnyErrorCode(t, err, pb.BunnyErrorCode_INVALID_ARGUMENT)
}

func TestManagementServer_DescribeCluster(t *testing.T) {
	srv := NewServer(&stubCoordinator{
		describeClusterFn: func(_ context.Context) (cluster.ClusterDescription, error) {
			return cluster.ClusterDescription{
				Nodes: []cluster.NodeDescriptor{
					{NodeID: 1, Address: "host1:9092"},
					{NodeID: 2, Address: "host2:9092"},
					{NodeID: 3, Address: "host3:9092"},
				},
			}, nil
		},
	}, nil, nil)

	resp, err := srv.DescribeCluster(context.Background(), &pb.DescribeClusterRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Nodes) != 3 {
		t.Errorf("nodes: got %d, want 3", len(resp.Nodes))
	}
	for i, n := range resp.Nodes {
		if n.NodeId != uint64(i+1) {
			t.Errorf("nodes[%d].node_id: got %d, want %d", i, n.NodeId, i+1)
		}
	}
}

func TestManagementServer_ListPartitions_Offsets(t *testing.T) {
	cc := &stubCoordinator{
		describeTopicFn: func(_ context.Context, _ string) (cluster.TopicDescription, error) {
			return cluster.TopicDescription{
				TopicInfo: cluster.TopicInfo{Name: "t", PartitionCount: 2},
				Partitions: []cluster.PartitionInfo{
					{PartitionID: 0, ShardID: 10, LeaderNodeID: 1},
					{PartitionID: 1, ShardID: 11, LeaderNodeID: 2},
				},
			}, nil
		},
	}
	dq := &stubDataQuery{
		partitionOffsetsFn: func(_ context.Context, shardID uint64) (int64, int64, error) {
			offsets := map[uint64][2]int64{
				10: {0, 42},
				11: {5, 100},
			}
			if o, ok := offsets[shardID]; ok {
				return o[0], o[1], nil
			}
			return -1, -1, nil
		},
	}
	srv := NewServer(cc, dq, nil)

	resp, err := srv.ListPartitions(context.Background(), &pb.ListPartitionsRequest{Topic: "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Partitions) != 2 {
		t.Fatalf("partitions: got %d, want 2", len(resp.Partitions))
	}
	cases := []struct{ earliest, latest int64 }{{0, 42}, {5, 100}}
	for i, p := range resp.Partitions {
		if p.EarliestOffset != cases[i].earliest {
			t.Errorf("partitions[%d].earliest: got %d, want %d", i, p.EarliestOffset, cases[i].earliest)
		}
		if p.LatestOffset != cases[i].latest {
			t.Errorf("partitions[%d].latest: got %d, want %d", i, p.LatestOffset, cases[i].latest)
		}
	}
}

// assertBunnyErrorCode checks that err contains a BunnyErrorDetail with the given code.
func assertBunnyErrorCode(t *testing.T, err error, want pb.BunnyErrorCode) {
	t.Helper()
	st := status.Convert(err)
	for _, d := range st.Details() {
		if detail, ok := d.(*pb.BunnyErrorDetail); ok {
			if detail.Code != want {
				t.Errorf("BunnyErrorCode: got %v, want %v", detail.Code, want)
			}
			return
		}
	}
	t.Errorf("no BunnyErrorDetail found in status details")
}
