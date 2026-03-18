package management

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/coordinator/cluster"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ManagementServer implements pb.ManagementServiceServer by delegating to ClusterCoordinatorIface.
type ManagementServer struct {
	pb.UnimplementedManagementServiceServer
	cc     ClusterCoordinatorIface
	logger *zap.Logger
}

// NewServer returns a ManagementServer backed by the given coordinator.
func NewServer(cc ClusterCoordinatorIface, logger *zap.Logger) *ManagementServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ManagementServer{cc: cc, logger: logger}
}

func (s *ManagementServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	overrides := cluster.TopicConfigOverrides{}
	if req.RetentionMs != 0 {
		v := req.RetentionMs
		overrides.RetentionMs = &v
	}
	if req.RetentionBytes != 0 {
		v := req.RetentionBytes
		overrides.RetentionBytes = &v
	}
	info, err := s.cc.CreateTopic(ctx, req.Name, req.PartitionCount, req.ReplicationFactor, overrides)
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &pb.CreateTopicResponse{Topic: protoTopicInfo(info)}, nil
}

func (s *ManagementServer) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if err := s.cc.DeleteTopic(ctx, req.Name); err != nil {
		return nil, mapCoordError(err)
	}
	return &pb.DeleteTopicResponse{}, nil
}

func (s *ManagementServer) ListTopics(ctx context.Context, _ *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	topics, err := s.cc.ListTopics(ctx)
	if err != nil {
		return nil, mapCoordError(err)
	}
	resp := &pb.ListTopicsResponse{Topics: make([]*pb.TopicInfo, len(topics))}
	for i, t := range topics {
		resp.Topics[i] = protoTopicInfo(t)
	}
	return resp, nil
}

func (s *ManagementServer) DescribeTopic(ctx context.Context, req *pb.DescribeTopicRequest) (*pb.DescribeTopicResponse, error) {
	desc, err := s.cc.DescribeTopic(ctx, req.Name)
	if err != nil {
		return nil, mapCoordError(err)
	}
	resp := &pb.DescribeTopicResponse{
		Topic:      protoTopicInfo(desc.TopicInfo),
		Partitions: make([]*pb.PartitionInfo, len(desc.Partitions)),
	}
	for i, p := range desc.Partitions {
		resp.Partitions[i] = protoPartitionInfo(p)
	}
	return resp, nil
}

func (s *ManagementServer) AlterTopicPartitions(ctx context.Context, req *pb.AlterTopicPartitionsRequest) (*pb.AlterTopicPartitionsResponse, error) {
	if err := s.cc.AlterTopicPartitionCount(ctx, req.Name, req.NewPartitionCount); err != nil {
		return nil, mapCoordError(err)
	}
	return &pb.AlterTopicPartitionsResponse{}, nil
}

func (s *ManagementServer) AlterTopicRetention(ctx context.Context, req *pb.AlterTopicRetentionRequest) (*pb.AlterTopicRetentionResponse, error) {
	if err := s.cc.AlterTopicRetention(ctx, req.Name, req.RetentionMs, req.RetentionBytes); err != nil {
		return nil, mapCoordError(err)
	}
	return &pb.AlterTopicRetentionResponse{}, nil
}

func (s *ManagementServer) DescribeCluster(ctx context.Context, _ *pb.DescribeClusterRequest) (*pb.DescribeClusterResponse, error) {
	desc, err := s.cc.DescribeCluster(ctx)
	if err != nil {
		return nil, mapCoordError(err)
	}
	resp := &pb.DescribeClusterResponse{Nodes: make([]*pb.NodeInfo, len(desc.Nodes))}
	for i, n := range desc.Nodes {
		resp.Nodes[i] = &pb.NodeInfo{NodeId: n.NodeID, Address: n.Address}
	}
	return resp, nil
}

func (s *ManagementServer) ListPartitions(ctx context.Context, req *pb.ListPartitionsRequest) (*pb.ListPartitionsResponse, error) {
	desc, err := s.cc.DescribeTopic(ctx, req.Topic)
	if err != nil {
		return nil, mapCoordError(err)
	}
	partitions := make([]*pb.PartitionInfoWithOffsets, len(desc.Partitions))
	for i, p := range desc.Partitions {
		partitions[i] = &pb.PartitionInfoWithOffsets{
			Info:            protoPartitionInfo(p),
			EarliestOffset:  0,
			LatestOffset:    0,
		}
	}
	return &pb.ListPartitionsResponse{Partitions: partitions}, nil
}

// mapCoordError converts coordinator sentinel errors to gRPC status errors with BunnyErrorDetail.
func mapCoordError(err error) error {
	var code codes.Code
	var bunnyCode pb.BunnyErrorCode
	var msg string

	switch {
	case errors.Is(err, ErrTopicAlreadyExists):
		code = codes.AlreadyExists
		bunnyCode = pb.BunnyErrorCode_TOPIC_ALREADY_EXISTS
		msg = err.Error()
	case errors.Is(err, ErrTopicNotFound):
		code = codes.NotFound
		bunnyCode = pb.BunnyErrorCode_TOPIC_NOT_FOUND
		msg = err.Error()
	case errors.Is(err, ErrInvalidArgument):
		code = codes.InvalidArgument
		bunnyCode = pb.BunnyErrorCode_INVALID_ARGUMENT
		msg = err.Error()
	case errors.Is(err, ErrUnavailable):
		code = codes.Unavailable
		bunnyCode = pb.BunnyErrorCode_UNAVAILABLE
		msg = err.Error()
	default:
		code = codes.Internal
		bunnyCode = pb.BunnyErrorCode_UNKNOWN
		msg = "internal server error"
	}

	st, detailErr := status.New(code, msg).WithDetails(&pb.BunnyErrorDetail{
		Code:    bunnyCode,
		Message: msg,
	})
	if detailErr != nil {
		return status.Error(code, msg)
	}
	return st.Err()
}

func protoTopicInfo(t cluster.TopicInfo) *pb.TopicInfo {
	return &pb.TopicInfo{
		Name:              t.Name,
		PartitionCount:    t.PartitionCount,
		ReplicationFactor: t.ReplicationFactor,
		RetentionMs:       t.RetentionMs,
		RetentionBytes:    t.RetentionBytes,
		CreatedAtMs:       t.CreatedAtMs,
	}
}

func protoPartitionInfo(p cluster.PartitionInfo) *pb.PartitionInfo {
	return &pb.PartitionInfo{
		PartitionId:     p.PartitionID,
		ShardId:         p.ShardID,
		LeaderNodeId:    p.LeaderNodeID,
		LeaderEpoch:     p.LeaderEpoch,
		ReplicaNodeIds:  p.ReplicaNodeIDs,
	}
}
