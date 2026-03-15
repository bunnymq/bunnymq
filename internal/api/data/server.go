package data

import (
	"context"
	"errors"

	coorddata "github.com/bunnymq/bunnymq/internal/coordinator/data"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements pb.DataServiceServer by delegating to DataCoordinatorIface.
type Server struct {
	pb.UnimplementedDataServiceServer
	dc DataCoordinatorIface
}

// New returns a Server backed by the given coordinator.
func New(dc DataCoordinatorIface) *Server {
	return &Server{dc: dc}
}

func (s *Server) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	if err := validateBatch(req.BatchData); err != nil {
		return nil, err
	}
	acks := coorddata.AcksAll
	if req.Acks == pb.AcksMode_ACKS_ZERO {
		acks = coorddata.AcksZero
	}
	offset, err := s.dc.Produce(ctx, req.Topic, req.PartitionId, req.BatchData, acks)
	if err != nil {
		return nil, mapDataError(err)
	}
	return &pb.ProduceResponse{PartitionId: req.PartitionId, Offset: offset}, nil
}

func (s *Server) GetOffsets(ctx context.Context, req *pb.GetOffsetsRequest) (*pb.GetOffsetsResponse, error) {
	var (
		offset int64
		err    error
	)
	switch req.QueryType {
	case pb.OffsetQueryType_EARLIEST:
		offset, err = s.dc.GetEarliestOffset(ctx, req.Topic, req.PartitionId)
	case pb.OffsetQueryType_LATEST:
		offset, err = s.dc.GetLatestOffset(ctx, req.Topic, req.PartitionId)
	case pb.OffsetQueryType_BY_TIMESTAMP:
		offset, err = s.dc.GetOffsetByTimestamp(ctx, req.Topic, req.PartitionId, req.TimestampMs)
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown query type")
	}
	if err != nil {
		return nil, mapDataError(err)
	}
	return &pb.GetOffsetsResponse{Offset: offset}, nil
}

// Consumer-group RPCs are stubbed for M3; full implementations land in M4.

func (s *Server) JoinGroup(_ context.Context, _ *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented in M3")
}

func (s *Server) Heartbeat(_ context.Context, _ *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented in M3")
}

func (s *Server) LeaveGroup(_ context.Context, _ *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented in M3")
}

func (s *Server) CommitOffset(_ context.Context, _ *pb.CommitOffsetRequest) (*pb.CommitOffsetResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented in M3")
}

func (s *Server) FetchCommittedOffsets(_ context.Context, _ *pb.FetchCommittedOffsetsRequest) (*pb.FetchCommittedOffsetsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented in M3")
}

func mapDataError(err error) error {
	var notLeader *coorddata.NotLeaderError
	if errors.As(err, &notLeader) {
		st, _ := status.New(codes.FailedPrecondition, err.Error()).
			WithDetails(
				&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_NOT_LEADER, Message: err.Error()},
				&pb.NotLeaderDetail{LeaderNodeId: notLeader.LeaderNodeID, LeaderAddress: notLeader.LeaderAddress},
			)
		return st.Err()
	}
	if errors.Is(err, ErrOffsetNotFound) {
		st, _ := status.New(codes.NotFound, err.Error()).
			WithDetails(&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_OFFSET_NOT_FOUND, Message: err.Error()})
		return st.Err()
	}
	return status.Error(codes.Internal, "internal server error")
}
