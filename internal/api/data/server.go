package data

import (
	"context"
	"errors"

	coorddata "github.com/bunnymq/bunnymq/internal/coordinator/data"
	coordgroup "github.com/bunnymq/bunnymq/internal/coordinator/group"
	"github.com/bunnymq/bunnymq/internal/metadata"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultFetchMaxBytes        = 1 * 1024 * 1024 // 1 MiB
	defaultSessionTimeoutMs     = 30000            // 30 s — used when proto does not carry session_timeout_ms
	defaultHeartbeatIntervalMs  = 3000             // 3 s
)

// Server implements pb.DataServiceServer by delegating to DataCoordinatorIface
// and GroupCoordinatorIface.
type Server struct {
	pb.UnimplementedDataServiceServer
	dc               DataCoordinatorIface
	groupCoord       GroupCoordinatorIface
	isMetadataLeader func() (bool, string)
}

// New returns a Server backed by the given coordinators.
// gc and isMetadataLeader may be nil when consumer group RPCs are not used.
func New(dc DataCoordinatorIface, gc GroupCoordinatorIface, isMetadataLeader func() (bool, string)) *Server {
	return &Server{dc: dc, groupCoord: gc, isMetadataLeader: isMetadataLeader}
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

func (s *Server) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	if req.Offset < 0 {
		return nil, status.Error(codes.InvalidArgument, "offset must be >= 0")
	}
	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = defaultFetchMaxBytes
	}
	records, nextOffset, err := s.dc.Fetch(ctx, req.Topic, req.PartitionId, req.Offset, maxBytes, req.MaxWaitMs)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, mapDataError(err)
	}
	if records == nil {
		return &pb.FetchResponse{Records: nil, NextOffset: req.Offset}, nil
	}
	return &pb.FetchResponse{Records: records, NextOffset: nextOffset}, nil
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

func (s *Server) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	if isLeader, leaderAddr := s.isMetadataLeader(); !isLeader {
		return nil, notLeaderStatus(leaderAddr)
	}
	sessionTimeoutMs := req.GetSessionTimeoutMs()
	if sessionTimeoutMs == 0 {
		sessionTimeoutMs = defaultSessionTimeoutMs
	}
	heartbeatIntervalMs := req.GetHeartbeatIntervalMs()
	if heartbeatIntervalMs == 0 {
		heartbeatIntervalMs = defaultHeartbeatIntervalMs
	}
	resp, err := s.groupCoord.JoinGroup(ctx, coordgroup.JoinGroupRequest{
		GroupID:             req.GetGroupId(),
		MemberID:            req.GetMemberId(),
		Topics:              req.GetSubscribedTopics(),
		SessionTimeoutMs:    sessionTimeoutMs,
		HeartbeatIntervalMs: heartbeatIntervalMs,
	})
	if err != nil {
		return nil, mapGroupError(err)
	}
	assignments := make([]*pb.TopicPartition, len(resp.Assignments))
	for i, tp := range resp.Assignments {
		assignments[i] = &pb.TopicPartition{Topic: tp.Topic, PartitionId: tp.PartitionID}
	}
	return &pb.JoinGroupResponse{
		MemberId:     resp.MemberID,
		GenerationId: resp.GenerationID,
		Assignments:  assignments,
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if isLeader, leaderAddr := s.isMetadataLeader(); !isLeader {
		return nil, notLeaderStatus(leaderAddr)
	}
	rebalanceRequired, err := s.groupCoord.Heartbeat(ctx, req.GetGroupId(), req.GetMemberId(), req.GetGenerationId())
	if err != nil {
		return nil, mapGroupError(err)
	}
	hbStatus := pb.HeartbeatStatus_HEARTBEAT_OK
	if rebalanceRequired {
		hbStatus = pb.HeartbeatStatus_HEARTBEAT_REBALANCE_REQUIRED
	}
	return &pb.HeartbeatResponse{Status: hbStatus}, nil
}

func (s *Server) LeaveGroup(ctx context.Context, req *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
	if isLeader, leaderAddr := s.isMetadataLeader(); !isLeader {
		return nil, notLeaderStatus(leaderAddr)
	}
	if err := s.groupCoord.LeaveGroup(ctx, coordgroup.LeaveGroupRequest{
		GroupID:  req.GetGroupId(),
		MemberID: req.GetMemberId(),
	}); err != nil {
		return nil, mapGroupError(err)
	}
	return &pb.LeaveGroupResponse{}, nil
}

func (s *Server) CommitOffset(ctx context.Context, req *pb.CommitOffsetRequest) (*pb.CommitOffsetResponse, error) {
	if isLeader, leaderAddr := s.isMetadataLeader(); !isLeader {
		return nil, notLeaderStatus(leaderAddr)
	}
	offsets := make(map[metadata.TopicPartition]int64, len(req.GetOffsets()))
	for _, po := range req.GetOffsets() {
		offsets[metadata.TopicPartition{Topic: po.GetTopic(), PartitionID: po.GetPartitionId()}] = po.GetOffset()
	}
	if err := s.groupCoord.CommitOffset(ctx, req.GetGroupId(), req.GetMemberId(), req.GetGenerationId(), offsets); err != nil {
		return nil, mapGroupError(err)
	}
	return &pb.CommitOffsetResponse{}, nil
}

func (s *Server) FetchCommittedOffsets(ctx context.Context, req *pb.FetchCommittedOffsetsRequest) (*pb.FetchCommittedOffsetsResponse, error) {
	if isLeader, leaderAddr := s.isMetadataLeader(); !isLeader {
		return nil, notLeaderStatus(leaderAddr)
	}
	partitions := make([]metadata.TopicPartition, len(req.GetPartitions()))
	for i, tp := range req.GetPartitions() {
		partitions[i] = metadata.TopicPartition{Topic: tp.GetTopic(), PartitionID: tp.GetPartitionId()}
	}
	result, err := s.groupCoord.FetchCommittedOffsets(ctx, req.GetGroupId(), partitions)
	if err != nil {
		return nil, mapGroupError(err)
	}
	offsets := make([]*pb.PartitionOffset, 0, len(result))
	for tp, offset := range result {
		offsets = append(offsets, &pb.PartitionOffset{
			Topic:       tp.Topic,
			PartitionId: tp.PartitionID,
			Offset:      offset,
		})
	}
	return &pb.FetchCommittedOffsetsResponse{Offsets: offsets}, nil
}

func notLeaderStatus(leaderAddr string) error {
	st, _ := status.New(codes.FailedPrecondition, "not the group coordinator").
		WithDetails(&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_NOT_LEADER, Message: "not the group coordinator"},
			&pb.NotLeaderDetail{LeaderAddress: leaderAddr})
	return st.Err()
}

func mapGroupError(err error) error {
	if errors.Is(err, ErrNotGroupMember) {
		st, _ := status.New(codes.NotFound, err.Error()).
			WithDetails(&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_NOT_GROUP_MEMBER, Message: err.Error()})
		return st.Err()
	}
	if errors.Is(err, ErrStaleGeneration) {
		st, _ := status.New(codes.FailedPrecondition, err.Error()).
			WithDetails(&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_STALE_GENERATION, Message: err.Error()})
		return st.Err()
	}
	if errors.Is(err, coordgroup.ErrInvalidArgument) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, "internal server error")
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
	if errors.Is(err, ErrOffsetOutOfRange) {
		st, _ := status.New(codes.OutOfRange, err.Error()).
			WithDetails(&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_OFFSET_OUT_OF_RANGE, Message: err.Error()})
		return st.Err()
	}
	if errors.Is(err, ErrOffsetNotFound) {
		st, _ := status.New(codes.NotFound, err.Error()).
			WithDetails(&pb.BunnyErrorDetail{Code: pb.BunnyErrorCode_OFFSET_NOT_FOUND, Message: err.Error()})
		return st.Err()
	}
	return status.Error(codes.Internal, "internal server error")
}
