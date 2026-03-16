package data

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	coorddata "github.com/bunnymq/bunnymq/internal/coordinator/data"
	coordgroup "github.com/bunnymq/bunnymq/internal/coordinator/group"
	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/storage"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubDataCoordinator is a minimal DataCoordinatorIface for unit tests.
type stubDataCoordinator struct {
	produceFn              func(ctx context.Context, topic string, partitionID int32, batch []byte, acks coorddata.AcksMode) (int64, error)
	fetchFn                func(ctx context.Context, topic string, partitionID int32, offset int64, maxBytes int, maxWaitMs int64) ([]byte, int64, error)
	getEarliestOffsetFn    func(ctx context.Context, topic string, partitionID int32) (int64, error)
	getLatestOffsetFn      func(ctx context.Context, topic string, partitionID int32) (int64, error)
	getOffsetByTimestampFn func(ctx context.Context, topic string, partitionID int32, timestampMs int64) (int64, error)
}

func (s *stubDataCoordinator) Produce(ctx context.Context, topic string, partitionID int32, batch []byte, acks coorddata.AcksMode) (int64, error) {
	return s.produceFn(ctx, topic, partitionID, batch, acks)
}

func (s *stubDataCoordinator) Fetch(ctx context.Context, topic string, partitionID int32, offset int64, maxBytes int, maxWaitMs int64) ([]byte, int64, error) {
	return s.fetchFn(ctx, topic, partitionID, offset, maxBytes, maxWaitMs)
}

func (s *stubDataCoordinator) GetEarliestOffset(ctx context.Context, topic string, partitionID int32) (int64, error) {
	return s.getEarliestOffsetFn(ctx, topic, partitionID)
}

func (s *stubDataCoordinator) GetLatestOffset(ctx context.Context, topic string, partitionID int32) (int64, error) {
	return s.getLatestOffsetFn(ctx, topic, partitionID)
}

func (s *stubDataCoordinator) GetOffsetByTimestamp(ctx context.Context, topic string, partitionID int32, timestampMs int64) (int64, error) {
	return s.getOffsetByTimestampFn(ctx, topic, partitionID, timestampMs)
}

// validBatch returns a valid encoded batch using EncodeBatch from the storage package.
func validBatch(t *testing.T) []byte {
	t.Helper()
	data, err := storage.EncodeBatch([]storage.Record{
		{TimestampMs: 1000, Value: []byte("hello")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	return data
}

func TestDataServer_Produce_ValidBatch(t *testing.T) {
	batch := validBatch(t)
	srv := New(&stubDataCoordinator{
		produceFn: func(_ context.Context, _ string, _ int32, _ []byte, _ coorddata.AcksMode) (int64, error) {
			return 42, nil
		},
	}, nil, nil)

	resp, err := srv.Produce(context.Background(), &pb.ProduceRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		Acks:        pb.AcksMode_ACKS_ALL,
		BatchData:   batch,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Offset != 42 {
		t.Errorf("offset: got %d, want 42", resp.Offset)
	}
}

func TestDataServer_Produce_CRCMismatch(t *testing.T) {
	batch := validBatch(t)
	// Corrupt one byte of the CRC field (bytes [16:20]).
	batch[17] ^= 0xFF
	srv := New(&stubDataCoordinator{}, nil, nil)

	_, err := srv.Produce(context.Background(), &pb.ProduceRequest{
		Topic:     "test-topic",
		BatchData: batch,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.InvalidArgument, pb.BunnyErrorCode_INVALID_MESSAGE_FORMAT)
}

func TestDataServer_Produce_TooLarge(t *testing.T) {
	// Build a batch that exceeds 4 MiB. We need it to pass the header and
	// batch_length checks, so use a valid batch and pad to exceed the limit.
	base := validBatch(t)

	// Adjust batch_length to encompass the padded size, then pad.
	padded := make([]byte, maxBatchBytes+1)
	copy(padded, base)
	// Set batch_length field to len(padded) so the length check passes.
	binary.BigEndian.PutUint32(padded[8:12], uint32(len(padded)))

	srv := New(&stubDataCoordinator{}, nil, nil)
	_, err := srv.Produce(context.Background(), &pb.ProduceRequest{
		Topic:     "test-topic",
		BatchData: padded,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.InvalidArgument, pb.BunnyErrorCode_BATCH_TOO_LARGE)
}

func TestDataServer_Produce_NotLeader(t *testing.T) {
	batch := validBatch(t)
	srv := New(&stubDataCoordinator{
		produceFn: func(_ context.Context, _ string, _ int32, _ []byte, _ coorddata.AcksMode) (int64, error) {
			return -1, &coorddata.NotLeaderError{LeaderNodeID: 2, LeaderAddress: "broker2:9092"}
		},
	}, nil, nil)

	_, err := srv.Produce(context.Background(), &pb.ProduceRequest{
		Topic:     "test-topic",
		BatchData: batch,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code: got %v, want %v", st.Code(), codes.FailedPrecondition)
	}
	var foundBunny, foundLeader bool
	for _, d := range st.Details() {
		switch detail := d.(type) {
		case *pb.BunnyErrorDetail:
			if detail.Code != pb.BunnyErrorCode_NOT_LEADER {
				t.Errorf("BunnyErrorCode: got %v, want NOT_LEADER", detail.Code)
			}
			foundBunny = true
		case *pb.NotLeaderDetail:
			if detail.LeaderNodeId != 2 {
				t.Errorf("leader_node_id: got %d, want 2", detail.LeaderNodeId)
			}
			if detail.LeaderAddress != "broker2:9092" {
				t.Errorf("leader_address: got %q, want %q", detail.LeaderAddress, "broker2:9092")
			}
			foundLeader = true
		}
	}
	if !foundBunny {
		t.Error("BunnyErrorDetail missing from status details")
	}
	if !foundLeader {
		t.Error("NotLeaderDetail missing from status details")
	}
}

func TestDataServer_GetOffsets_Earliest(t *testing.T) {
	srv := New(&stubDataCoordinator{
		getEarliestOffsetFn: func(_ context.Context, _ string, _ int32) (int64, error) {
			return 0, nil
		},
	}, nil, nil)

	resp, err := srv.GetOffsets(context.Background(), &pb.GetOffsetsRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		QueryType:   pb.OffsetQueryType_EARLIEST,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Offset != 0 {
		t.Errorf("offset: got %d, want 0", resp.Offset)
	}
}

func TestDataServer_GetOffsets_ByTimestamp_NotFound(t *testing.T) {
	srv := New(&stubDataCoordinator{
		getOffsetByTimestampFn: func(_ context.Context, _ string, _ int32, _ int64) (int64, error) {
			return -1, ErrOffsetNotFound
		},
	}, nil, nil)

	_, err := srv.GetOffsets(context.Background(), &pb.GetOffsetsRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		QueryType:   pb.OffsetQueryType_BY_TIMESTAMP,
		TimestampMs: 9999999999,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.NotFound, pb.BunnyErrorCode_OFFSET_NOT_FOUND)
}

func TestValidateBatch_ShortHeader(t *testing.T) {
	err := validateBatch(bytes.Repeat([]byte{0}, 37))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.InvalidArgument, pb.BunnyErrorCode_INVALID_MESSAGE_FORMAT)
}

func TestValidateBatch_BadBatchLength(t *testing.T) {
	// Build a 50-byte buffer but set batch_length to 100 (> len).
	data := make([]byte, 50)
	binary.BigEndian.PutUint32(data[8:12], 100)

	err := validateBatch(data)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.InvalidArgument, pb.BunnyErrorCode_INVALID_MESSAGE_FORMAT)
}

func TestDataServer_Fetch_ImmediateData(t *testing.T) {
	records := validBatch(t)
	srv := New(&stubDataCoordinator{
		fetchFn: func(_ context.Context, _ string, _ int32, _ int64, _ int, _ int64) ([]byte, int64, error) {
			return records, 10, nil
		},
	}, nil, nil)

	resp, err := srv.Fetch(context.Background(), &pb.FetchRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		Offset:      5,
		MaxBytes:    1024,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) == 0 {
		t.Error("expected non-empty records")
	}
	if resp.NextOffset != 10 {
		t.Errorf("next_offset: got %d, want 10", resp.NextOffset)
	}
}

func TestDataServer_Fetch_EmptyNoWait(t *testing.T) {
	srv := New(&stubDataCoordinator{
		fetchFn: func(_ context.Context, _ string, _ int32, offset int64, _ int, maxWaitMs int64) ([]byte, int64, error) {
			return nil, offset, nil
		},
	}, nil, nil)

	resp, err := srv.Fetch(context.Background(), &pb.FetchRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		Offset:      7,
		MaxWaitMs:   0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Records != nil {
		t.Error("expected nil records on empty no-wait response")
	}
	if resp.NextOffset != 7 {
		t.Errorf("next_offset: got %d, want 7 (unchanged)", resp.NextOffset)
	}
}

func TestDataServer_Fetch_LongPollReturnsData(t *testing.T) {
	records := validBatch(t)
	srv := New(&stubDataCoordinator{
		fetchFn: func(ctx context.Context, _ string, _ int32, _ int64, _ int, _ int64) ([]byte, int64, error) {
			select {
			case <-time.After(10 * time.Millisecond):
				return records, 20, nil
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		},
	}, nil, nil)

	resp, err := srv.Fetch(context.Background(), &pb.FetchRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		Offset:      15,
		MaxWaitMs:   200,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) == 0 {
		t.Error("expected non-empty records after long-poll")
	}
	if resp.NextOffset != 20 {
		t.Errorf("next_offset: got %d, want 20", resp.NextOffset)
	}
}

func TestDataServer_Fetch_OffsetOutOfRange(t *testing.T) {
	srv := New(&stubDataCoordinator{
		fetchFn: func(_ context.Context, _ string, _ int32, _ int64, _ int, _ int64) ([]byte, int64, error) {
			return nil, 0, ErrOffsetOutOfRange
		},
	}, nil, nil)

	_, err := srv.Fetch(context.Background(), &pb.FetchRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		Offset:      999,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.OutOfRange, pb.BunnyErrorCode_OFFSET_OUT_OF_RANGE)
}

func TestDataServer_Fetch_NotLeader(t *testing.T) {
	srv := New(&stubDataCoordinator{
		fetchFn: func(_ context.Context, _ string, _ int32, _ int64, _ int, _ int64) ([]byte, int64, error) {
			return nil, 0, &coorddata.NotLeaderError{LeaderNodeID: 3, LeaderAddress: "broker3:9092"}
		},
	}, nil, nil)

	_, err := srv.Fetch(context.Background(), &pb.FetchRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		Offset:      0,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code: got %v, want %v", st.Code(), codes.FailedPrecondition)
	}
	var foundBunny, foundLeader bool
	for _, d := range st.Details() {
		switch detail := d.(type) {
		case *pb.BunnyErrorDetail:
			if detail.Code != pb.BunnyErrorCode_NOT_LEADER {
				t.Errorf("BunnyErrorCode: got %v, want NOT_LEADER", detail.Code)
			}
			foundBunny = true
		case *pb.NotLeaderDetail:
			if detail.LeaderNodeId != 3 {
				t.Errorf("leader_node_id: got %d, want 3", detail.LeaderNodeId)
			}
			if detail.LeaderAddress != "broker3:9092" {
				t.Errorf("leader_address: got %q, want %q", detail.LeaderAddress, "broker3:9092")
			}
			foundLeader = true
		}
	}
	if !foundBunny {
		t.Error("BunnyErrorDetail missing from status details")
	}
	if !foundLeader {
		t.Error("NotLeaderDetail missing from status details")
	}
}

func TestDataServer_Fetch_CtxCancelled(t *testing.T) {
	srv := New(&stubDataCoordinator{
		fetchFn: func(ctx context.Context, _ string, _ int32, _ int64, _ int, _ int64) ([]byte, int64, error) {
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(5 * time.Second):
				return nil, 0, nil
			}
		},
	}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := srv.Fetch(ctx, &pb.FetchRequest{
		Topic:       "test-topic",
		PartitionId: 0,
		Offset:      0,
		MaxWaitMs:   5000,
	})
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.Canceled {
		t.Errorf("code: got %v, want %v", st.Code(), codes.Canceled)
	}
}

// stubGroupCoordinator is a minimal GroupCoordinatorIface for unit tests.
type stubGroupCoordinator struct {
	joinGroupFn             func(ctx context.Context, req coordgroup.JoinGroupRequest) (coordgroup.JoinGroupResponse, error)
	leaveGroupFn            func(ctx context.Context, req coordgroup.LeaveGroupRequest) error
	heartbeatFn             func(ctx context.Context, groupID, memberID string, generationID int32) (bool, error)
	commitOffsetFn          func(ctx context.Context, groupID, memberID string, generationID int32, offsets map[metadata.TopicPartition]int64) error
	fetchCommittedOffsetsFn func(ctx context.Context, groupID string, partitions []metadata.TopicPartition) (map[metadata.TopicPartition]int64, error)
}

func (s *stubGroupCoordinator) JoinGroup(ctx context.Context, req coordgroup.JoinGroupRequest) (coordgroup.JoinGroupResponse, error) {
	return s.joinGroupFn(ctx, req)
}

func (s *stubGroupCoordinator) LeaveGroup(ctx context.Context, req coordgroup.LeaveGroupRequest) error {
	return s.leaveGroupFn(ctx, req)
}

func (s *stubGroupCoordinator) Heartbeat(ctx context.Context, groupID, memberID string, generationID int32) (bool, error) {
	return s.heartbeatFn(ctx, groupID, memberID, generationID)
}

func (s *stubGroupCoordinator) CommitOffset(ctx context.Context, groupID, memberID string, generationID int32, offsets map[metadata.TopicPartition]int64) error {
	return s.commitOffsetFn(ctx, groupID, memberID, generationID, offsets)
}

func (s *stubGroupCoordinator) FetchCommittedOffsets(ctx context.Context, groupID string, partitions []metadata.TopicPartition) (map[metadata.TopicPartition]int64, error) {
	return s.fetchCommittedOffsetsFn(ctx, groupID, partitions)
}

func TestDataServer_JoinGroup_NotLeader(t *testing.T) {
	srv := New(nil, nil, func() (bool, string) { return false, "leader:9092" })
	_, err := srv.JoinGroup(context.Background(), &pb.JoinGroupRequest{
		GroupId:          "g1",
		SubscribedTopics: []string{"t1"},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code: got %v, want %v", st.Code(), codes.FailedPrecondition)
	}
	for _, d := range st.Details() {
		if detail, ok := d.(*pb.BunnyErrorDetail); ok {
			if detail.Code != pb.BunnyErrorCode_NOT_LEADER {
				t.Errorf("BunnyErrorCode: got %v, want NOT_LEADER", detail.Code)
			}
			return
		}
	}
	t.Error("BunnyErrorDetail NOT_LEADER not found in status details")
}

func TestDataServer_JoinGroup_Success(t *testing.T) {
	gc := &stubGroupCoordinator{
		joinGroupFn: func(_ context.Context, req coordgroup.JoinGroupRequest) (coordgroup.JoinGroupResponse, error) {
			return coordgroup.JoinGroupResponse{
				MemberID:     "m1",
				GenerationID: 3,
				Assignments: []metadata.TopicPartition{
					{Topic: "t1", PartitionID: 0},
				},
			}, nil
		},
	}
	srv := New(nil, gc, func() (bool, string) { return true, "" })
	resp, err := srv.JoinGroup(context.Background(), &pb.JoinGroupRequest{
		GroupId:          "g1",
		SubscribedTopics: []string{"t1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MemberId != "m1" {
		t.Errorf("member_id: got %q, want %q", resp.MemberId, "m1")
	}
	if resp.GenerationId != 3 {
		t.Errorf("generation_id: got %d, want 3", resp.GenerationId)
	}
	if len(resp.Assignments) != 1 || resp.Assignments[0].Topic != "t1" || resp.Assignments[0].PartitionId != 0 {
		t.Errorf("assignments mismatch: %v", resp.Assignments)
	}
}

func TestDataServer_Heartbeat_RebalanceRequired(t *testing.T) {
	gc := &stubGroupCoordinator{
		heartbeatFn: func(_ context.Context, _, _ string, _ int32) (bool, error) {
			return true, nil
		},
	}
	srv := New(nil, gc, func() (bool, string) { return true, "" })
	resp, err := srv.Heartbeat(context.Background(), &pb.HeartbeatRequest{
		GroupId:      "g1",
		MemberId:     "m1",
		GenerationId: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != pb.HeartbeatStatus_HEARTBEAT_REBALANCE_REQUIRED {
		t.Errorf("status: got %v, want HEARTBEAT_REBALANCE_REQUIRED", resp.Status)
	}
}

func TestDataServer_CommitOffset_StaleGeneration(t *testing.T) {
	gc := &stubGroupCoordinator{
		commitOffsetFn: func(_ context.Context, _, _ string, _ int32, _ map[metadata.TopicPartition]int64) error {
			return ErrStaleGeneration
		},
	}
	srv := New(nil, gc, func() (bool, string) { return true, "" })
	_, err := srv.CommitOffset(context.Background(), &pb.CommitOffsetRequest{
		GroupId:      "g1",
		MemberId:     "m1",
		GenerationId: 1,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.FailedPrecondition, pb.BunnyErrorCode_STALE_GENERATION)
}

func TestDataServer_FetchCommittedOffsets_MissingPartition(t *testing.T) {
	gc := &stubGroupCoordinator{
		fetchCommittedOffsetsFn: func(_ context.Context, _ string, partitions []metadata.TopicPartition) (map[metadata.TopicPartition]int64, error) {
			result := make(map[metadata.TopicPartition]int64, len(partitions))
			for _, tp := range partitions {
				result[tp] = -1
			}
			return result, nil
		},
	}
	srv := New(nil, gc, func() (bool, string) { return true, "" })
	resp, err := srv.FetchCommittedOffsets(context.Background(), &pb.FetchCommittedOffsetsRequest{
		GroupId: "g1",
		Partitions: []*pb.TopicPartition{
			{Topic: "t1", PartitionId: 0},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Offsets) != 1 {
		t.Fatalf("expected 1 offset entry, got %d", len(resp.Offsets))
	}
	if resp.Offsets[0].Offset != -1 {
		t.Errorf("offset: got %d, want -1", resp.Offsets[0].Offset)
	}
}

func TestDataServer_LeaveGroup_NotMember(t *testing.T) {
	gc := &stubGroupCoordinator{
		leaveGroupFn: func(_ context.Context, _ coordgroup.LeaveGroupRequest) error {
			return ErrNotGroupMember
		},
	}
	srv := New(nil, gc, func() (bool, string) { return true, "" })
	_, err := srv.LeaveGroup(context.Background(), &pb.LeaveGroupRequest{
		GroupId:  "g1",
		MemberId: "m1",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertBunnyCode(t, err, codes.NotFound, pb.BunnyErrorCode_NOT_GROUP_MEMBER)
}

// assertBunnyCode verifies that err has the given gRPC status code and BunnyErrorCode.
func assertBunnyCode(t *testing.T, err error, wantCode codes.Code, wantBunny pb.BunnyErrorCode) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != wantCode {
		t.Errorf("grpc code: got %v, want %v", st.Code(), wantCode)
	}
	for _, d := range st.Details() {
		if detail, ok := d.(*pb.BunnyErrorDetail); ok {
			if detail.Code != wantBunny {
				t.Errorf("BunnyErrorCode: got %v, want %v", detail.Code, wantBunny)
			}
			return
		}
	}
	t.Errorf("no BunnyErrorDetail found in status details")
}
