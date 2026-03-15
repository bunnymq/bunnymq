package data

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	coorddata "github.com/bunnymq/bunnymq/internal/coordinator/data"
	"github.com/bunnymq/bunnymq/internal/storage"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubDataCoordinator is a minimal DataCoordinatorIface for unit tests.
type stubDataCoordinator struct {
	produceFn              func(ctx context.Context, topic string, partitionID int32, batch []byte, acks coorddata.AcksMode) (int64, error)
	getEarliestOffsetFn    func(ctx context.Context, topic string, partitionID int32) (int64, error)
	getLatestOffsetFn      func(ctx context.Context, topic string, partitionID int32) (int64, error)
	getOffsetByTimestampFn func(ctx context.Context, topic string, partitionID int32, timestampMs int64) (int64, error)
}

func (s *stubDataCoordinator) Produce(ctx context.Context, topic string, partitionID int32, batch []byte, acks coorddata.AcksMode) (int64, error) {
	return s.produceFn(ctx, topic, partitionID, batch, acks)
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
	})

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
	srv := New(&stubDataCoordinator{})

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

	srv := New(&stubDataCoordinator{})
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
	})

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
	})

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
	})

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
