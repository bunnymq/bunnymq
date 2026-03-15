package data

import (
	"encoding/binary"
	"hash/crc32"

	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxBatchBytes = 4 * 1024 * 1024 // 4 MiB

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// validateBatch checks the four wire-format invariants before the batch is
// forwarded to the DataCoordinator. The checks are ordered so that the cheapest
// structural checks run first and the CRC (more expensive) runs last.
func validateBatch(data []byte) error {
	if len(data) < 38 {
		return invalidFormat("batch too short")
	}
	batchLength := int(binary.BigEndian.Uint32(data[8:12]))
	if batchLength < 38 || batchLength > len(data) {
		return invalidFormat("batch_length field inconsistency")
	}
	if len(data) > maxBatchBytes {
		st, _ := status.New(codes.InvalidArgument, "batch exceeds 4 MiB").
			WithDetails(&pb.BunnyErrorDetail{
				Code:    pb.BunnyErrorCode_BATCH_TOO_LARGE,
				Message: "batch exceeds 4 MiB",
			})
		return st.Err()
	}
	crc := crc32.Checksum(data[38:batchLength], castagnoli)
	if crc != binary.BigEndian.Uint32(data[16:20]) {
		return invalidFormat("CRC-32C mismatch")
	}
	return nil
}

func invalidFormat(msg string) error {
	st, _ := status.New(codes.InvalidArgument, msg).
		WithDetails(&pb.BunnyErrorDetail{
			Code:    pb.BunnyErrorCode_INVALID_MESSAGE_FORMAT,
			Message: msg,
		})
	return st.Err()
}
