package client

import (
	"math"
	"time"

	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/status"
)

// RetryPolicy mirrors pkg/client.RetryPolicy and is used by internal helpers.
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
}

// extractBunnyError extracts BunnyErrorDetail and NotLeaderDetail from a gRPC
// status error. Returns nil for either field if it is absent.
func extractBunnyError(err error) (*pb.BunnyErrorDetail, *pb.NotLeaderDetail) {
	st, ok := status.FromError(err)
	if !ok {
		return nil, nil
	}
	var bunnyErr *pb.BunnyErrorDetail
	var notLeader *pb.NotLeaderDetail
	for _, detail := range st.Details() {
		switch d := detail.(type) {
		case *pb.BunnyErrorDetail:
			bunnyErr = d
		case *pb.NotLeaderDetail:
			notLeader = d
		}
	}
	return bunnyErr, notLeader
}

// backoffDuration returns the capped exponential backoff for the given attempt
// (0-indexed: attempt=0 returns InitialBackoff).
func backoffDuration(attempt int, policy RetryPolicy) time.Duration {
	if policy.InitialBackoff <= 0 {
		return 0
	}
	d := float64(policy.InitialBackoff) * math.Pow(policy.BackoffFactor, float64(attempt))
	if d > float64(policy.MaxBackoff) {
		return policy.MaxBackoff
	}
	return time.Duration(d)
}

// isRetryable returns true for error codes that warrant automatic retry.
func isRetryable(code pb.BunnyErrorCode) bool {
	switch code {
	case pb.BunnyErrorCode_NOT_LEADER,
		pb.BunnyErrorCode_UNAVAILABLE,
		pb.BunnyErrorCode_TIMEOUT:
		return true
	default:
		return false
	}
}
