package client

import (
	"testing"
	"time"

	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryable(t *testing.T) {
	retryable := []pb.BunnyErrorCode{
		pb.BunnyErrorCode_NOT_LEADER,
		pb.BunnyErrorCode_UNAVAILABLE,
		pb.BunnyErrorCode_TIMEOUT,
	}
	for _, code := range retryable {
		if !isRetryable(code) {
			t.Errorf("isRetryable(%v) = false, want true", code)
		}
	}
	nonRetryable := []pb.BunnyErrorCode{
		pb.BunnyErrorCode_INVALID_ARGUMENT,
		pb.BunnyErrorCode_UNAUTHENTICATED,
		pb.BunnyErrorCode_TOPIC_NOT_FOUND,
	}
	for _, code := range nonRetryable {
		if isRetryable(code) {
			t.Errorf("isRetryable(%v) = true, want false", code)
		}
	}
}

func TestBackoffDuration_Caps(t *testing.T) {
	policy := RetryPolicy{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		BackoffFactor:  2.0,
	}
	for attempt := range 10 {
		d := backoffDuration(attempt, policy)
		if d > policy.MaxBackoff {
			t.Errorf("attempt %d: duration %v exceeds MaxBackoff %v", attempt, d, policy.MaxBackoff)
		}
	}
}

func TestExtractBunnyError_NotLeader(t *testing.T) {
	bunnyDetail := &pb.BunnyErrorDetail{
		Code:    pb.BunnyErrorCode_NOT_LEADER,
		Message: "not the leader",
	}
	notLeaderDetail := &pb.NotLeaderDetail{
		LeaderNodeId:  42,
		LeaderAddress: "newleader:9092",
	}

	st, err := status.New(codes.FailedPrecondition, "not leader").
		WithDetails(bunnyDetail, notLeaderDetail)
	if err != nil {
		t.Fatalf("status.WithDetails: %v", err)
	}

	gotBunny, gotNotLeader := extractBunnyError(st.Err())

	if gotBunny == nil {
		t.Fatal("expected BunnyErrorDetail, got nil")
	}
	if gotBunny.Code != pb.BunnyErrorCode_NOT_LEADER {
		t.Errorf("BunnyErrorDetail.Code = %v, want NOT_LEADER", gotBunny.Code)
	}

	if gotNotLeader == nil {
		t.Fatal("expected NotLeaderDetail, got nil")
	}
	if gotNotLeader.LeaderAddress != "newleader:9092" {
		t.Errorf("NotLeaderDetail.LeaderAddress = %q, want %q", gotNotLeader.LeaderAddress, "newleader:9092")
	}
	if gotNotLeader.LeaderNodeId != 42 {
		t.Errorf("NotLeaderDetail.LeaderNodeId = %d, want 42", gotNotLeader.LeaderNodeId)
	}
}
