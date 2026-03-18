package logging

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDataServer_LogsHandlerError(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	interceptor := UnaryInterceptor(logger)
	info := &grpc.UnaryServerInfo{FullMethod: "/bunny.v1.DataService/Produce"}
	grpcErr := status.Error(codes.Internal, "something went wrong")

	handler := func(_ context.Context, _ any) (any, error) {
		return nil, grpcErr
	}

	_, err := interceptor(context.Background(), nil, info, handler)
	if !errors.Is(err, grpcErr) {
		t.Fatalf("expected grpcErr, got %v", err)
	}

	var errorEntries []observer.LoggedEntry
	for _, e := range logs.All() {
		if e.Level == zapcore.ErrorLevel && e.Message == "gRPC handler returned error" {
			errorEntries = append(errorEntries, e)
		}
	}
	if len(errorEntries) == 0 {
		t.Fatal("expected 'gRPC handler returned error' error log entry")
	}
	fields := errorEntries[0].ContextMap()
	if fields["method"] == nil {
		t.Error("expected 'method' field in error log entry")
	}
	if fields["error"] == nil {
		t.Error("expected 'error' field in error log entry")
	}
}
