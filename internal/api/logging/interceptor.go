package logging

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryInterceptor logs RPC entry at debug and result at info.
func UnaryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		logger.Debug("rpc start", zap.String("method", info.FullMethod))
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.Info("rpc done",
			zap.String("method", info.FullMethod),
			zap.String("code", status.Code(err).String()),
			zap.Duration("latency", time.Since(start)),
		)
		return resp, err
	}
}

// StreamInterceptor logs stream RPC entry at debug and result at info.
func StreamInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		logger.Debug("rpc stream start", zap.String("method", info.FullMethod))
		start := time.Now()
		err := handler(srv, ss)
		logger.Info("rpc stream done",
			zap.String("method", info.FullMethod),
			zap.String("code", status.Code(err).String()),
			zap.Duration("latency", time.Since(start)),
		)
		return err
	}
}
