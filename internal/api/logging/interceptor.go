package logging

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryInterceptor logs RPC entry at debug and errors at error level.
func UnaryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		reqID := requestIDFromCtx(ctx)
		logger.Debug("RPC request received",
			zap.String("method", info.FullMethod),
			zap.String("request_id", reqID),
		)
		resp, err := handler(ctx, req)
		if err != nil {
			st := status.Convert(err)
			if st.Code() == codes.Unimplemented {
				logger.Warn("unrecognized RPC method called", zap.String("method", info.FullMethod))
			} else {
				logger.Error("gRPC handler returned error",
					zap.String("method", info.FullMethod),
					zap.String("request_id", reqID),
					zap.Error(err),
				)
			}
		}
		return resp, err
	}
}

// StreamInterceptor logs stream RPC entry at debug and errors at error level.
func StreamInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		reqID := requestIDFromCtx(ss.Context())
		logger.Debug("RPC request received",
			zap.String("method", info.FullMethod),
			zap.String("request_id", reqID),
		)
		err := handler(srv, ss)
		if err != nil {
			st := status.Convert(err)
			if st.Code() == codes.Unimplemented {
				logger.Warn("unrecognized RPC method called", zap.String("method", info.FullMethod))
			} else {
				logger.Error("gRPC handler returned error",
					zap.String("method", info.FullMethod),
					zap.String("request_id", reqID),
					zap.Error(err),
				)
			}
		}
		return err
	}
}

func requestIDFromCtx(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("x-request-id")
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}
