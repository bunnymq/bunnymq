package auth

import (
	"context"
	"slices"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func validateToken(ctx context.Context, validTokens []string) error {
	if len(validTokens) == 0 {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get("bunnymq-auth-token")
	if len(tokens) == 0 {
		return status.Error(codes.Unauthenticated, "missing bunnymq-auth-token")
	}
	if slices.Contains(validTokens, tokens[0]) {
		return nil
	}
	return status.Error(codes.Unauthenticated, "invalid token")
}

// UnaryInterceptor returns a gRPC unary interceptor that validates bunnymq-auth-token.
// If validTokens is empty, all requests are accepted (PLAINTEXT mode).
func UnaryInterceptor(validTokens []string, logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := validateToken(ctx, validTokens); err != nil {
			logger.Warn("auth failed", zap.String("method", info.FullMethod), zap.Error(err))
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream interceptor that validates bunnymq-auth-token.
// If validTokens is empty, all requests are accepted (PLAINTEXT mode).
func StreamInterceptor(validTokens []string, logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validateToken(ss.Context(), validTokens); err != nil {
			logger.Warn("auth failed", zap.String("method", info.FullMethod), zap.Error(err))
			return err
		}
		return handler(srv, ss)
	}
}
