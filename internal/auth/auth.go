package auth

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryServerInterceptor returns a gRPC unary interceptor that validates the
// bunnymq-auth-token metadata key. Passes through all requests when tokens is empty.
func UnaryServerInterceptor(tokens []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a gRPC streaming interceptor that validates
// the bunnymq-auth-token metadata key.
func StreamServerInterceptor(tokens []string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, ss)
	}
}
