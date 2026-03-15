package auth

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var noopInfo = &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

func ctxWithToken(token string) context.Context {
	md := metadata.Pairs("bunnymq-auth-token", token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestAuthInterceptor_ValidToken(t *testing.T) {
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}
	interceptor := UnaryInterceptor([]string{"secret"}, zap.NewNop())
	_, err := interceptor(ctxWithToken("secret"), nil, noopInfo, handler)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}

func TestAuthInterceptor_InvalidToken(t *testing.T) {
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}
	interceptor := UnaryInterceptor([]string{"secret"}, zap.NewNop())
	_, err := interceptor(ctxWithToken("wrong"), nil, noopInfo, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	if called {
		t.Fatal("handler must not be called on invalid token")
	}
}

func TestAuthInterceptor_MissingToken(t *testing.T) {
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}
	interceptor := UnaryInterceptor([]string{"secret"}, zap.NewNop())
	// metadata present but no auth key
	md := metadata.Pairs("other-key", "value")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err := interceptor(ctx, nil, noopInfo, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	if called {
		t.Fatal("handler must not be called when token is missing")
	}
}

func TestAuthInterceptor_Plaintext(t *testing.T) {
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}
	interceptor := UnaryInterceptor([]string{}, zap.NewNop())
	// no metadata at all — PLAINTEXT mode must still pass
	_, err := interceptor(context.Background(), nil, noopInfo, handler)
	if err != nil {
		t.Fatalf("expected nil error in PLAINTEXT mode, got %v", err)
	}
	if !called {
		t.Fatal("handler must be called in PLAINTEXT mode")
	}
}

func TestInterceptorChain_Order(t *testing.T) {
	var order []string

	record := func(name string) grpc.UnaryServerInterceptor {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			order = append(order, name)
			return handler(ctx, req)
		}
	}

	handler := func(ctx context.Context, req any) (any, error) {
		order = append(order, "handler")
		return nil, nil
	}

	// Wrap auth interceptor in a recorder to observe its position in the chain.
	rawAuth := UnaryInterceptor([]string{}, zap.NewNop())
	recordAuth := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		order = append(order, "auth")
		return rawAuth(ctx, req, info, handler)
	}

	chained := chainUnary(recordAuth, record("logging"))
	_, err := chained(context.Background(), nil, noopInfo, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"auth", "logging", "handler"}
	if len(order) != len(want) {
		t.Fatalf("expected %d interceptor calls, got %d: %v", len(want), len(order), order)
	}
	for i, got := range order {
		if got != want[i] {
			t.Fatalf("interceptor order mismatch at index %d: want %q, got %q", i, want[i], got)
		}
	}
}

// chainUnary mimics grpc.ChainUnaryInterceptor for test purposes.
func chainUnary(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var chain func(i int) grpc.UnaryHandler
		chain = func(i int) grpc.UnaryHandler {
			if i == len(interceptors) {
				return handler
			}
			return func(ctx context.Context, req any) (any, error) {
				return interceptors[i](ctx, req, info, chain(i+1))
			}
		}
		return chain(0)(ctx, req)
	}
}
