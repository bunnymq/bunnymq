package api_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bunnymq/bunnymq/internal/api"
)

func TestMetricsServer_ServeMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())

	ms := api.NewMetricsServer("127.0.0.1:0", reg)
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ms.Stop(ctx) //nolint:errcheck
	})

	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", ms.Addr()))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "go_goroutines") {
		t.Errorf("response body does not contain go_goroutines:\n%s", body)
	}
}

func TestMetricsServer_BuildInfo(t *testing.T) {
	reg := prometheus.NewRegistry()
	sm := api.NewServerMetrics(reg)

	ms := api.NewMetricsServer("127.0.0.1:0", reg)
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ms.Stop(ctx) //nolint:errcheck
	})

	sm.RecordBuildInfo("test", "go1.21", "1", "abc123")

	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", ms.Addr()))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), `bunnymq_build_info{`) {
		t.Errorf("response body does not contain bunnymq_build_info:\n%s", body)
	}
	if !strings.Contains(string(body), `version="test"`) {
		t.Errorf("response body does not contain version=\"test\":\n%s", body)
	}
}

func TestServerMetricsInterceptor_RecordsRequest(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := api.NewServerMetrics(reg)
	interceptor := api.ServerMetricsInterceptor(metrics)

	handler := func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

	_, err := interceptor(context.Background(), nil, info, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}

	count := testutil.ToFloat64(metrics.GRPCRequestsTotal.WithLabelValues("/test.Service/Method", codes.OK.String()))
	if count != 1 {
		t.Errorf("expected grpc_requests_total{code=OK} = 1, got %v", count)
	}

	if n := testutil.CollectAndCount(metrics.GRPCRequestDuration); n == 0 {
		t.Error("expected at least one observation in grpc_request_duration_seconds")
	}
}

func TestServerMetricsInterceptor_RecordsError(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := api.NewServerMetrics(reg)
	interceptor := api.ServerMetricsInterceptor(metrics)

	handler := func(_ context.Context, _ any) (any, error) {
		return nil, status.Error(codes.NotFound, "not found")
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

	_, _ = interceptor(context.Background(), nil, info, handler)

	count := testutil.ToFloat64(metrics.GRPCRequestsTotal.WithLabelValues("/test.Service/Method", codes.NotFound.String()))
	if count != 1 {
		t.Errorf("expected grpc_requests_total{code=NotFound} = 1, got %v", count)
	}
}

func TestPprofServer_PublicBindRejected(t *testing.T) {
	_, err := api.NewPprofServer("0.0.0.0:6060")
	if err == nil {
		t.Error("expected error for 0.0.0.0 bind, got nil")
	}
}

func TestPprofServer_LoopbackOK(t *testing.T) {
	ps, err := api.NewPprofServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewPprofServer: %v", err)
	}
	if err := ps.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ps.Stop(ctx) //nolint:errcheck
	})

	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", ps.Addr()))
	if err != nil {
		t.Fatalf("GET /debug/pprof/: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}
