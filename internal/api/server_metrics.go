package api

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var grpcBuckets = []float64{0.0005, 0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0}

// ServerMetrics holds Prometheus metrics for the gRPC API layer.
type ServerMetrics struct {
	GRPCRequestsTotal   *prometheus.CounterVec
	GRPCRequestDuration *prometheus.HistogramVec
	BuildInfo           *prometheus.GaugeVec
	UptimeSeconds       prometheus.Gauge
}

// NewServerMetrics creates and registers all server-wide metrics with reg.
func NewServerMetrics(reg prometheus.Registerer) *ServerMetrics {
	m := &ServerMetrics{
		GRPCRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bunnymq_grpc_requests_total",
			Help: "RPC requests by method and gRPC status code.",
		}, []string{"method", "code"}),
		GRPCRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_grpc_request_duration_seconds",
			Help:    "End-to-end handler latency.",
			Buckets: grpcBuckets,
		}, []string{"method"}),
		BuildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_build_info",
			Help: "Always 1; carries build metadata as labels.",
		}, []string{"version", "go_version", "node_id", "commit"}),
		UptimeSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bunnymq_uptime_seconds",
			Help: "Seconds since the process started.",
		}),
	}
	reg.MustRegister(
		m.GRPCRequestsTotal,
		m.GRPCRequestDuration,
		m.BuildInfo,
		m.UptimeSeconds,
	)
	return m
}

// RecordBuildInfo sets the build_info gauge to 1.0 with the provided labels.
func (m *ServerMetrics) RecordBuildInfo(version, goVersion, nodeID, commit string) {
	m.BuildInfo.WithLabelValues(version, goVersion, nodeID, commit).Set(1.0)
}

// StartUptimeTicker starts a goroutine that updates UptimeSeconds every 10 s until ctx is cancelled.
func (m *ServerMetrics) StartUptimeTicker(ctx context.Context, startTime time.Time) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.UptimeSeconds.Set(time.Since(startTime).Seconds())
			}
		}
	}()
}

// ServerMetricsInterceptor returns a unary interceptor that records per-RPC counters and latency.
// Must be placed after the logging interceptor in the chain: Auth → Logging → Metrics → Handler.
func ServerMetricsInterceptor(metrics *ServerMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err).String()
		metrics.GRPCRequestsTotal.WithLabelValues(info.FullMethod, code).Inc()
		metrics.GRPCRequestDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		return resp, err
	}
}
