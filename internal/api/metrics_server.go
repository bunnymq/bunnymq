package api

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer serves GET /metrics in Prometheus text format on a dedicated HTTP port.
type MetricsServer struct {
	addr     string
	registry *prometheus.Registry
	srv      *http.Server
}

// NewMetricsServer constructs a MetricsServer that will bind to addr when Start is called.
func NewMetricsServer(addr string, registry *prometheus.Registry) *MetricsServer {
	return &MetricsServer{addr: addr, registry: registry}
}

// Start binds the listener and begins serving /metrics. The actual bound address is
// available via Addr() after Start returns without error (useful when addr is ":0").
func (ms *MetricsServer) Start() error {
	ln, err := net.Listen("tcp", ms.addr)
	if err != nil {
		return err
	}
	ms.addr = ln.Addr().String()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(ms.registry, promhttp.HandlerOpts{}))
	ms.srv = &http.Server{
		Addr:         ms.addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go ms.srv.Serve(ln) //nolint:errcheck
	return nil
}

// Addr returns the actual bound address (host:port) after Start succeeds.
func (ms *MetricsServer) Addr() string {
	return ms.addr
}

// Stop gracefully shuts down the HTTP server within the deadline of ctx.
func (ms *MetricsServer) Stop(ctx context.Context) error {
	if ms.srv == nil {
		return nil
	}
	return ms.srv.Shutdown(ctx)
}
