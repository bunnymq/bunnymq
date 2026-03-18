package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
)

// PprofServer serves the standard net/http/pprof endpoints on a dedicated HTTP port.
// It must be bound to a loopback or private address; binding to 0.0.0.0 is rejected.
type PprofServer struct {
	addr string
	srv  *http.Server
}

// NewPprofServer constructs a PprofServer. Returns an error if addr starts with "0.0.0.0"
// to prevent accidental public exposure of profiling data.
func NewPprofServer(addr string) (*PprofServer, error) {
	if strings.HasPrefix(addr, "0.0.0.0") {
		return nil, errors.New("pprof server must not bind to public address 0.0.0.0")
	}
	return &PprofServer{addr: addr}, nil
}

// Start registers pprof handlers and begins serving. The actual bound address is
// available via Addr() after Start returns without error.
func (ps *PprofServer) Start() error {
	ln, err := net.Listen("tcp", ps.addr)
	if err != nil {
		return err
	}
	ps.addr = ln.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ps.srv = &http.Server{
		Addr:    ps.addr,
		Handler: mux,
	}
	go ps.srv.Serve(ln) //nolint:errcheck
	return nil
}

// Addr returns the actual bound address (host:port) after Start succeeds.
func (ps *PprofServer) Addr() string {
	return ps.addr
}

// Stop gracefully shuts down the pprof HTTP server within the deadline of ctx.
func (ps *PprofServer) Stop(ctx context.Context) error {
	if ps.srv == nil {
		return nil
	}
	return ps.srv.Shutdown(ctx)
}
