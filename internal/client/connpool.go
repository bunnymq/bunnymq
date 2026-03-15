package client

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// errPoolClosed is returned by Get after Close has been called.
var errPoolClosed = errors.New("connpool: pool is closed")

// ConnPool maintains one gRPC connection per distinct broker address.
// Connections are established lazily on the first Get call.
type ConnPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

// NewConnPool creates a ConnPool with keepalive defaults applied before any
// caller-supplied opts so callers can override them if needed.
func NewConnPool(opts ...grpc.DialOption) *ConnPool {
	defaultOpts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30e9, // 30 s
			Timeout: 10e9, // 10 s
		}),
	}
	return &ConnPool{
		conns: make(map[string]*grpc.ClientConn),
		opts:  append(defaultOpts, opts...),
	}
}

// Get returns an existing connection for addr or lazily creates one.
func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()
	if ok {
		return conn, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conns == nil {
		return nil, errPoolClosed
	}
	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, p.opts...)
	if err != nil {
		return nil, fmt.Errorf("connpool: dial %s: %w", addr, err)
	}
	p.conns[addr] = conn
	return conn, nil
}

// Close closes all connections and clears the pool.
// Subsequent Get calls return an error.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	p.conns = nil
	return errors.Join(errs...)
}
