package api

import (
	"crypto/tls"

	apidata "github.com/bunnymq/bunnymq/internal/api/data"
	"github.com/bunnymq/bunnymq/internal/api/auth"
	"github.com/bunnymq/bunnymq/internal/api/logging"
	apimgmt "github.com/bunnymq/bunnymq/internal/api/management"
	"github.com/bunnymq/bunnymq/internal/coordinator/cluster"
	"github.com/bunnymq/bunnymq/internal/coordinator/data"
	proto "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ServerConfig holds shared configuration for both gRPC servers.
type ServerConfig struct {
	Addr       string
	AuthTokens []string
	TLSConfig  *tls.Config
	Metrics    *ServerMetrics
}

// NewManagementServer builds a grpc.Server for the Management API.
func NewManagementServer(config ServerConfig, cc *cluster.ClusterCoordinator, dc *data.DataCoordinator, logger *zap.Logger) *grpc.Server {
	srv := grpc.NewServer(serverOptions(config, logger)...)
	proto.RegisterManagementServiceServer(srv, apimgmt.NewServer(cc, dc, logger))
	return srv
}

// NewDataServer builds a grpc.Server for the Data API.
func NewDataServer(config ServerConfig, dc *data.DataCoordinator, gc apidata.GroupCoordinatorIface, isMetadataLeader func() (bool, string), logger *zap.Logger) *grpc.Server {
	srv := grpc.NewServer(serverOptions(config, logger)...)
	proto.RegisterDataServiceServer(srv, apidata.NewServer(dc, gc, isMetadataLeader, logger))
	return srv
}

func serverOptions(config ServerConfig, logger *zap.Logger) []grpc.ServerOption {
	unary := []grpc.UnaryServerInterceptor{
		auth.UnaryInterceptor(config.AuthTokens, logger),
		logging.UnaryInterceptor(logger),
	}
	if config.Metrics != nil {
		unary = append(unary, ServerMetricsInterceptor(config.Metrics))
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(
			auth.StreamInterceptor(config.AuthTokens, logger),
			logging.StreamInterceptor(logger),
		),
	}
	if config.TLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(config.TLSConfig)))
	}
	return opts
}
