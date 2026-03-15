package management

import (
	proto "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"github.com/bunnymq/bunnymq/internal/coordinator/cluster"
)

// Server is the gRPC server stub for the Management API service.
// Handler implementations are added in T-036.
type Server struct {
	proto.UnimplementedManagementServiceServer
	cc *cluster.ClusterCoordinator
}

func New(cc *cluster.ClusterCoordinator) *Server {
	return &Server{cc: cc}
}
