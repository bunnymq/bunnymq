package data

import (
	proto "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"github.com/bunnymq/bunnymq/internal/coordinator/data"
)

// Server is the gRPC server stub for the Data API service.
// Handler implementations are added in T-037/T-038.
type Server struct {
	proto.UnimplementedDataServiceServer
	dc *data.DataCoordinator
}

func New(dc *data.DataCoordinator) *Server {
	return &Server{dc: dc}
}
