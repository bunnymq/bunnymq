package data

import (
	"context"

	coorddata "github.com/bunnymq/bunnymq/internal/coordinator/data"
)

// DataCoordinatorIface is the subset of DataCoordinator used by Server.
// It exists to allow test doubles without importing the full coordinator.
type DataCoordinatorIface interface {
	Produce(ctx context.Context, topic string, partitionID int32, batch []byte, acks coorddata.AcksMode) (int64, error)
	Fetch(ctx context.Context, topic string, partitionID int32, offset int64, maxBytes int, maxWaitMs int64) ([]byte, int64, error)
	GetEarliestOffset(ctx context.Context, topic string, partitionID int32) (int64, error)
	GetLatestOffset(ctx context.Context, topic string, partitionID int32) (int64, error)
	GetOffsetByTimestamp(ctx context.Context, topic string, partitionID int32, timestampMs int64) (int64, error)
}
