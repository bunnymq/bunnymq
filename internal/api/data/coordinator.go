package data

import (
	"context"

	coorddata "github.com/bunnymq/bunnymq/internal/coordinator/data"
	coordgroup "github.com/bunnymq/bunnymq/internal/coordinator/group"
	"github.com/bunnymq/bunnymq/internal/metadata"
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

// GroupCoordinatorIface is the subset of GroupCoordinator used by Server.
type GroupCoordinatorIface interface {
	JoinGroup(ctx context.Context, req coordgroup.JoinGroupRequest) (coordgroup.JoinGroupResponse, error)
	LeaveGroup(ctx context.Context, req coordgroup.LeaveGroupRequest) error
	Heartbeat(ctx context.Context, groupID, memberID string, generationID int32) (rebalanceRequired bool, err error)
	CommitOffset(ctx context.Context, groupID, memberID string, generationID int32, offsets map[metadata.TopicPartition]int64) error
	FetchCommittedOffsets(ctx context.Context, groupID string, partitions []metadata.TopicPartition) (map[metadata.TopicPartition]int64, error)
}
