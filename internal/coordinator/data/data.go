package data

import (
	"context"
	"errors"
	"fmt"
)

// AcksMode controls produce acknowledgement semantics.
type AcksMode int8

const (
	AcksAll  AcksMode = -1 // SyncPropose; returns assigned offset
	AcksZero AcksMode = 0  // Propose (async); returns -1
)

// NotLeaderError is returned when this node is not the partition leader.
type NotLeaderError struct {
	LeaderNodeID  uint64
	LeaderAddress string
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not leader: leader is node %d at %s", e.LeaderNodeID, e.LeaderAddress)
}

// DataCoordinator routes produce and fetch requests to the correct partition
// Raft shard on this node.
type DataCoordinator struct{}

// Produce appends batch to the partition identified by (topic, partitionID).
func (dc *DataCoordinator) Produce(
	ctx context.Context,
	topic string,
	partitionID int32,
	batch []byte,
	acks AcksMode,
) (offset int64, err error) {
	return 0, errors.New("not implemented")
}

// Fetch returns up to maxBytes of serialised batch data starting at offset.
func (dc *DataCoordinator) Fetch(
	ctx context.Context,
	topic string,
	partitionID int32,
	offset int64,
	maxBytes int,
	maxWaitMs int64,
) (records []byte, nextOffset int64, err error) {
	return nil, 0, errors.New("not implemented")
}

// GetEarliestOffset returns the base_offset of the oldest available batch.
func (dc *DataCoordinator) GetEarliestOffset(
	ctx context.Context, topic string, partitionID int32,
) (int64, error) {
	return 0, errors.New("not implemented")
}

// GetLatestOffset returns the next offset to be assigned.
func (dc *DataCoordinator) GetLatestOffset(
	ctx context.Context, topic string, partitionID int32,
) (int64, error) {
	return 0, errors.New("not implemented")
}

// GetOffsetByTimestamp returns the base_offset of the first batch whose
// max_timestamp >= timestampMs.
func (dc *DataCoordinator) GetOffsetByTimestamp(
	ctx context.Context, topic string, partitionID int32, timestampMs int64,
) (int64, error) {
	return 0, errors.New("not implemented")
}

// StartPartitionReplica registers a partition shard as available for routing.
func (dc *DataCoordinator) StartPartitionReplica(topic string, partitionID int32, shardID uint64) {}

// StopPartitionReplica removes a partition shard from the routing registry.
func (dc *DataCoordinator) StopPartitionReplica(topic string, partitionID int32, shardID uint64) {}
