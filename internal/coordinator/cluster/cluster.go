package cluster

import (
	"context"
	"errors"
)

// ClusterCoordinator manages the topic and partition lifecycle for the cluster.
// All methods are safe to call concurrently from multiple gRPC handler goroutines.
type ClusterCoordinator struct{}

// TopicConfigOverrides carries optional per-topic retention overrides.
type TopicConfigOverrides struct {
	RetentionMs    *int64
	RetentionBytes *int64
}

// TopicInfo is a summary of a topic.
type TopicInfo struct {
	Name              string
	PartitionCount    int32
	ReplicationFactor int32
	RetentionMs       int64
	RetentionBytes    int64
	CreatedAtMs       int64
}

// PartitionInfo is metadata for a single partition.
type PartitionInfo struct {
	PartitionID    int32
	ShardID        uint64
	LeaderNodeID   uint64
	LeaderEpoch    int64
	ReplicaNodeIDs []uint64
}

// TopicDescription is full metadata for a topic including per-partition details.
type TopicDescription struct {
	TopicInfo
	Partitions []PartitionInfo
}

// ClusterDescription is the current cluster topology.
type ClusterDescription struct {
	Nodes []NodeDescriptor
}

// NodeDescriptor describes a cluster node.
type NodeDescriptor struct {
	NodeID  uint64
	Address string
}

// CreateTopic creates a new topic.
func (cc *ClusterCoordinator) CreateTopic(
	ctx context.Context,
	name string,
	partitionCount int32,
	replicationFactor int32,
	configOverrides TopicConfigOverrides,
) (TopicInfo, error) {
	return TopicInfo{}, errors.New("not implemented")
}

// DeleteTopic removes the topic from the Metadata FSM.
func (cc *ClusterCoordinator) DeleteTopic(ctx context.Context, name string) error {
	return errors.New("not implemented")
}

// ListTopics returns a summary of all topics in the cluster.
func (cc *ClusterCoordinator) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	return nil, errors.New("not implemented")
}

// DescribeTopic returns full metadata for a named topic.
func (cc *ClusterCoordinator) DescribeTopic(ctx context.Context, name string) (TopicDescription, error) {
	return TopicDescription{}, errors.New("not implemented")
}

// AlterTopicPartitionCount increases the partition count of an existing topic.
func (cc *ClusterCoordinator) AlterTopicPartitionCount(
	ctx context.Context,
	name string,
	newCount int32,
) error {
	return errors.New("not implemented")
}

// AlterTopicRetention updates the retention configuration for an existing topic.
func (cc *ClusterCoordinator) AlterTopicRetention(
	ctx context.Context,
	name string,
	retentionMs int64,
	retentionBytes int64,
) error {
	return errors.New("not implemented")
}

// DescribeCluster returns the current cluster topology.
func (cc *ClusterCoordinator) DescribeCluster(ctx context.Context) (ClusterDescription, error) {
	return ClusterDescription{}, errors.New("not implemented")
}
