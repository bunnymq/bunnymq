package client

import "errors"

// Sentinel errors returned by AdminClient methods.
var (
	ErrTopicAlreadyExists = errors.New("client: topic already exists")
	ErrTopicNotFound      = errors.New("client: topic not found")
)

// CreateTopicRequest is the input to AdminClient.CreateTopic.
type CreateTopicRequest struct {
	Name              string
	PartitionCount    int32
	ReplicationFactor int32
	RetentionMs       int64
	RetentionBytes    int64
}

// TopicInfo is a summary view of a topic.
type TopicInfo struct {
	Name              string
	PartitionCount    int32
	ReplicationFactor int32
	RetentionMs       int64
	RetentionBytes    int64
	CreatedAtMs       int64
}

// PartitionInfo describes a single partition's placement and leadership.
type PartitionInfo struct {
	PartitionID    int32
	ShardID        uint64
	LeaderNodeID   uint64
	LeaderEpoch    int64
	ReplicaNodeIDs []uint64
}

// TopicDescription is the full description of a topic, including partitions.
type TopicDescription struct {
	Topic      TopicInfo
	Partitions []PartitionInfo
}

// NodeDescriptor describes a broker node visible to the cluster.
type NodeDescriptor struct {
	NodeID  uint64
	Address string
}

// ClusterDescription contains the list of broker nodes.
type ClusterDescription struct {
	Nodes []NodeDescriptor
}

// PartitionInfoWithOffsets extends PartitionInfo with the current offset range.
type PartitionInfoWithOffsets struct {
	Info           PartitionInfo
	EarliestOffset int64
	LatestOffset   int64
}
