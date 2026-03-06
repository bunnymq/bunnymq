package metadata

import (
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// MetadataFSM implements dragonboat's IStateMachine for the cluster metadata shard.
// It maintains in-memory state for topics, partitions, nodes, and consumer groups.
type MetadataFSM struct {
	state *MetadataState //nolint:unused
}

var _ sm.IStateMachine = (*MetadataFSM)(nil)

// MetadataState is the in-memory state of the MetadataFSM.
type MetadataState struct {
	Topics      map[string]*TopicMeta
	Partitions  map[PartitionKey]*PartitionMeta
	Nodes       map[uint64]*NodeInfo
	Groups      map[string]*ConsumerGroupMeta
	NextShardID uint64
}

// PartitionKey uniquely identifies a partition within a topic.
type PartitionKey struct {
	Topic       string
	PartitionID int32
}

// TopicMeta holds metadata for a topic.
type TopicMeta struct {
	Name              string
	PartitionCount    int32
	ReplicationFactor int32
	RetentionMs       int64
	RetentionBytes    int64
	CreatedAtMs       int64
}

// PartitionMeta holds metadata for a single partition.
type PartitionMeta struct {
	Topic          string
	PartitionID    int32
	ShardID        uint64
	ReplicaNodeIDs []uint64
	LeaderNodeID   uint64
	LeaderEpoch    int64
}

// NodeInfo holds information about a cluster node.
type NodeInfo struct {
	NodeID  uint64
	Address string
}

// ConsumerGroupMeta holds state for a consumer group.
type ConsumerGroupMeta struct {
	GroupID          string
	GenerationID     int32
	Members          map[string]*MemberInfo
	CommittedOffsets map[PartitionKey]int64
}

// MemberInfo holds state for a consumer group member.
type MemberInfo struct {
	MemberID           string
	ClientHost         string
	SubscribedTopics   []string
	AssignedPartitions []PartitionKey
	LastHeartbeatMs    int64
}

func (fsm *MetadataFSM) Update(e sm.Entry) (sm.Result, error) {
	return sm.Result{}, nil
}

func (fsm *MetadataFSM) Lookup(q interface{}) (interface{}, error) {
	return nil, nil
}

func (fsm *MetadataFSM) SaveSnapshot(w io.Writer, c sm.ISnapshotFileCollection, done <-chan struct{}) error {
	return nil
}

func (fsm *MetadataFSM) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	return nil
}

func (fsm *MetadataFSM) Close() error {
	return nil
}
