package raft

import (
	"context"
	"errors"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// Host wraps dragonboat's NodeHost and exposes typed helpers for metadata and
// partition shard operations. Other modules never import dragonboat directly.
type Host struct{}

// MetadataCommand is the command envelope for the Metadata FSM.
type MetadataCommand struct {
	Type                   CommandType `json:"type"`
	CreateTopic            *CreateTopicCmd `json:"ct,omitempty"`
	DeleteTopic            *DeleteTopicCmd `json:"dt,omitempty"`
	AlterTopicPartCount    *AlterTopicPartCountCmd `json:"atpc,omitempty"`
	AlterTopicRetention    *AlterTopicRetentionCmd `json:"atr,omitempty"`
	RegisterNode           *RegisterNodeCmd `json:"rn,omitempty"`
	AssignPartitionLeader  *AssignPartitionLeaderCmd `json:"apl,omitempty"`
	JoinConsumerGroup      *JoinConsumerGroupCmd `json:"jcg,omitempty"`
	LeaveConsumerGroup     *LeaveConsumerGroupCmd `json:"lcg,omitempty"`
	HeartbeatConsumerGroup *HeartbeatConsumerGroupCmd `json:"hcg,omitempty"`
	CommitConsumerOffset   *CommitConsumerOffsetCmd `json:"cco,omitempty"`
	RebalanceConsumerGroup *RebalanceConsumerGroupCmd `json:"rcg,omitempty"`
}

// CommandType identifies which Metadata FSM command is encoded.
type CommandType string

const (
	CmdCreateTopic            CommandType = "create_topic"
	CmdDeleteTopic            CommandType = "delete_topic"
	CmdAlterTopicPartCount    CommandType = "alter_topic_partition_count"
	CmdAlterTopicRetention    CommandType = "alter_topic_retention"
	CmdRegisterNode           CommandType = "register_node"
	CmdAssignPartitionLeader  CommandType = "assign_partition_leader"
	CmdJoinConsumerGroup      CommandType = "join_consumer_group"
	CmdLeaveConsumerGroup     CommandType = "leave_consumer_group"
	CmdHeartbeatConsumerGroup CommandType = "heartbeat_consumer_group"
	CmdCommitConsumerOffset   CommandType = "commit_consumer_offset"
	CmdRebalanceConsumerGroup CommandType = "rebalance_consumer_group"
)

// Stub command payload types.
type CreateTopicCmd struct{}
type DeleteTopicCmd struct{}
type AlterTopicPartCountCmd struct{}
type AlterTopicRetentionCmd struct{}
type RegisterNodeCmd struct{}
type AssignPartitionLeaderCmd struct{}
type JoinConsumerGroupCmd struct{}
type LeaveConsumerGroupCmd struct{}
type HeartbeatConsumerGroupCmd struct{}
type CommitConsumerOffsetCmd struct{}
type RebalanceConsumerGroupCmd struct{}

// MetadataQuery is the query envelope for the Metadata FSM Lookup calls.
type MetadataQuery struct {
	Type        QueryType
	TopicName   string
	PartitionID int32
	GroupID     string
}

// QueryType identifies which Metadata FSM query is being issued.
type QueryType int

const (
	QueryGetTopic            QueryType = iota + 1
	QueryListTopics
	QueryGetPartition
	QueryGetPartitions
	QueryGetNode
	QueryListNodes
	QueryGetGroup
	QueryGetCommittedOffset
)

// PartitionCommand is the command envelope for Partition FSM commands.
// The wire format is a 1-byte type prefix followed by a type-specific payload.
type PartitionCommand struct {
	Type    byte
	Payload []byte
}

// Partition FSM command type bytes.
const (
	CmdPartitionAppendBatch    byte = 0x01
	CmdPartitionRetentionConfig byte = 0x02
)

// PartitionQuery is the query envelope for Partition FSM Lookup calls.
type PartitionQuery struct {
	Type        PartitionQueryType
	Offset      int64
	TimestampMs int64
	MaxBytes    int
}

// PartitionQueryType identifies which Partition FSM query is being issued.
type PartitionQueryType int

const (
	QueryRead           PartitionQueryType = iota + 1
	QueryReadByTime
	QueryEarliestOffset
	QueryLatestOffset
	QueryGetNewDataCh
)

// SyncProposeMetadata proposes a metadata command and blocks until quorum commit.
func (h *Host) SyncProposeMetadata(ctx context.Context, cmd MetadataCommand) (sm.Result, error) {
	return sm.Result{}, errors.New("not implemented")
}

// ProposeMetadata proposes a metadata command with acks=0 (fire and forget).
func (h *Host) ProposeMetadata(ctx context.Context, cmd MetadataCommand) error {
	return errors.New("not implemented")
}

// LookupMetadata queries the local Metadata FSM without a Raft round-trip.
func (h *Host) LookupMetadata(ctx context.Context, q MetadataQuery) (interface{}, error) {
	return nil, errors.New("not implemented")
}

// SyncProposePartition proposes a partition command and blocks until quorum commit.
func (h *Host) SyncProposePartition(ctx context.Context, shardID uint64, cmd PartitionCommand) (sm.Result, error) {
	return sm.Result{}, errors.New("not implemented")
}

// ProposePartition proposes a partition command with acks=0 (fire and forget).
func (h *Host) ProposePartition(ctx context.Context, shardID uint64, cmd PartitionCommand) error {
	return errors.New("not implemented")
}

// LookupPartition queries the local Partition FSM without a Raft round-trip.
func (h *Host) LookupPartition(ctx context.Context, shardID uint64, q PartitionQuery) (interface{}, error) {
	return nil, errors.New("not implemented")
}

// StartPartitionShard starts a partition Raft shard on this node.
func (h *Host) StartPartitionShard(shardID uint64, initialMembers map[uint64]string, join bool) error {
	return errors.New("not implemented")
}

// StopPartitionShard stops a partition Raft shard on this node.
func (h *Host) StopPartitionShard(shardID uint64) error {
	return errors.New("not implemented")
}
