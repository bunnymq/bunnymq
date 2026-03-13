package raft

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

