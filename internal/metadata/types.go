package metadata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrNotFound = errors.New("not found")

// --- In-memory state ---

type MetadataState struct {
	Topics      map[string]*TopicMeta
	Partitions  map[PartitionKey]*PartitionMeta
	Nodes       map[uint64]*NodeInfo
	Groups      map[string]*ConsumerGroupMeta
	NextShardID uint64
}

type PartitionKey struct {
	Topic       string
	PartitionID int32
}

// MarshalText implements encoding.TextMarshaler so PartitionKey can be used as a JSON map key.
func (k PartitionKey) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%s\x00%d", k.Topic, k.PartitionID)), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (k *PartitionKey) UnmarshalText(data []byte) error {
	s := string(data)
	idx := strings.LastIndex(s, "\x00")
	if idx < 0 {
		return fmt.Errorf("invalid PartitionKey text: %q", s)
	}
	n, err := strconv.ParseInt(s[idx+1:], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid PartitionKey partition_id: %w", err)
	}
	k.Topic = s[:idx]
	k.PartitionID = int32(n)
	return nil
}

type TopicMeta struct {
	Name              string `json:"name"`
	PartitionCount    int32  `json:"partition_count"`
	ReplicationFactor int32  `json:"replication_factor"`
	RetentionMs       int64  `json:"retention_ms"`
	RetentionBytes    int64  `json:"retention_bytes"`
	CreatedAtMs       int64  `json:"created_at_ms"`
}

type PartitionMeta struct {
	Topic          string   `json:"topic"`
	PartitionID    int32    `json:"partition_id"`
	ShardID        uint64   `json:"shard_id"`
	ReplicaNodeIDs []uint64 `json:"replica_node_ids"`
	LeaderNodeID   uint64   `json:"leader_node_id"`
	LeaderEpoch    int64    `json:"leader_epoch"`
}

type NodeInfo struct {
	NodeID  uint64 `json:"node_id"`
	Address string `json:"address"`
}

type ConsumerGroupMeta struct {
	GroupID          string                  `json:"group_id"`
	GenerationID     int32                   `json:"generation_id"`
	Members          map[string]*MemberInfo  `json:"members"`
	CommittedOffsets map[PartitionKey]int64  `json:"committed_offsets"`
}

type MemberInfo struct {
	MemberID           string         `json:"member_id"`
	ClientHost         string         `json:"client_host"`
	SubscribedTopics   []string       `json:"subscribed_topics"`
	AssignedPartitions []PartitionKey `json:"assigned_partitions"`
	LastHeartbeatMs    int64          `json:"last_heartbeat_ms"`
}

// --- Command types ---

type CommandType string

const (
	CmdCreateTopic             CommandType = "create_topic"
	CmdDeleteTopic             CommandType = "delete_topic"
	CmdAlterTopicPartCount     CommandType = "alter_topic_part_count"
	CmdAlterTopicRetention     CommandType = "alter_topic_retention"
	CmdRegisterNode            CommandType = "register_node"
	CmdAssignPartitionLeader   CommandType = "assign_partition_leader"
	CmdJoinConsumerGroup       CommandType = "join_consumer_group"
	CmdLeaveConsumerGroup      CommandType = "leave_consumer_group"
	CmdHeartbeatConsumerGroup  CommandType = "heartbeat_consumer_group"
	CmdCommitConsumerOffset    CommandType = "commit_consumer_offset"
	CmdRebalanceConsumerGroup  CommandType = "rebalance_consumer_group"
)

// MetadataCommand is the Raft log entry envelope for the metadata shard.
// Short JSON field names reduce snapshot size; only one inner struct is non-nil per command.
type MetadataCommand struct {
	Type                   CommandType                `json:"type"`
	CreateTopic            *CreateTopicCmd            `json:"ct,omitempty"`
	DeleteTopic            *DeleteTopicCmd            `json:"dt,omitempty"`
	AlterTopicPartCount    *AlterTopicPartCountCmd    `json:"atpc,omitempty"`
	AlterTopicRetention    *AlterTopicRetentionCmd    `json:"atr,omitempty"`
	RegisterNode           *RegisterNodeCmd           `json:"rn,omitempty"`
	AssignPartitionLeader  *AssignPartitionLeaderCmd  `json:"apl,omitempty"`
	JoinConsumerGroup      *JoinConsumerGroupCmd      `json:"jcg,omitempty"`
	LeaveConsumerGroup     *LeaveConsumerGroupCmd     `json:"lcg,omitempty"`
	HeartbeatConsumerGroup *HeartbeatConsumerGroupCmd `json:"hcg,omitempty"`
	CommitConsumerOffset   *CommitConsumerOffsetCmd   `json:"cco,omitempty"`
	RebalanceConsumerGroup *RebalanceConsumerGroupCmd `json:"rcg,omitempty"`
}

type CreateTopicCmd struct {
	Name              string       `json:"name"`
	PartitionCount    int32        `json:"partition_count"`
	ReplicationFactor int32        `json:"replication_factor"`
	RetentionMs       int64        `json:"retention_ms"`
	RetentionBytes    int64        `json:"retention_bytes"`
	CreatedAtMs       int64        `json:"created_at_ms"`
	ReplicaNodeIDs    [][]uint64   `json:"replica_node_ids"`
}

type DeleteTopicCmd struct {
	Name string `json:"name"`
}

type AlterTopicPartCountCmd struct {
	Name                string       `json:"name"`
	NewPartitionCount   int32        `json:"new_partition_count"`
	NewReplicaAssignments [][]uint64 `json:"new_replica_assignments"`
}

type AlterTopicRetentionCmd struct {
	Name           string `json:"name"`
	RetentionMs    int64  `json:"retention_ms"`
	RetentionBytes int64  `json:"retention_bytes"`
}

type RegisterNodeCmd struct {
	NodeID  uint64 `json:"node_id"`
	Address string `json:"address"`
}

type AssignPartitionLeaderCmd struct {
	Topic        string `json:"topic"`
	PartitionID  int32  `json:"partition_id"`
	LeaderNodeID uint64 `json:"leader_node_id"`
	LeaderEpoch  int64  `json:"leader_epoch"`
}

type JoinConsumerGroupCmd struct {
	GroupID          string   `json:"group_id"`
	MemberID         string   `json:"member_id"`
	ClientHost       string   `json:"client_host"`
	SubscribedTopics []string `json:"subscribed_topics"`
	JoinedAtMs       int64    `json:"joined_at_ms"`
}

type LeaveConsumerGroupCmd struct {
	GroupID  string `json:"group_id"`
	MemberID string `json:"member_id"`
}

type HeartbeatConsumerGroupCmd struct {
	GroupID      string `json:"group_id"`
	MemberID     string `json:"member_id"`
	GenerationID int32  `json:"generation_id"`
	TimestampMs  int64  `json:"timestamp_ms"`
}

type CommittedOffset struct {
	Topic       string `json:"topic"`
	PartitionID int32  `json:"partition_id"`
	Offset      int64  `json:"offset"`
}

type CommitConsumerOffsetCmd struct {
	GroupID string            `json:"group_id"`
	Offsets []CommittedOffset `json:"offsets"`
}

type RebalanceConsumerGroupCmd struct {
	GroupID          string   `json:"group_id"`
	ExpiredMemberIDs []string `json:"expired_member_ids"`
	TimestampMs      int64    `json:"timestamp_ms"`
}

// --- Query types ---

type QueryType string

const (
	QueryGetTopic           QueryType = "get_topic"
	QueryListTopics         QueryType = "list_topics"
	QueryGetPartition       QueryType = "get_partition"
	QueryGetPartitions      QueryType = "get_partitions"
	QueryGetNode            QueryType = "get_node"
	QueryListNodes          QueryType = "list_nodes"
	QueryGetGroup           QueryType = "get_group"
	QueryGetCommittedOffset QueryType = "get_committed_offset"
)

type MetadataQuery struct {
	Type        QueryType
	TopicName   string
	PartitionID int32
	NodeID      uint64
	GroupID     string
	PartKey     PartitionKey
}
