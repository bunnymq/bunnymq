package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// MetadataFSM implements dragonboat's IStateMachine for the cluster metadata shard.
// It maintains in-memory state for topics, partitions, nodes, and consumer groups.
type MetadataFSM struct {
	state *MetadataState
}

var _ sm.IStateMachine = (*MetadataFSM)(nil)

func NewMetadataFSM() *MetadataFSM {
	return &MetadataFSM{
		state: &MetadataState{
			Topics:      make(map[string]*TopicMeta),
			Partitions:  make(map[PartitionKey]*PartitionMeta),
			Nodes:       make(map[uint64]*NodeInfo),
			Groups:      make(map[string]*ConsumerGroupMeta),
			NextShardID: 1,
		},
	}
}

func (fsm *MetadataFSM) Update(e sm.Entry) (sm.Result, error) {
	var cmd MetadataCommand
	if err := json.Unmarshal(e.Cmd, &cmd); err != nil {
		return ErrorResult(ResultErrInvalidArg, "invalid command JSON"), nil
	}
	switch cmd.Type {
	case CmdCreateTopic:
		if cmd.CreateTopic == nil {
			return ErrorResult(ResultErrInvalidArg, "missing create_topic payload"), nil
		}
		return fsm.applyCreateTopic(cmd.CreateTopic), nil
	case CmdDeleteTopic:
		if cmd.DeleteTopic == nil {
			return ErrorResult(ResultErrInvalidArg, "missing delete_topic payload"), nil
		}
		return fsm.applyDeleteTopic(cmd.DeleteTopic), nil
	case CmdAlterTopicPartCount:
		if cmd.AlterTopicPartCount == nil {
			return ErrorResult(ResultErrInvalidArg, "missing alter_topic_part_count payload"), nil
		}
		return fsm.applyAlterTopicPartCount(cmd.AlterTopicPartCount), nil
	case CmdAlterTopicRetention:
		if cmd.AlterTopicRetention == nil {
			return ErrorResult(ResultErrInvalidArg, "missing alter_topic_retention payload"), nil
		}
		return fsm.applyAlterTopicRetention(cmd.AlterTopicRetention), nil
	case CmdRegisterNode:
		if cmd.RegisterNode == nil {
			return ErrorResult(ResultErrInvalidArg, "missing register_node payload"), nil
		}
		return fsm.applyRegisterNode(cmd.RegisterNode), nil
	case CmdAssignPartitionLeader:
		if cmd.AssignPartitionLeader == nil {
			return ErrorResult(ResultErrInvalidArg, "missing assign_partition_leader payload"), nil
		}
		return fsm.applyAssignPartitionLeader(cmd.AssignPartitionLeader), nil
	case CmdJoinConsumerGroup:
		if cmd.JoinConsumerGroup == nil {
			return ErrorResult(ResultErrInvalidArg, "missing join_consumer_group payload"), nil
		}
		return fsm.applyJoinConsumerGroup(cmd.JoinConsumerGroup), nil
	case CmdLeaveConsumerGroup:
		if cmd.LeaveConsumerGroup == nil {
			return ErrorResult(ResultErrInvalidArg, "missing leave_consumer_group payload"), nil
		}
		return fsm.applyLeaveConsumerGroup(cmd.LeaveConsumerGroup), nil
	case CmdHeartbeatConsumerGroup:
		if cmd.HeartbeatConsumerGroup == nil {
			return ErrorResult(ResultErrInvalidArg, "missing heartbeat_consumer_group payload"), nil
		}
		return fsm.applyHeartbeatConsumerGroup(cmd.HeartbeatConsumerGroup), nil
	case CmdCommitConsumerOffset:
		if cmd.CommitConsumerOffset == nil {
			return ErrorResult(ResultErrInvalidArg, "missing commit_consumer_offset payload"), nil
		}
		return fsm.applyCommitConsumerOffset(cmd.CommitConsumerOffset), nil
	case CmdRebalanceConsumerGroup:
		if cmd.RebalanceConsumerGroup == nil {
			return ErrorResult(ResultErrInvalidArg, "missing rebalance_consumer_group payload"), nil
		}
		return fsm.applyRebalanceConsumerGroup(cmd.RebalanceConsumerGroup), nil
	default:
		return ErrorResult(ResultErrInvalidArg, "unknown command type"), nil
	}
}

func (fsm *MetadataFSM) applyCreateTopic(cmd *CreateTopicCmd) sm.Result {
	if _, exists := fsm.state.Topics[cmd.Name]; exists {
		return ErrorResult(ResultErrAlreadyExists, "topic already exists")
	}
	if cmd.PartitionCount <= 0 || cmd.ReplicationFactor <= 0 {
		return ErrorResult(ResultErrInvalidArg, "partition_count and replication_factor must be positive")
	}
	if int32(len(cmd.ReplicaNodeIDs)) != cmd.PartitionCount {
		return ErrorResult(ResultErrInvalidArg, "replica_node_ids length must equal partition_count")
	}
	fsm.state.Topics[cmd.Name] = &TopicMeta{
		Name:              cmd.Name,
		PartitionCount:    cmd.PartitionCount,
		ReplicationFactor: cmd.ReplicationFactor,
		RetentionMs:       cmd.RetentionMs,
		RetentionBytes:    cmd.RetentionBytes,
		CreatedAtMs:       cmd.CreatedAtMs,
	}
	for i := int32(0); i < cmd.PartitionCount; i++ {
		key := PartitionKey{Topic: cmd.Name, PartitionID: i}
		fsm.state.Partitions[key] = &PartitionMeta{
			Topic:          cmd.Name,
			PartitionID:    i,
			ShardID:        fsm.state.NextShardID + uint64(i),
			ReplicaNodeIDs: cmd.ReplicaNodeIDs[i],
		}
	}
	fsm.state.NextShardID += uint64(cmd.PartitionCount)
	return OKResult()
}

func (fsm *MetadataFSM) applyDeleteTopic(cmd *DeleteTopicCmd) sm.Result {
	topic, exists := fsm.state.Topics[cmd.Name]
	if !exists {
		return OKResult()
	}
	for i := int32(0); i < topic.PartitionCount; i++ {
		delete(fsm.state.Partitions, PartitionKey{Topic: cmd.Name, PartitionID: i})
	}
	delete(fsm.state.Topics, cmd.Name)
	return OKResult()
}

func (fsm *MetadataFSM) applyAlterTopicPartCount(cmd *AlterTopicPartCountCmd) sm.Result {
	topic, exists := fsm.state.Topics[cmd.Name]
	if !exists {
		return ErrorResult(ResultErrNotFound, "topic not found")
	}
	if cmd.NewPartitionCount <= topic.PartitionCount {
		return ErrorResult(ResultErrInvalidArg, "new_partition_count must be greater than current")
	}
	addCount := cmd.NewPartitionCount - topic.PartitionCount
	for i := int32(0); i < addCount; i++ {
		newPartID := topic.PartitionCount + i
		var replicas []uint64
		if int(i) < len(cmd.NewReplicaAssignments) {
			replicas = cmd.NewReplicaAssignments[i]
		}
		key := PartitionKey{Topic: cmd.Name, PartitionID: newPartID}
		fsm.state.Partitions[key] = &PartitionMeta{
			Topic:          cmd.Name,
			PartitionID:    newPartID,
			ShardID:        fsm.state.NextShardID + uint64(i),
			ReplicaNodeIDs: replicas,
		}
	}
	fsm.state.NextShardID += uint64(addCount)
	topic.PartitionCount = cmd.NewPartitionCount
	return OKResult()
}

func (fsm *MetadataFSM) applyAlterTopicRetention(cmd *AlterTopicRetentionCmd) sm.Result {
	topic, exists := fsm.state.Topics[cmd.Name]
	if !exists {
		return ErrorResult(ResultErrNotFound, "topic not found")
	}
	topic.RetentionMs = cmd.RetentionMs
	topic.RetentionBytes = cmd.RetentionBytes
	return OKResult()
}

func (fsm *MetadataFSM) applyRegisterNode(cmd *RegisterNodeCmd) sm.Result {
	fsm.state.Nodes[cmd.NodeID] = &NodeInfo{
		NodeID:  cmd.NodeID,
		Address: cmd.Address,
	}
	return OKResult()
}

func (fsm *MetadataFSM) applyAssignPartitionLeader(cmd *AssignPartitionLeaderCmd) sm.Result {
	key := PartitionKey{Topic: cmd.Topic, PartitionID: cmd.PartitionID}
	partition, exists := fsm.state.Partitions[key]
	if !exists {
		return ErrorResult(ResultErrNotFound, "partition not found")
	}
	if cmd.LeaderEpoch <= partition.LeaderEpoch {
		return ErrorResult(ResultErrInvalidArg, "leader_epoch must be greater than current")
	}
	partition.LeaderNodeID = cmd.LeaderNodeID
	partition.LeaderEpoch = cmd.LeaderEpoch
	return OKResult()
}

type joinResult struct {
	MemberID           string         `json:"member_id"`
	AssignedPartitions []PartitionKey `json:"assigned_partitions"`
	GenerationID       int32          `json:"generation_id"`
}

func (fsm *MetadataFSM) applyJoinConsumerGroup(cmd *JoinConsumerGroupCmd) sm.Result {
	group, exists := fsm.state.Groups[cmd.GroupID]
	if !exists {
		group = &ConsumerGroupMeta{
			GroupID:          cmd.GroupID,
			Members:          make(map[string]*MemberInfo),
			CommittedOffsets: make(map[PartitionKey]int64),
		}
		fsm.state.Groups[cmd.GroupID] = group
	}

	memberID := cmd.MemberID
	if memberID == "" {
		memberID = fmt.Sprintf("member-%s-%d", cmd.ClientHost, cmd.JoinedAtMs)
	}

	group.Members[memberID] = &MemberInfo{
		MemberID:         memberID,
		ClientHost:       cmd.ClientHost,
		SubscribedTopics: cmd.SubscribedTopics,
		LastHeartbeatMs:  cmd.JoinedAtMs,
	}

	rebalance(group, fsm.state.Topics, fsm.state.Partitions)
	group.GenerationID++

	assigned := group.Members[memberID].AssignedPartitions
	if assigned == nil {
		assigned = []PartitionKey{}
	}

	data, _ := json.Marshal(joinResult{
		MemberID:           memberID,
		AssignedPartitions: assigned,
		GenerationID:       group.GenerationID,
	})
	return sm.Result{Value: ResultOK, Data: data}
}

func (fsm *MetadataFSM) applyLeaveConsumerGroup(cmd *LeaveConsumerGroupCmd) sm.Result {
	group, exists := fsm.state.Groups[cmd.GroupID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	if _, exists := group.Members[cmd.MemberID]; !exists {
		return ErrorResult(ResultErrNotFound, "member not found")
	}
	delete(group.Members, cmd.MemberID)
	rebalance(group, fsm.state.Topics, fsm.state.Partitions)
	group.GenerationID++
	return OKResult()
}

func (fsm *MetadataFSM) applyHeartbeatConsumerGroup(cmd *HeartbeatConsumerGroupCmd) sm.Result {
	group, exists := fsm.state.Groups[cmd.GroupID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	member, exists := group.Members[cmd.MemberID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "member not found")
	}
	member.LastHeartbeatMs = cmd.TimestampMs
	if cmd.GenerationID != group.GenerationID {
		return sm.Result{Value: 1}
	}
	return OKResult()
}

func (fsm *MetadataFSM) applyCommitConsumerOffset(cmd *CommitConsumerOffsetCmd) sm.Result {
	group, exists := fsm.state.Groups[cmd.GroupID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	for _, o := range cmd.Offsets {
		group.CommittedOffsets[PartitionKey{Topic: o.Topic, PartitionID: o.PartitionID}] = o.Offset
	}
	return OKResult()
}

func (fsm *MetadataFSM) applyRebalanceConsumerGroup(cmd *RebalanceConsumerGroupCmd) sm.Result {
	group, exists := fsm.state.Groups[cmd.GroupID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	for _, id := range cmd.ExpiredMemberIDs {
		delete(group.Members, id)
	}
	rebalance(group, fsm.state.Topics, fsm.state.Partitions)
	group.GenerationID++
	return OKResult()
}

// rebalance computes range-based partition assignment for all group members.
// Inputs are sorted before iteration to guarantee determinism across replicas.
func rebalance(group *ConsumerGroupMeta, topics map[string]*TopicMeta, partitions map[PartitionKey]*PartitionMeta) {
	eligible := collectEligiblePartitions(group.Members, topics, partitions)
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].Topic != eligible[j].Topic {
			return eligible[i].Topic < eligible[j].Topic
		}
		return eligible[i].PartitionID < eligible[j].PartitionID
	})

	members := sortedMemberIDs(group.Members)
	if len(members) == 0 {
		return
	}

	for i, memberID := range members {
		lo := i * len(eligible) / len(members)
		hi := (i + 1) * len(eligible) / len(members)
		group.Members[memberID].AssignedPartitions = eligible[lo:hi]
	}
}

func collectEligiblePartitions(members map[string]*MemberInfo, topics map[string]*TopicMeta, partitions map[PartitionKey]*PartitionMeta) []PartitionKey {
	subscribed := make(map[string]struct{})
	for _, m := range members {
		for _, t := range m.SubscribedTopics {
			subscribed[t] = struct{}{}
		}
	}

	var eligible []PartitionKey
	for key := range partitions {
		if _, ok := subscribed[key.Topic]; ok {
			if _, ok := topics[key.Topic]; ok {
				eligible = append(eligible, key)
			}
		}
	}
	return eligible
}

func sortedMemberIDs(members map[string]*MemberInfo) []string {
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
