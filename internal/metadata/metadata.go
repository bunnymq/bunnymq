package metadata

import (
	"encoding/json"
	"io"

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
