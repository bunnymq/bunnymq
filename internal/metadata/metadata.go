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
			GroupStates: make(map[string]*GroupState),
			NextShardID: 1,
		},
	}
}

func (fsm *MetadataFSM) Update(e sm.Entry) (sm.Result, error) {
	var cmd MetadataCommand
	if err := json.Unmarshal(e.Cmd, &cmd); err != nil {
		return ErrorResult(ResultErrInvalidArg, "invalid command JSON"), nil
	}
	return fsm.applyCommand(&cmd), nil
}

func (fsm *MetadataFSM) applyCommand(cmd *MetadataCommand) sm.Result {
	switch cmd.Type {
	case CmdCreateTopic:
		return fsm.applyCreateTopic(cmd.CreateTopic)
	case CmdDeleteTopic:
		return fsm.applyDeleteTopic(cmd.DeleteTopic)
	case CmdAlterTopicPartCount:
		return fsm.applyAlterTopicPartCount(cmd.AlterTopicPartCount)
	case CmdAlterTopicRetention:
		return fsm.applyAlterTopicRetention(cmd.AlterTopicRetention)
	case CmdRegisterNode:
		return fsm.applyRegisterNode(cmd.RegisterNode)
	case CmdAssignPartitionLeader:
		return fsm.applyAssignPartitionLeader(cmd.AssignPartitionLeader)
	case CmdJoinConsumerGroup:
		return fsm.applyJoinConsumerGroup(cmd.JoinConsumerGroup)
	case CmdLeaveConsumerGroup:
		return fsm.applyLeaveConsumerGroup(cmd.LeaveConsumerGroup)
	case CmdHeartbeatConsumerGroup:
		return fsm.applyHeartbeatConsumerGroup(cmd.HeartbeatConsumerGroup)
	case CmdCommitConsumerOffset:
		return fsm.applyCommitConsumerOffset(cmd.CommitConsumerOffset)
	case CmdRebalanceConsumerGroup:
		return fsm.applyRebalanceConsumerGroup(cmd.RebalanceConsumerGroup)
	default:
		return ErrorResult(ResultErrInvalidArg, "unknown command type")
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
	if cmd.Member != nil {
		return fsm.applyGroupStateJoin(cmd)
	}

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

// applyGroupStateJoin handles JoinConsumerGroupCmd when the coordinator has
// pre-computed the assignment. The FSM stores the member and assignment without
// running rebalance itself.
func (fsm *MetadataFSM) applyGroupStateJoin(cmd *JoinConsumerGroupCmd) sm.Result {
	if fsm.state.GroupStates == nil {
		fsm.state.GroupStates = make(map[string]*GroupState)
	}
	group, exists := fsm.state.GroupStates[cmd.GroupID]
	if !exists {
		group = &GroupState{
			GroupID:     cmd.GroupID,
			Members:     make(map[string]MemberState),
			Assignments: make(map[string][]TopicPartition),
			Offsets:     make(map[TopicPartition]int64),
		}
		fsm.state.GroupStates[cmd.GroupID] = group
	}
	group.Members[cmd.MemberID] = *cmd.Member
	if cmd.NewAssignment != nil {
		group.Assignments = cmd.NewAssignment
	}
	group.GenerationID++
	return OKResult()
}

func (fsm *MetadataFSM) applyLeaveConsumerGroup(cmd *LeaveConsumerGroupCmd) sm.Result {
	// If the group was created via the T-049 design (GroupStates), use that path.
	if fsm.state.GroupStates != nil {
		if _, inGroupStates := fsm.state.GroupStates[cmd.GroupID]; inGroupStates {
			return fsm.applyGroupStateLeave(cmd)
		}
	}

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

// applyGroupStateLeave handles LeaveConsumerGroupCmd when the coordinator has
// pre-computed the new assignment. Empty groups are deleted from GroupStates.
func (fsm *MetadataFSM) applyGroupStateLeave(cmd *LeaveConsumerGroupCmd) sm.Result {
	if fsm.state.GroupStates == nil {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	group, exists := fsm.state.GroupStates[cmd.GroupID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	if _, exists := group.Members[cmd.MemberID]; !exists {
		return ErrorResult(ResultErrNotFound, "member not found")
	}
	delete(group.Members, cmd.MemberID)
	if cmd.NewAssignment != nil {
		group.Assignments = cmd.NewAssignment
	} else {
		// Empty map after JSON round-trip of omitempty field; clear assignments.
		group.Assignments = make(map[string][]TopicPartition)
	}
	group.GenerationID++
	if len(group.Members) == 0 {
		delete(fsm.state.GroupStates, cmd.GroupID)
	}
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
	if cmd.GroupOffsets != nil {
		return fsm.applyGroupStateCommitOffset(cmd)
	}

	group, exists := fsm.state.Groups[cmd.GroupID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	for _, o := range cmd.Offsets {
		group.CommittedOffsets[PartitionKey{Topic: o.Topic, PartitionID: o.PartitionID}] = o.Offset
	}
	return OKResult()
}

// applyGroupStateCommitOffset merges GroupOffsets into GroupState.Offsets.
func (fsm *MetadataFSM) applyGroupStateCommitOffset(cmd *CommitConsumerOffsetCmd) sm.Result {
	if fsm.state.GroupStates == nil {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	group, exists := fsm.state.GroupStates[cmd.GroupID]
	if !exists {
		return ErrorResult(ResultErrNotFound, "group not found")
	}
	if group.Offsets == nil {
		group.Offsets = make(map[TopicPartition]int64)
	}
	for tp, offset := range cmd.GroupOffsets {
		group.Offsets[tp] = offset
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
	query, ok := q.(MetadataQuery)
	if !ok {
		return nil, fmt.Errorf("expected MetadataQuery, got %T", q)
	}

	switch query.Type {
	case QueryGetTopic:
		t, exists := fsm.state.Topics[query.TopicName]
		if !exists {
			return nil, ErrNotFound
		}
		c := *t
		return &c, nil

	case QueryListTopics:
		topics := make([]*TopicMeta, 0, len(fsm.state.Topics))
		for _, t := range fsm.state.Topics {
			c := *t
			topics = append(topics, &c)
		}
		sort.Slice(topics, func(i, j int) bool {
			return topics[i].Name < topics[j].Name
		})
		return topics, nil

	case QueryGetPartition:
		key := PartitionKey{Topic: query.TopicName, PartitionID: query.PartitionID}
		p, exists := fsm.state.Partitions[key]
		if !exists {
			return nil, ErrNotFound
		}
		c := *p
		return &c, nil

	case QueryGetPartitions:
		if _, exists := fsm.state.Topics[query.TopicName]; !exists {
			return nil, ErrNotFound
		}
		var parts []*PartitionMeta
		for key, p := range fsm.state.Partitions {
			if key.Topic == query.TopicName {
				c := *p
				parts = append(parts, &c)
			}
		}
		sort.Slice(parts, func(i, j int) bool {
			return parts[i].PartitionID < parts[j].PartitionID
		})
		return parts, nil

	case QueryGetNode:
		n, exists := fsm.state.Nodes[query.NodeID]
		if !exists {
			return nil, ErrNotFound
		}
		c := *n
		return &c, nil

	case QueryListNodes:
		nodes := make([]*NodeInfo, 0, len(fsm.state.Nodes))
		for _, n := range fsm.state.Nodes {
			c := *n
			nodes = append(nodes, &c)
		}
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].NodeID < nodes[j].NodeID
		})
		return nodes, nil

	case QueryGetGroup:
		g, exists := fsm.state.Groups[query.GroupID]
		if !exists {
			return nil, ErrNotFound
		}
		c := *g
		return &c, nil

	case QueryGetCommittedOffset:
		g, exists := fsm.state.Groups[query.GroupID]
		if !exists {
			return nil, ErrNotFound
		}
		return g.CommittedOffsets[query.PartKey], nil

	default:
		return fsm.lookupGroupState(query)
	}
}

func (fsm *MetadataFSM) lookupGroupState(query MetadataQuery) (interface{}, error) {
	switch query.Type {
	case QueryGetGroupState:
		if fsm.state.GroupStates == nil {
			return nil, nil
		}
		g, exists := fsm.state.GroupStates[query.GroupID]
		if !exists {
			return nil, nil
		}
		c := *g
		return &c, nil

	case QueryGetGroupOffsets:
		return fsm.lookupGroupOffsets(query.GroupID, query.Partitions), nil

	case QueryGetAllGroupStates:
		if fsm.state.GroupStates == nil {
			return map[string]*GroupState{}, nil
		}
		result := make(map[string]*GroupState, len(fsm.state.GroupStates))
		for k, v := range fsm.state.GroupStates {
			c := *v
			result[k] = &c
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unknown query type: %q", query.Type)
	}
}

func (fsm *MetadataFSM) lookupGroupOffsets(groupID string, partitions []TopicPartition) map[TopicPartition]int64 {
	result := make(map[TopicPartition]int64, len(partitions))
	for _, tp := range partitions {
		result[tp] = -1
	}
	if fsm.state.GroupStates == nil {
		return result
	}
	g, exists := fsm.state.GroupStates[groupID]
	if !exists {
		return result
	}
	for _, tp := range partitions {
		if offset, ok := g.Offsets[tp]; ok {
			result[tp] = offset
		}
	}
	return result
}

func (fsm *MetadataFSM) SaveSnapshot(w io.Writer, _ sm.ISnapshotFileCollection, done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	return json.NewEncoder(w).Encode(fsm.state)
}

func (fsm *MetadataFSM) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile, done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	s := &MetadataState{}
	if err := json.NewDecoder(r).Decode(s); err != nil {
		return err
	}
	if s.GroupStates == nil {
		s.GroupStates = make(map[string]*GroupState)
	}
	fsm.state = s
	return nil
}

func (fsm *MetadataFSM) Close() error {
	return nil
}
