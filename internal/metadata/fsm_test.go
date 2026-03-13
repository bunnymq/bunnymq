package metadata

import (
	"encoding/json"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// helpers for CG tests

func createTopicWithPartitions(t *testing.T, fsm *MetadataFSM, name string, count int32) {
	t.Helper()
	replicas := make([][]uint64, count)
	for i := range replicas {
		replicas[i] = []uint64{1}
	}
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              name,
			PartitionCount:    count,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    replicas,
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("createTopic %s: %d %s", name, result.Value, result.Data)
	}
}

func joinGroup(t *testing.T, fsm *MetadataFSM, groupID, memberID, clientHost string, topics []string, joinedAtMs int64) sm.Result {
	t.Helper()
	return applyCmd(t, fsm, MetadataCommand{
		Type: CmdJoinConsumerGroup,
		JoinConsumerGroup: &JoinConsumerGroupCmd{
			GroupID:          groupID,
			MemberID:         memberID,
			ClientHost:       clientHost,
			SubscribedTopics: topics,
			JoinedAtMs:       joinedAtMs,
		},
	})
}

func mustMarshal(t *testing.T, cmd MetadataCommand) []byte {
	t.Helper()
	b, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func applyCmd(t *testing.T, fsm *MetadataFSM, cmd MetadataCommand) sm.Result {
	t.Helper()
	result, err := fsm.Update(sm.Entry{Cmd: mustMarshal(t, cmd)})
	if err != nil {
		t.Fatalf("Update returned non-nil error: %v", err)
	}
	return result
}

func TestMetadataFSM_CreateTopic(t *testing.T) {
	fsm := NewMetadataFSM()
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "test-topic",
			PartitionCount:    3,
			ReplicationFactor: 1,
			RetentionMs:       3600000,
			RetentionBytes:    -1,
			CreatedAtMs:       1000,
			ReplicaNodeIDs:    [][]uint64{{1}, {2}, {3}},
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got code=%d msg=%s", result.Value, result.Data)
	}
	for i := int32(0); i < 3; i++ {
		key := PartitionKey{Topic: "test-topic", PartitionID: i}
		pm, ok := fsm.state.Partitions[key]
		if !ok {
			t.Fatalf("partition %d not found", i)
		}
		want := uint64(i + 1)
		if pm.ShardID != want {
			t.Errorf("partition %d: ShardID = %d, want %d", i, pm.ShardID, want)
		}
	}
	if fsm.state.NextShardID != 4 {
		t.Errorf("NextShardID = %d, want 4", fsm.state.NextShardID)
	}
}

func TestMetadataFSM_CreateTopic_Duplicate(t *testing.T) {
	fsm := NewMetadataFSM()
	cmd := MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "my-topic",
			PartitionCount:    1,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}},
		},
	}
	applyCmd(t, fsm, cmd)
	result := applyCmd(t, fsm, cmd)
	if result.Value != ResultErrAlreadyExists {
		t.Fatalf("expected AlreadyExists, got %d", result.Value)
	}
	if len(fsm.state.Topics) != 1 {
		t.Errorf("expected 1 topic in state, got %d", len(fsm.state.Topics))
	}
}

func TestMetadataFSM_DeleteTopic(t *testing.T) {
	fsm := NewMetadataFSM()
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "gone-topic",
			PartitionCount:    2,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}, {2}},
		},
	})
	result := applyCmd(t, fsm, MetadataCommand{
		Type:        CmdDeleteTopic,
		DeleteTopic: &DeleteTopicCmd{Name: "gone-topic"},
	})
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got %d", result.Value)
	}
	if _, ok := fsm.state.Topics["gone-topic"]; ok {
		t.Error("topic still present after delete")
	}
	if len(fsm.state.Partitions) != 0 {
		t.Errorf("expected 0 partitions after delete, got %d", len(fsm.state.Partitions))
	}
}

func TestMetadataFSM_AlterPartCount_Invalid(t *testing.T) {
	fsm := NewMetadataFSM()
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "alter-topic",
			PartitionCount:    3,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}, {2}, {3}},
		},
	})
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdAlterTopicPartCount,
		AlterTopicPartCount: &AlterTopicPartCountCmd{
			Name:              "alter-topic",
			NewPartitionCount: 2,
		},
	})
	if result.Value != ResultErrInvalidArg {
		t.Fatalf("expected InvalidArg, got %d", result.Value)
	}
	if fsm.state.Topics["alter-topic"].PartitionCount != 3 {
		t.Error("partition count changed after invalid alter")
	}
}

func TestMetadataFSM_AlterRetention(t *testing.T) {
	fsm := NewMetadataFSM()
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "ret-topic",
			PartitionCount:    1,
			ReplicationFactor: 1,
			RetentionMs:       1000,
			RetentionBytes:    512,
			ReplicaNodeIDs:    [][]uint64{{1}},
		},
	})
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdAlterTopicRetention,
		AlterTopicRetention: &AlterTopicRetentionCmd{
			Name:           "ret-topic",
			RetentionMs:    9999,
			RetentionBytes: 8192,
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got %d", result.Value)
	}
	topic := fsm.state.Topics["ret-topic"]
	if topic.RetentionMs != 9999 || topic.RetentionBytes != 8192 {
		t.Errorf("retention not updated: ms=%d bytes=%d", topic.RetentionMs, topic.RetentionBytes)
	}
}

func TestMetadataFSM_RegisterNode_Idempotent(t *testing.T) {
	fsm := NewMetadataFSM()
	cmd := MetadataCommand{
		Type:         CmdRegisterNode,
		RegisterNode: &RegisterNodeCmd{NodeID: 42, Address: "host:9000"},
	}
	applyCmd(t, fsm, cmd)
	applyCmd(t, fsm, cmd)
	if len(fsm.state.Nodes) != 1 {
		t.Errorf("expected 1 node after two registrations of same node, got %d", len(fsm.state.Nodes))
	}
}

func TestMetadataFSM_AssignLeader_StaleEpoch(t *testing.T) {
	fsm := NewMetadataFSM()
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "leader-topic",
			PartitionCount:    1,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}},
		},
	})
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdAssignPartitionLeader,
		AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic:        "leader-topic",
			PartitionID:  0,
			LeaderNodeID: 1,
			LeaderEpoch:  5,
		},
	})
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdAssignPartitionLeader,
		AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic:        "leader-topic",
			PartitionID:  0,
			LeaderNodeID: 2,
			LeaderEpoch:  3,
		},
	})
	if result.Value != ResultErrInvalidArg {
		t.Fatalf("expected InvalidArg for stale epoch, got %d", result.Value)
	}
	pm := fsm.state.Partitions[PartitionKey{Topic: "leader-topic", PartitionID: 0}]
	if pm.LeaderNodeID != 1 || pm.LeaderEpoch != 5 {
		t.Errorf("leader changed after stale epoch: node=%d epoch=%d", pm.LeaderNodeID, pm.LeaderEpoch)
	}
}

func TestMetadataFSM_BadJSON(t *testing.T) {
	fsm := NewMetadataFSM()
	result, err := fsm.Update(sm.Entry{Cmd: []byte("not-valid-json{{")})
	if err != nil {
		t.Fatalf("Update must not return a Go error, got: %v", err)
	}
	if result.Value != ResultErrInvalidArg {
		t.Fatalf("expected InvalidArg for bad JSON, got %d", result.Value)
	}
	if len(fsm.state.Topics) != 0 {
		t.Error("state must be unchanged after bad JSON")
	}
}

func TestCGFSM_JoinGroup_NewGroup(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 3)

	result := joinGroup(t, fsm, "g1", "m1", "host1", []string{"t1"}, 1000)
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got %d %s", result.Value, result.Data)
	}

	var jr joinResult
	if err := json.Unmarshal(result.Data, &jr); err != nil {
		t.Fatalf("unmarshal join result: %v", err)
	}
	if jr.MemberID != "m1" {
		t.Errorf("member_id: got %q want %q", jr.MemberID, "m1")
	}
	if jr.GenerationID != 1 {
		t.Errorf("generation_id: got %d want 1", jr.GenerationID)
	}
	if len(jr.AssignedPartitions) != 3 {
		t.Errorf("assigned_partitions: got %d want 3", len(jr.AssignedPartitions))
	}

	group := fsm.state.Groups["g1"]
	if group == nil {
		t.Fatal("group not created")
	}
	if group.GenerationID != 1 {
		t.Errorf("group generation_id: got %d want 1", group.GenerationID)
	}
}

func TestCGFSM_JoinGroup_RebalanceTwoMembers(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 4)

	joinGroup(t, fsm, "g1", "m1", "host1", []string{"t1"}, 1000)
	result := joinGroup(t, fsm, "g1", "m2", "host2", []string{"t1"}, 2000)
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got %d", result.Value)
	}

	var jr joinResult
	if err := json.Unmarshal(result.Data, &jr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if jr.GenerationID != 2 {
		t.Errorf("generation_id: got %d want 2", jr.GenerationID)
	}

	group := fsm.state.Groups["g1"]
	m1Parts := len(group.Members["m1"].AssignedPartitions)
	m2Parts := len(group.Members["m2"].AssignedPartitions)
	if m1Parts+m2Parts != 4 {
		t.Errorf("total assigned partitions: got %d want 4", m1Parts+m2Parts)
	}
	if m1Parts == 0 || m2Parts == 0 {
		t.Errorf("each member must get at least one partition: m1=%d m2=%d", m1Parts, m2Parts)
	}
}

func TestCGFSM_JoinGroup_ServerAssignedID(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 1)

	result := joinGroup(t, fsm, "g1", "", "host1", []string{"t1"}, 5000)
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got %d", result.Value)
	}

	var jr joinResult
	if err := json.Unmarshal(result.Data, &jr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	wantID := "member-host1-5000"
	if jr.MemberID != wantID {
		t.Errorf("member_id: got %q want %q", jr.MemberID, wantID)
	}
	if _, ok := fsm.state.Groups["g1"].Members[wantID]; !ok {
		t.Errorf("member %q not found in group state", wantID)
	}
}

func TestCGFSM_LeaveGroup(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 6)

	joinGroup(t, fsm, "g1", "m1", "host1", []string{"t1"}, 1000)
	joinGroup(t, fsm, "g1", "m2", "host2", []string{"t1"}, 2000)
	joinGroup(t, fsm, "g1", "m3", "host3", []string{"t1"}, 3000)

	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdLeaveConsumerGroup,
		LeaveConsumerGroup: &LeaveConsumerGroupCmd{
			GroupID:  "g1",
			MemberID: "m2",
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got %d %s", result.Value, result.Data)
	}

	group := fsm.state.Groups["g1"]
	if _, ok := group.Members["m2"]; ok {
		t.Error("m2 still present after leave")
	}
	if group.GenerationID != 4 {
		t.Errorf("generation_id: got %d want 4", group.GenerationID)
	}
	total := len(group.Members["m1"].AssignedPartitions) + len(group.Members["m3"].AssignedPartitions)
	if total != 6 {
		t.Errorf("total assigned: got %d want 6", total)
	}
}

func TestCGFSM_Heartbeat_OK(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 1)
	joinGroup(t, fsm, "g1", "m1", "host1", []string{"t1"}, 1000)

	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdHeartbeatConsumerGroup,
		HeartbeatConsumerGroup: &HeartbeatConsumerGroupCmd{
			GroupID:      "g1",
			MemberID:     "m1",
			GenerationID: 1,
			TimestampMs:  9999,
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("expected OK (0), got %d", result.Value)
	}
	if fsm.state.Groups["g1"].Members["m1"].LastHeartbeatMs != 9999 {
		t.Errorf("LastHeartbeatMs not updated")
	}
}

func TestCGFSM_Heartbeat_Stale(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 1)
	joinGroup(t, fsm, "g1", "m1", "host1", []string{"t1"}, 1000)

	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdHeartbeatConsumerGroup,
		HeartbeatConsumerGroup: &HeartbeatConsumerGroupCmd{
			GroupID:      "g1",
			MemberID:     "m1",
			GenerationID: 99,
			TimestampMs:  2000,
		},
	})
	if result.Value != 1 {
		t.Fatalf("expected 1 (rebalance needed), got %d", result.Value)
	}
}

func TestCGFSM_CommitOffset(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 2)
	joinGroup(t, fsm, "g1", "m1", "host1", []string{"t1"}, 1000)

	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdCommitConsumerOffset,
		CommitConsumerOffset: &CommitConsumerOffsetCmd{
			GroupID: "g1",
			Offsets: []CommittedOffset{
				{Topic: "t1", PartitionID: 0, Offset: 42},
				{Topic: "t1", PartitionID: 1, Offset: 77},
			},
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("expected OK, got %d %s", result.Value, result.Data)
	}

	group := fsm.state.Groups["g1"]
	if off := group.CommittedOffsets[PartitionKey{"t1", 0}]; off != 42 {
		t.Errorf("partition 0 offset: got %d want 42", off)
	}
	if off := group.CommittedOffsets[PartitionKey{"t1", 1}]; off != 77 {
		t.Errorf("partition 1 offset: got %d want 77", off)
	}
}

func TestCGFSM_Rebalance_Determinism(t *testing.T) {
	setup := func() *MetadataFSM {
		fsm := NewMetadataFSM()
		createTopicWithPartitions(t, fsm, "t1", 5)
		joinGroup(t, fsm, "g1", "mA", "hostA", []string{"t1"}, 1000)
		joinGroup(t, fsm, "g1", "mB", "hostB", []string{"t1"}, 2000)
		joinGroup(t, fsm, "g1", "mC", "hostC", []string{"t1"}, 3000)
		return fsm
	}

	fsm1 := setup()
	fsm2 := setup()

	applyRebalance := func(fsm *MetadataFSM) {
		applyCmd(t, fsm, MetadataCommand{
			Type: CmdRebalanceConsumerGroup,
			RebalanceConsumerGroup: &RebalanceConsumerGroupCmd{
				GroupID:          "g1",
				ExpiredMemberIDs: []string{"mC"},
				TimestampMs:      9000,
			},
		})
	}
	applyRebalance(fsm1)
	applyRebalance(fsm2)

	for _, mID := range []string{"mA", "mB"} {
		p1 := fsm1.state.Groups["g1"].Members[mID].AssignedPartitions
		p2 := fsm2.state.Groups["g1"].Members[mID].AssignedPartitions
		if len(p1) != len(p2) {
			t.Errorf("member %s: fsm1 got %d partitions, fsm2 got %d", mID, len(p1), len(p2))
			continue
		}
		for i := range p1 {
			if p1[i] != p2[i] {
				t.Errorf("member %s partition[%d]: fsm1=%v fsm2=%v", mID, i, p1[i], p2[i])
			}
		}
	}
}
