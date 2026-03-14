package metadata

import (
	"bytes"
	"encoding/json"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

func applyAll(t *testing.T, fsm *MetadataFSM, cmds []MetadataCommand) {
	t.Helper()
	for _, cmd := range cmds {
		b, err := json.Marshal(cmd)
		if err != nil {
			t.Fatalf("marshal command: %v", err)
		}
		if _, err := fsm.Update(sm.Entry{Cmd: b}); err != nil {
			t.Fatalf("Update: %v", err)
		}
	}
}

func snapshotBytes(t *testing.T, fsm *MetadataFSM) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := fsm.SaveSnapshot(&buf, nil, nil); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	return buf.Bytes()
}

// TestMetadataFSM_Determinism_Topics applies 20+ topic-lifecycle commands to two
// independent FSM instances and verifies their snapshots are byte-for-byte identical.
func TestMetadataFSM_Determinism_Topics(t *testing.T) {
	cmds := []MetadataCommand{
		// RegisterNode×3
		{Type: CmdRegisterNode, RegisterNode: &RegisterNodeCmd{NodeID: 1, Address: "127.0.0.1:4001"}},
		{Type: CmdRegisterNode, RegisterNode: &RegisterNodeCmd{NodeID: 2, Address: "127.0.0.1:4002"}},
		{Type: CmdRegisterNode, RegisterNode: &RegisterNodeCmd{NodeID: 3, Address: "127.0.0.1:4003"}},
		// CreateTopic×5
		{Type: CmdCreateTopic, CreateTopic: &CreateTopicCmd{
			Name: "topic-a", PartitionCount: 2, ReplicationFactor: 1,
			RetentionMs: 3_600_000, RetentionBytes: -1, CreatedAtMs: 1000,
			ReplicaNodeIDs: [][]uint64{{1}, {2}},
		}},
		{Type: CmdCreateTopic, CreateTopic: &CreateTopicCmd{
			Name: "topic-b", PartitionCount: 3, ReplicationFactor: 1,
			RetentionMs: 7_200_000, RetentionBytes: -1, CreatedAtMs: 2000,
			ReplicaNodeIDs: [][]uint64{{1}, {2}, {3}},
		}},
		{Type: CmdCreateTopic, CreateTopic: &CreateTopicCmd{
			Name: "topic-c", PartitionCount: 1, ReplicationFactor: 1,
			RetentionMs: 3_600_000, RetentionBytes: -1, CreatedAtMs: 3000,
			ReplicaNodeIDs: [][]uint64{{1}},
		}},
		{Type: CmdCreateTopic, CreateTopic: &CreateTopicCmd{
			Name: "topic-d", PartitionCount: 2, ReplicationFactor: 1,
			RetentionMs: 3_600_000, RetentionBytes: -1, CreatedAtMs: 4000,
			ReplicaNodeIDs: [][]uint64{{2}, {3}},
		}},
		{Type: CmdCreateTopic, CreateTopic: &CreateTopicCmd{
			Name: "topic-e", PartitionCount: 2, ReplicationFactor: 1,
			RetentionMs: 3_600_000, RetentionBytes: -1, CreatedAtMs: 5000,
			ReplicaNodeIDs: [][]uint64{{1}, {2}},
		}},
		// AlterTopicRetention×2
		{Type: CmdAlterTopicRetention, AlterTopicRetention: &AlterTopicRetentionCmd{
			Name: "topic-a", RetentionMs: 86_400_000, RetentionBytes: 1 << 30,
		}},
		{Type: CmdAlterTopicRetention, AlterTopicRetention: &AlterTopicRetentionCmd{
			Name: "topic-b", RetentionMs: 43_200_000, RetentionBytes: 1 << 29,
		}},
		// AssignPartitionLeader×5
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-a", PartitionID: 0, LeaderNodeID: 1, LeaderEpoch: 1,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-a", PartitionID: 1, LeaderNodeID: 2, LeaderEpoch: 1,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-b", PartitionID: 0, LeaderNodeID: 1, LeaderEpoch: 1,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-b", PartitionID: 1, LeaderNodeID: 2, LeaderEpoch: 1,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-b", PartitionID: 2, LeaderNodeID: 3, LeaderEpoch: 1,
		}},
		// DeleteTopic×1, then CreateTopic×1 with the same name
		{Type: CmdDeleteTopic, DeleteTopic: &DeleteTopicCmd{Name: "topic-c"}},
		{Type: CmdCreateTopic, CreateTopic: &CreateTopicCmd{
			Name: "topic-c", PartitionCount: 2, ReplicationFactor: 1,
			RetentionMs: 3_600_000, RetentionBytes: -1, CreatedAtMs: 6000,
			ReplicaNodeIDs: [][]uint64{{1}, {2}},
		}},
		// Additional commands to reach 20+
		{Type: CmdAlterTopicRetention, AlterTopicRetention: &AlterTopicRetentionCmd{
			Name: "topic-d", RetentionMs: 3_600_000, RetentionBytes: 512 << 20,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-c", PartitionID: 0, LeaderNodeID: 1, LeaderEpoch: 1,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-c", PartitionID: 1, LeaderNodeID: 2, LeaderEpoch: 1,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-d", PartitionID: 0, LeaderNodeID: 2, LeaderEpoch: 1,
		}},
		{Type: CmdAssignPartitionLeader, AssignPartitionLeader: &AssignPartitionLeaderCmd{
			Topic: "topic-d", PartitionID: 1, LeaderNodeID: 3, LeaderEpoch: 1,
		}},
	}

	fsmA := NewMetadataFSM()
	fsmB := NewMetadataFSM()

	applyAll(t, fsmA, cmds)
	applyAll(t, fsmB, cmds)

	snapA := snapshotBytes(t, fsmA)
	snapB := snapshotBytes(t, fsmB)

	if !bytes.Equal(snapA, snapB) {
		t.Fatalf("snapshots differ:\n  A: %s\n  B: %s", snapA, snapB)
	}
}

// TestMetadataFSM_Determinism_ConsumerGroups applies a consumer group lifecycle
// sequence to two FSM instances and verifies byte-equal snapshots.
func TestMetadataFSM_Determinism_ConsumerGroups(t *testing.T) {
	cmds := []MetadataCommand{
		{Type: CmdCreateTopic, CreateTopic: &CreateTopicCmd{
			Name: "cg-topic", PartitionCount: 3, ReplicationFactor: 1,
			RetentionMs: 3_600_000, RetentionBytes: -1, CreatedAtMs: 1000,
			ReplicaNodeIDs: [][]uint64{{1}, {1}, {1}},
		}},
		// JoinGroup×3 — explicit member IDs for determinism
		{Type: CmdJoinConsumerGroup, JoinConsumerGroup: &JoinConsumerGroupCmd{
			GroupID: "group-1", MemberID: "member-1", ClientHost: "host-a",
			SubscribedTopics: []string{"cg-topic"}, JoinedAtMs: 1000,
		}},
		{Type: CmdJoinConsumerGroup, JoinConsumerGroup: &JoinConsumerGroupCmd{
			GroupID: "group-1", MemberID: "member-2", ClientHost: "host-b",
			SubscribedTopics: []string{"cg-topic"}, JoinedAtMs: 2000,
		}},
		{Type: CmdJoinConsumerGroup, JoinConsumerGroup: &JoinConsumerGroupCmd{
			GroupID: "group-1", MemberID: "member-3", ClientHost: "host-c",
			SubscribedTopics: []string{"cg-topic"}, JoinedAtMs: 3000,
		}},
		// Heartbeat×3 with generation_id=3 (after 3 joins)
		{Type: CmdHeartbeatConsumerGroup, HeartbeatConsumerGroup: &HeartbeatConsumerGroupCmd{
			GroupID: "group-1", MemberID: "member-1", GenerationID: 3, TimestampMs: 4000,
		}},
		{Type: CmdHeartbeatConsumerGroup, HeartbeatConsumerGroup: &HeartbeatConsumerGroupCmd{
			GroupID: "group-1", MemberID: "member-2", GenerationID: 3, TimestampMs: 4000,
		}},
		{Type: CmdHeartbeatConsumerGroup, HeartbeatConsumerGroup: &HeartbeatConsumerGroupCmd{
			GroupID: "group-1", MemberID: "member-3", GenerationID: 3, TimestampMs: 4000,
		}},
		// CommitOffset×2
		{Type: CmdCommitConsumerOffset, CommitConsumerOffset: &CommitConsumerOffsetCmd{
			GroupID: "group-1",
			Offsets: []CommittedOffset{{Topic: "cg-topic", PartitionID: 0, Offset: 5}},
		}},
		{Type: CmdCommitConsumerOffset, CommitConsumerOffset: &CommitConsumerOffsetCmd{
			GroupID: "group-1",
			Offsets: []CommittedOffset{{Topic: "cg-topic", PartitionID: 1, Offset: 3}},
		}},
		// LeaveGroup×1
		{Type: CmdLeaveConsumerGroup, LeaveConsumerGroup: &LeaveConsumerGroupCmd{
			GroupID: "group-1", MemberID: "member-3",
		}},
	}

	fsmA := NewMetadataFSM()
	fsmB := NewMetadataFSM()

	applyAll(t, fsmA, cmds)
	applyAll(t, fsmB, cmds)

	snapA := snapshotBytes(t, fsmA)
	snapB := snapshotBytes(t, fsmB)

	if !bytes.Equal(snapA, snapB) {
		t.Fatalf("snapshots differ:\n  A: %s\n  B: %s", snapA, snapB)
	}
}

// TestMetadataFSM_Determinism_RebalanceOrdering joins members in different orders on
// two FSMs and verifies that the range-based rebalance produces byte-equal snapshots.
// This directly tests that sortedMemberIDs drives assignment, not map iteration order.
func TestMetadataFSM_Determinism_RebalanceOrdering(t *testing.T) {
	setupTopic := MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name: "rebalance-topic", PartitionCount: 4, ReplicationFactor: 1,
			RetentionMs: 3_600_000, RetentionBytes: -1, CreatedAtMs: 1000,
			ReplicaNodeIDs: [][]uint64{{1}, {1}, {1}, {1}},
		},
	}
	joinM1 := MetadataCommand{
		Type: CmdJoinConsumerGroup,
		JoinConsumerGroup: &JoinConsumerGroupCmd{
			GroupID: "group-order", MemberID: "member-1", ClientHost: "host-a",
			SubscribedTopics: []string{"rebalance-topic"}, JoinedAtMs: 1000,
		},
	}
	joinM2 := MetadataCommand{
		Type: CmdJoinConsumerGroup,
		JoinConsumerGroup: &JoinConsumerGroupCmd{
			GroupID: "group-order", MemberID: "member-2", ClientHost: "host-b",
			SubscribedTopics: []string{"rebalance-topic"}, JoinedAtMs: 2000,
		},
	}

	// FSM-A: M1 joins first, then M2.
	fsmA := NewMetadataFSM()
	applyAll(t, fsmA, []MetadataCommand{setupTopic, joinM1, joinM2})

	// FSM-B: M2 joins first, then M1.
	fsmB := NewMetadataFSM()
	applyAll(t, fsmB, []MetadataCommand{setupTopic, joinM2, joinM1})

	snapA := snapshotBytes(t, fsmA)
	snapB := snapshotBytes(t, fsmB)

	if !bytes.Equal(snapA, snapB) {
		t.Fatalf("snapshots differ with different join order:\n  A: %s\n  B: %s", snapA, snapB)
	}
}
