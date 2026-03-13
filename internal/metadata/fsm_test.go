package metadata

import (
	"encoding/json"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

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
