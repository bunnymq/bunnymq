package metadata

import (
	"errors"
	"testing"
)

func lookup(t *testing.T, fsm *MetadataFSM, q MetadataQuery) interface{} {
	t.Helper()
	result, err := fsm.Lookup(q)
	if err != nil {
		t.Fatalf("Lookup(%v): unexpected error: %v", q.Type, err)
	}
	return result
}

func lookupErr(t *testing.T, fsm *MetadataFSM, q MetadataQuery) error {
	t.Helper()
	_, err := fsm.Lookup(q)
	return err
}

func TestMetadataLookup_GetTopic(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "alpha", 2)

	result := lookup(t, fsm, MetadataQuery{Type: QueryGetTopic, TopicName: "alpha"})
	tm, ok := result.(*TopicMeta)
	if !ok || tm == nil {
		t.Fatalf("expected *TopicMeta, got %T", result)
	}
	if tm.Name != "alpha" {
		t.Errorf("Name: got %q, want %q", tm.Name, "alpha")
	}
	if tm.PartitionCount != 2 {
		t.Errorf("PartitionCount: got %d, want 2", tm.PartitionCount)
	}
}

func TestMetadataLookup_GetTopic_NotFound(t *testing.T) {
	fsm := NewMetadataFSM()
	err := lookupErr(t, fsm, MetadataQuery{Type: QueryGetTopic, TopicName: "missing"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMetadataLookup_ListTopics_Sorted(t *testing.T) {
	fsm := NewMetadataFSM()
	for _, name := range []string{"zebra", "apple", "mango"} {
		createTopicWithPartitions(t, fsm, name, 1)
	}

	result := lookup(t, fsm, MetadataQuery{Type: QueryListTopics})
	topics, ok := result.([]*TopicMeta)
	if !ok {
		t.Fatalf("expected []*TopicMeta, got %T", result)
	}
	if len(topics) != 3 {
		t.Fatalf("expected 3 topics, got %d", len(topics))
	}
	want := []string{"apple", "mango", "zebra"}
	for i, w := range want {
		if topics[i].Name != w {
			t.Errorf("topics[%d].Name: got %q, want %q", i, topics[i].Name, w)
		}
	}
}

func TestMetadataLookup_GetPartition(t *testing.T) {
	fsm := NewMetadataFSM()
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "pt",
			PartitionCount:    2,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{10}, {20}},
		},
	})

	result := lookup(t, fsm, MetadataQuery{Type: QueryGetPartition, TopicName: "pt", PartitionID: 1})
	pm, ok := result.(*PartitionMeta)
	if !ok || pm == nil {
		t.Fatalf("expected *PartitionMeta, got %T", result)
	}
	if pm.Topic != "pt" || pm.PartitionID != 1 {
		t.Errorf("got topic=%q partitionID=%d", pm.Topic, pm.PartitionID)
	}
	if len(pm.ReplicaNodeIDs) != 1 || pm.ReplicaNodeIDs[0] != 20 {
		t.Errorf("ReplicaNodeIDs: %v", pm.ReplicaNodeIDs)
	}
}

func TestMetadataLookup_GetPartitions(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "multi", 4)

	result := lookup(t, fsm, MetadataQuery{Type: QueryGetPartitions, TopicName: "multi"})
	parts, ok := result.([]*PartitionMeta)
	if !ok {
		t.Fatalf("expected []*PartitionMeta, got %T", result)
	}
	if len(parts) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(parts))
	}
	for i, p := range parts {
		if p.PartitionID != int32(i) {
			t.Errorf("parts[%d].PartitionID: got %d, want %d", i, p.PartitionID, i)
		}
	}
}

func TestMetadataLookup_GetGroup(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 2)
	joinGroup(t, fsm, "grp1", "m1", "host1", []string{"t1"}, 1000)

	result := lookup(t, fsm, MetadataQuery{Type: QueryGetGroup, GroupID: "grp1"})
	gm, ok := result.(*ConsumerGroupMeta)
	if !ok || gm == nil {
		t.Fatalf("expected *ConsumerGroupMeta, got %T", result)
	}
	if gm.GroupID != "grp1" {
		t.Errorf("GroupID: got %q, want %q", gm.GroupID, "grp1")
	}
	if _, hasMember := gm.Members["m1"]; !hasMember {
		t.Error("member m1 not present in returned ConsumerGroupMeta")
	}
}

func TestMetadataLookup_CommittedOffset_Zero(t *testing.T) {
	fsm := NewMetadataFSM()
	createTopicWithPartitions(t, fsm, "t1", 1)
	joinGroup(t, fsm, "grp1", "m1", "host1", []string{"t1"}, 1000)

	result, err := fsm.Lookup(MetadataQuery{
		Type:    QueryGetCommittedOffset,
		GroupID: "grp1",
		PartKey: PartitionKey{Topic: "t1", PartitionID: 0},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	offset, ok := result.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", result)
	}
	if offset != 0 {
		t.Errorf("expected 0, got %d", offset)
	}
}

func TestMetadataLookup_ResultIsCopy(t *testing.T) {
	fsm := NewMetadataFSM()
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "original",
			PartitionCount:    1,
			ReplicationFactor: 1,
			ReplicaNodeIDs:    [][]uint64{{1}},
		},
	})

	result := lookup(t, fsm, MetadataQuery{Type: QueryGetTopic, TopicName: "original"})
	tm := result.(*TopicMeta)
	tm.Name = "mutated"

	result2 := lookup(t, fsm, MetadataQuery{Type: QueryGetTopic, TopicName: "original"})
	tm2 := result2.(*TopicMeta)
	if tm2.Name != "original" {
		t.Errorf("FSM state was mutated: got %q, want %q", tm2.Name, "original")
	}
}
