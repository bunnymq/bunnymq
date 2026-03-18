package metadata

import (
	"bytes"
	"testing"
	"time"
)

// joinGroupState applies JoinConsumerGroupCmd using the T-049 design:
// the coordinator supplies a MemberState and a pre-computed assignment.
func joinGroupState(t *testing.T, fsm *MetadataFSM, groupID, memberID string, ms MemberState, assignment map[string][]TopicPartition) {
	t.Helper()
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdJoinConsumerGroup,
		JoinConsumerGroup: &JoinConsumerGroupCmd{
			GroupID:       groupID,
			MemberID:      memberID,
			Member:        &ms,
			NewAssignment: assignment,
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("joinGroupState %s/%s: code=%d msg=%s", groupID, memberID, result.Value, result.Data)
	}
}

// leaveGroupState applies LeaveConsumerGroupCmd using the T-049 design.
func leaveGroupState(t *testing.T, fsm *MetadataFSM, groupID, memberID, reason string, assignment map[string][]TopicPartition) {
	t.Helper()
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdLeaveConsumerGroup,
		LeaveConsumerGroup: &LeaveConsumerGroupCmd{
			GroupID:       groupID,
			MemberID:      memberID,
			Reason:        reason,
			NewAssignment: assignment,
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("leaveGroupState %s/%s: code=%d msg=%s", groupID, memberID, result.Value, result.Data)
	}
}

func getGroupState(t *testing.T, fsm *MetadataFSM, groupID string) *GroupState {
	t.Helper()
	res, err := fsm.Lookup(MetadataQuery{Type: QueryGetGroupState, GroupID: groupID})
	if err != nil {
		t.Fatalf("QueryGetGroupState %q: %v", groupID, err)
	}
	if res == nil {
		return nil
	}
	gs, ok := res.(*GroupState)
	if !ok {
		t.Fatalf("QueryGetGroupState returned %T, want *GroupState", res)
	}
	return gs
}

func getGroupOffsets(t *testing.T, fsm *MetadataFSM, groupID string, partitions []TopicPartition) map[TopicPartition]int64 {
	t.Helper()
	res, err := fsm.Lookup(MetadataQuery{
		Type:       QueryGetGroupOffsets,
		GroupID:    groupID,
		Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("QueryGetGroupOffsets %q: %v", groupID, err)
	}
	m, ok := res.(map[TopicPartition]int64)
	if !ok {
		t.Fatalf("QueryGetGroupOffsets returned %T", res)
	}
	return m
}

func newMemberState(memberID string) MemberState {
	return MemberState{
		MemberID:            memberID,
		SubscribedTopics:    []string{"t1"},
		SessionTimeoutMs:    30000,
		HeartbeatIntervalMs: 3000,
		JoinedAt:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// TestMetadataFSM_JoinGroup_NewGroup verifies that the first JoinConsumerGroupCmd
// creates a group with GenerationID=1, stores the member, and stores the assignment.
func TestMetadataFSM_JoinGroup_NewGroup(t *testing.T) {
	fsm := NewMetadataFSM()

	assignment := map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}, {Topic: "t1", PartitionID: 1}},
	}
	joinGroupState(t, fsm, "g1", "m1", newMemberState("m1"), assignment)

	gs := getGroupState(t, fsm, "g1")
	if gs == nil {
		t.Fatal("group g1 not created")
	}
	if gs.GenerationID != 1 {
		t.Errorf("GenerationID: got %d, want 1", gs.GenerationID)
	}
	if _, ok := gs.Members["m1"]; !ok {
		t.Error("member m1 not present in group")
	}
	if len(gs.Assignments["m1"]) != 2 {
		t.Errorf("assignment for m1: got %d partitions, want 2", len(gs.Assignments["m1"]))
	}
}

// TestMetadataFSM_JoinGroup_ExistingGroup verifies that a second join increments
// GenerationID to 2 and both members are present.
func TestMetadataFSM_JoinGroup_ExistingGroup(t *testing.T) {
	fsm := NewMetadataFSM()

	joinGroupState(t, fsm, "g1", "m1", newMemberState("m1"), map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}},
	})
	joinGroupState(t, fsm, "g1", "m2", newMemberState("m2"), map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}},
		"m2": {{Topic: "t1", PartitionID: 1}},
	})

	gs := getGroupState(t, fsm, "g1")
	if gs == nil {
		t.Fatal("group g1 not found")
	}
	if gs.GenerationID != 2 {
		t.Errorf("GenerationID: got %d, want 2", gs.GenerationID)
	}
	if _, ok := gs.Members["m1"]; !ok {
		t.Error("member m1 missing")
	}
	if _, ok := gs.Members["m2"]; !ok {
		t.Error("member m2 missing")
	}
}

// TestMetadataFSM_LeaveGroup verifies that LeaveConsumerGroupCmd removes the
// member, increments GenerationID, and stores the updated assignment.
func TestMetadataFSM_LeaveGroup(t *testing.T) {
	fsm := NewMetadataFSM()

	joinGroupState(t, fsm, "g1", "m1", newMemberState("m1"), map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}},
		"m2": {{Topic: "t1", PartitionID: 1}},
	})
	joinGroupState(t, fsm, "g1", "m2", newMemberState("m2"), map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}},
		"m2": {{Topic: "t1", PartitionID: 1}},
	})

	// m2 leaves; coordinator pre-computes new assignment with only m1.
	leaveGroupState(t, fsm, "g1", "m2", "Voluntary", map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}, {Topic: "t1", PartitionID: 1}},
	})

	gs := getGroupState(t, fsm, "g1")
	if gs == nil {
		t.Fatal("group g1 not found after leave")
	}
	if gs.GenerationID != 3 {
		t.Errorf("GenerationID: got %d, want 3", gs.GenerationID)
	}
	if _, ok := gs.Members["m2"]; ok {
		t.Error("member m2 still present after leave")
	}
	if len(gs.Assignments["m1"]) != 2 {
		t.Errorf("m1 assignment after leave: got %d partitions, want 2", len(gs.Assignments["m1"]))
	}
}

// TestMetadataFSM_LeaveGroup_LastMember verifies that when the last member
// leaves, the group is retained in GroupStates with no members and no assignments
// so that committed offsets survive for the next consumer that joins.
func TestMetadataFSM_LeaveGroup_LastMember(t *testing.T) {
	fsm := NewMetadataFSM()

	joinGroupState(t, fsm, "g1", "m1", newMemberState("m1"), map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}},
	})

	leaveGroupState(t, fsm, "g1", "m1", "Voluntary", map[string][]TopicPartition{})

	gs := getGroupState(t, fsm, "g1")
	if gs == nil {
		t.Fatal("expected group to be retained after last member leaves (to preserve committed offsets)")
	}
	if len(gs.Members) != 0 {
		t.Errorf("expected no members after last member leaves, got %d", len(gs.Members))
	}
	if len(gs.Assignments) != 0 {
		t.Errorf("expected empty assignments after last member leaves, got %d", len(gs.Assignments))
	}
}

// TestMetadataFSM_CommitOffset verifies that CommitConsumerOffsetCmd (T-049 form)
// stores all 3 offsets and QueryGetGroupOffsets returns them correctly.
func TestMetadataFSM_CommitOffset(t *testing.T) {
	fsm := NewMetadataFSM()

	joinGroupState(t, fsm, "g1", "m1", newMemberState("m1"), map[string][]TopicPartition{
		"m1": {
			{Topic: "t1", PartitionID: 0},
			{Topic: "t1", PartitionID: 1},
			{Topic: "t1", PartitionID: 2},
		},
	})

	offsets := map[TopicPartition]int64{
		{Topic: "t1", PartitionID: 0}: 100,
		{Topic: "t1", PartitionID: 1}: 200,
		{Topic: "t1", PartitionID: 2}: 300,
	}
	result := applyCmd(t, fsm, MetadataCommand{
		Type: CmdCommitConsumerOffset,
		CommitConsumerOffset: &CommitConsumerOffsetCmd{
			GroupID:      "g1",
			GroupOffsets: offsets,
		},
	})
	if result.Value != ResultOK {
		t.Fatalf("CommitConsumerOffset: code=%d msg=%s", result.Value, result.Data)
	}

	partitions := []TopicPartition{
		{Topic: "t1", PartitionID: 0},
		{Topic: "t1", PartitionID: 1},
		{Topic: "t1", PartitionID: 2},
	}
	got := getGroupOffsets(t, fsm, "g1", partitions)
	for _, tp := range partitions {
		want := offsets[tp]
		if got[tp] != want {
			t.Errorf("offset %v: got %d, want %d", tp, got[tp], want)
		}
	}
}

// TestMetadataFSM_QueryGetGroup_NotFound verifies that QueryGetGroupState
// returns nil (no error) for an unknown group.
func TestMetadataFSM_QueryGetGroup_NotFound(t *testing.T) {
	fsm := NewMetadataFSM()

	gs := getGroupState(t, fsm, "nonexistent")
	if gs != nil {
		t.Errorf("expected nil for unknown group, got %+v", gs)
	}
}

// TestMetadataFSM_QueryGetGroupOffsets_MissingPartition verifies that
// QueryGetGroupOffsets returns -1 for partitions with no committed offset.
func TestMetadataFSM_QueryGetGroupOffsets_MissingPartition(t *testing.T) {
	fsm := NewMetadataFSM()

	joinGroupState(t, fsm, "g1", "m1", newMemberState("m1"), map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}},
	})

	// Only commit offset for partition 0; query both 0 and 1.
	applyCmd(t, fsm, MetadataCommand{
		Type: CmdCommitConsumerOffset,
		CommitConsumerOffset: &CommitConsumerOffsetCmd{
			GroupID: "g1",
			GroupOffsets: map[TopicPartition]int64{
				{Topic: "t1", PartitionID: 0}: 42,
			},
		},
	})

	got := getGroupOffsets(t, fsm, "g1", []TopicPartition{
		{Topic: "t1", PartitionID: 0},
		{Topic: "t1", PartitionID: 1},
	})
	if got[TopicPartition{Topic: "t1", PartitionID: 0}] != 42 {
		t.Errorf("partition 0: got %d, want 42", got[TopicPartition{Topic: "t1", PartitionID: 0}])
	}
	if got[TopicPartition{Topic: "t1", PartitionID: 1}] != -1 {
		t.Errorf("partition 1 (uncommitted): got %d, want -1", got[TopicPartition{Topic: "t1", PartitionID: 1}])
	}
}

// TestMetadataFSM_GroupSnapshot verifies that GroupStates (including committed
// offsets) survive a snapshot round-trip.
func TestMetadataFSM_GroupSnapshot(t *testing.T) {
	src := NewMetadataFSM()

	// Join two groups.
	joinGroupState(t, src, "g1", "m1", newMemberState("m1"), map[string][]TopicPartition{
		"m1": {{Topic: "t1", PartitionID: 0}},
	})
	joinGroupState(t, src, "g2", "m2", newMemberState("m2"), map[string][]TopicPartition{
		"m2": {{Topic: "t2", PartitionID: 0}},
	})

	// Commit offsets for both groups.
	applyCmd(t, src, MetadataCommand{
		Type: CmdCommitConsumerOffset,
		CommitConsumerOffset: &CommitConsumerOffsetCmd{
			GroupID: "g1",
			GroupOffsets: map[TopicPartition]int64{
				{Topic: "t1", PartitionID: 0}: 10,
			},
		},
	})
	applyCmd(t, src, MetadataCommand{
		Type: CmdCommitConsumerOffset,
		CommitConsumerOffset: &CommitConsumerOffsetCmd{
			GroupID: "g2",
			GroupOffsets: map[TopicPartition]int64{
				{Topic: "t2", PartitionID: 0}: 20,
			},
		},
	})

	var buf bytes.Buffer
	if err := src.SaveSnapshot(&buf, nil, make(chan struct{})); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	dst := NewMetadataFSM()
	if err := dst.RecoverFromSnapshot(&buf, nil, make(chan struct{})); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}

	g1 := getGroupState(t, dst, "g1")
	if g1 == nil {
		t.Fatal("group g1 not restored")
	}
	if _, ok := g1.Members["m1"]; !ok {
		t.Error("g1: member m1 missing after restore")
	}
	if g1.GenerationID != 1 {
		t.Errorf("g1 GenerationID: got %d, want 1", g1.GenerationID)
	}

	g2 := getGroupState(t, dst, "g2")
	if g2 == nil {
		t.Fatal("group g2 not restored")
	}
	if _, ok := g2.Members["m2"]; !ok {
		t.Error("g2: member m2 missing after restore")
	}

	offs1 := getGroupOffsets(t, dst, "g1", []TopicPartition{{Topic: "t1", PartitionID: 0}})
	if offs1[TopicPartition{Topic: "t1", PartitionID: 0}] != 10 {
		t.Errorf("g1 offset: got %d, want 10", offs1[TopicPartition{Topic: "t1", PartitionID: 0}])
	}

	offs2 := getGroupOffsets(t, dst, "g2", []TopicPartition{{Topic: "t2", PartitionID: 0}})
	if offs2[TopicPartition{Topic: "t2", PartitionID: 0}] != 20 {
		t.Errorf("g2 offset: got %d, want 20", offs2[TopicPartition{Topic: "t2", PartitionID: 0}])
	}
}
