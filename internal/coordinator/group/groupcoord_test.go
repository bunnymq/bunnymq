package group

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/metadata"
)

// stubNodeHost drives an in-memory MetadataFSM, applying commands and serving lookups.
type stubNodeHost struct {
	fsm *metadata.MetadataFSM
}

func newStub() *stubNodeHost {
	return &stubNodeHost{fsm: metadata.NewMetadataFSM()}
}

func (s *stubNodeHost) SyncProposeMetadata(_ context.Context, cmd metadata.MetadataCommand) (sm.Result, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return sm.Result{}, err
	}
	return s.fsm.Update(sm.Entry{Cmd: data})
}

func (s *stubNodeHost) LookupMetadata(_ context.Context, q metadata.MetadataQuery) (interface{}, error) {
	return s.fsm.Lookup(q)
}

func defaultConfig() GroupCoordinatorConfig {
	return GroupCoordinatorConfig{
		SessionTimeoutMinMs: 1000,
		SessionTimeoutMaxMs: 300000,
	}
}

func seedTopic(t *testing.T, stub *stubNodeHost, name string, partCount int32) {
	t.Helper()
	replicas := make([][]uint64, partCount)
	for i := range replicas {
		replicas[i] = []uint64{1}
	}
	result, err := stub.SyncProposeMetadata(context.Background(), metadata.MetadataCommand{
		Type: metadata.CmdCreateTopic,
		CreateTopic: &metadata.CreateTopicCmd{
			Name:              name,
			PartitionCount:    partCount,
			ReplicationFactor: 1,
			RetentionBytes:    -1,
			CreatedAtMs:       time.Now().UnixMilli(),
			ReplicaNodeIDs:    replicas,
		},
	})
	if err != nil {
		t.Fatalf("seed topic %q: %v", name, err)
	}
	if result.Value != metadata.ResultOK {
		t.Fatalf("seed topic %q: FSM error: %s", name, string(result.Data))
	}
}

func joinReq(groupID, memberID string, topics []string) JoinGroupRequest {
	return JoinGroupRequest{
		GroupID:             groupID,
		MemberID:            memberID,
		Topics:              topics,
		SessionTimeoutMs:    5000,
		HeartbeatIntervalMs: 3000,
	}
}

func TestGroupCoordinator_JoinGroup_NewMemberID(t *testing.T) {
	stub := newStub()
	seedTopic(t, stub, "topic-a", 2)
	gc := NewGroupCoordinator(defaultConfig(), stub)

	resp, err := gc.JoinGroup(context.Background(), joinReq("g1", "", []string{"topic-a"}))
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if resp.MemberID == "" {
		t.Fatal("expected non-empty member ID")
	}
}

func TestGroupCoordinator_JoinGroup_ReusesMemberID(t *testing.T) {
	stub := newStub()
	seedTopic(t, stub, "topic-a", 2)
	gc := NewGroupCoordinator(defaultConfig(), stub)

	const want = "my-member-id"
	resp, err := gc.JoinGroup(context.Background(), joinReq("g1", want, []string{"topic-a"}))
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if resp.MemberID != want {
		t.Fatalf("got member_id %q, want %q", resp.MemberID, want)
	}
}

func TestGroupCoordinator_JoinGroup_UnknownTopic(t *testing.T) {
	stub := newStub()
	gc := NewGroupCoordinator(defaultConfig(), stub)

	_, err := gc.JoinGroup(context.Background(), joinReq("g1", "", []string{"does-not-exist"}))
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGroupCoordinator_JoinGroup_MixedSubscriptions(t *testing.T) {
	stub := newStub()
	seedTopic(t, stub, "topic-a", 2)
	seedTopic(t, stub, "topic-b", 2)
	gc := NewGroupCoordinator(defaultConfig(), stub)

	if _, err := gc.JoinGroup(context.Background(), joinReq("g1", "m1", []string{"topic-a"})); err != nil {
		t.Fatalf("first JoinGroup: %v", err)
	}

	_, err := gc.JoinGroup(context.Background(), joinReq("g1", "m2", []string{"topic-b"}))
	if err == nil {
		t.Fatal("expected error for mixed subscriptions")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestGroupCoordinator_JoinGroup_GenerationIncrements(t *testing.T) {
	stub := newStub()
	seedTopic(t, stub, "topic-a", 4)
	gc := NewGroupCoordinator(defaultConfig(), stub)

	resp1, err := gc.JoinGroup(context.Background(), joinReq("g1", "m1", []string{"topic-a"}))
	if err != nil {
		t.Fatalf("first JoinGroup: %v", err)
	}
	if resp1.GenerationID != 1 {
		t.Fatalf("want generation 1, got %d", resp1.GenerationID)
	}

	resp2, err := gc.JoinGroup(context.Background(), joinReq("g1", "m2", []string{"topic-a"}))
	if err != nil {
		t.Fatalf("second JoinGroup: %v", err)
	}
	if resp2.GenerationID != 2 {
		t.Fatalf("want generation 2, got %d", resp2.GenerationID)
	}
}

func TestGroupCoordinator_JoinGroup_AssignmentCoverage(t *testing.T) {
	stub := newStub()
	seedTopic(t, stub, "topic-a", 4)
	gc := NewGroupCoordinator(defaultConfig(), stub)

	if _, err := gc.JoinGroup(context.Background(), joinReq("g1", "m1", []string{"topic-a"})); err != nil {
		t.Fatalf("first JoinGroup: %v", err)
	}

	resp2, err := gc.JoinGroup(context.Background(), joinReq("g1", "m2", []string{"topic-a"}))
	if err != nil {
		t.Fatalf("second JoinGroup: %v", err)
	}
	if len(resp2.Assignments) != 2 {
		t.Fatalf("want 2 partitions for second member, got %d", len(resp2.Assignments))
	}
}

func TestGroupCoordinator_LeaveGroup_NotMember(t *testing.T) {
	stub := newStub()
	seedTopic(t, stub, "topic-a", 2)
	gc := NewGroupCoordinator(defaultConfig(), stub)

	err := gc.LeaveGroup(context.Background(), LeaveGroupRequest{GroupID: "g1", MemberID: "nobody"})
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if !errors.Is(err, ErrNotGroupMember) {
		t.Fatalf("expected ErrNotGroupMember, got %v", err)
	}
}

func TestGroupCoordinator_LeaveGroup_RemovesFromHeartbeat(t *testing.T) {
	stub := newStub()
	seedTopic(t, stub, "topic-a", 2)
	gc := NewGroupCoordinator(defaultConfig(), stub)

	if _, err := gc.JoinGroup(context.Background(), joinReq("g1", "m1", []string{"topic-a"})); err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}

	gc.heartbeatMu.RLock()
	_, present := gc.lastHeartbeat["g1"]["m1"]
	gc.heartbeatMu.RUnlock()
	if !present {
		t.Fatal("expected lastHeartbeat entry after JoinGroup")
	}

	if err := gc.LeaveGroup(context.Background(), LeaveGroupRequest{GroupID: "g1", MemberID: "m1"}); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}

	gc.heartbeatMu.RLock()
	_, stillPresent := gc.lastHeartbeat["g1"]["m1"]
	gc.heartbeatMu.RUnlock()
	if stillPresent {
		t.Fatal("expected lastHeartbeat entry to be removed after LeaveGroup")
	}
}
