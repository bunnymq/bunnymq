package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"

	cmetrics "github.com/bunnymq/bunnymq/internal/cluster"
	"github.com/bunnymq/bunnymq/internal/metadata"
)

// --- metric read helpers ---

func histSampleCount(reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if dtoLabelsMatch(m.GetLabel(), labels) {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func counterValue(reg *prometheus.Registry, name string, labels map[string]string) float64 {
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if dtoLabelsMatch(m.GetLabel(), labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func gaugeValue(reg *prometheus.Registry, name string, labels map[string]string) float64 {
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if dtoLabelsMatch(m.GetLabel(), labels) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func dtoLabelsMatch(metricLabels []*dto.LabelPair, want map[string]string) bool {
	matched := 0
	for _, lp := range metricLabels {
		if v, ok := want[lp.GetName()]; ok {
			if v != lp.GetValue() {
				return false
			}
			matched++
		}
	}
	return matched == len(want)
}

// --- tests ---

// TestRaftMetrics_ProposeDuration_All verifies that SyncProposeMetadata via the
// ClusterCoordinator wrapper observes propose_duration_seconds{acks="all"} once.
func TestRaftMetrics_ProposeDuration_All(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := cmetrics.NewRaftMetrics(reg)

	nodes := []*metadata.NodeInfo{{NodeID: 1, Address: "x"}}
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			if q.Type == metadata.QueryListNodes {
				return nodes, nil
			}
			return nil, errors.New("unexpected")
		},
	}
	cc := NewClusterCoordinator(CoordinatorConfig{
		NodeID: 1,
		Peers:  map[uint64]string{1: "x"},
	}, host, &stubDataCoord{}, m, zap.NewNop())

	// CreateTopic issues exactly one SyncProposeMetadata call.
	_, _ = cc.CreateTopic(context.Background(), "t", 1, 1, TopicConfigOverrides{})

	got := histSampleCount(reg, "bunnymq_raft_propose_duration_seconds",
		map[string]string{"shard_id": "0", "acks": "all"})
	if got != 1 {
		t.Errorf("propose_duration_seconds{acks=all} count: got %d, want 1", got)
	}
}

// TestRaftMetrics_ProposeDuration_Zero verifies that ProposePartition via the
// ClusterCoordinator wrapper observes propose_duration_seconds{acks="zero"} once.
func TestRaftMetrics_ProposeDuration_Zero(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := cmetrics.NewRaftMetrics(reg)

	partitions := []*metadata.PartitionMeta{
		{Topic: "t", PartitionID: 0, ShardID: 10},
	}
	host := &stubRaftHost{
		lookupFn: func(q metadata.MetadataQuery) (any, error) {
			switch q.Type {
			case metadata.QueryGetTopic:
				return &metadata.TopicMeta{Name: "t"}, nil
			case metadata.QueryGetPartitions:
				return partitions, nil
			}
			return nil, errors.New("unexpected")
		},
	}
	cc := NewClusterCoordinator(CoordinatorConfig{
		NodeID: 1,
		Peers:  map[uint64]string{1: "x"},
	}, host, &stubDataCoord{}, m, zap.NewNop())

	// AlterTopicRetention fires ProposePartition (acks=0) for each partition shard.
	_ = cc.AlterTopicRetention(context.Background(), "t", 1000, 2000)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if histSampleCount(reg, "bunnymq_raft_propose_duration_seconds",
			map[string]string{"shard_id": "10", "acks": "zero"}) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	got := histSampleCount(reg, "bunnymq_raft_propose_duration_seconds",
		map[string]string{"shard_id": "10", "acks": "zero"})
	if got != 1 {
		t.Errorf("propose_duration_seconds{acks=zero} count: got %d, want 1", got)
	}
}

// TestRaftMetrics_LeaderChange verifies that sweepLeaders increments
// leader_changes_total and sets is_leader when a leadership change is detected.
func TestRaftMetrics_LeaderChange(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := cmetrics.NewRaftMetrics(reg)

	host := &stubRaftHost{
		getLeaderIDFn: func(_ uint64) (uint64, uint64, bool, error) {
			return 1, 5, true, nil // leaderID=1 == cc.config.NodeID → is_leader=1
		},
	}
	dc := &stubDataCoord{}
	cc := NewClusterCoordinator(CoordinatorConfig{
		NodeID: 1,
		Peers:  map[uint64]string{1: "x"},
	}, host, dc, m, zap.NewNop())

	cc.shardMu.Lock()
	cc.runningShards[10] = shardInfo{Topic: "t", PartitionID: 0, ShardID: 10, Peers: map[uint64]string{1: "x"}}
	cc.shardMu.Unlock()

	cc.sweepLeaders(context.Background())

	got := counterValue(reg, "bunnymq_raft_leader_changes_total", map[string]string{"shard_id": "10"})
	if got != 1 {
		t.Errorf("leader_changes_total: got %v, want 1", got)
	}

	isLeader := gaugeValue(reg, "bunnymq_raft_is_leader", map[string]string{"shard_id": "10"})
	if isLeader != 1 {
		t.Errorf("is_leader: got %v, want 1 (leaderID==NodeID)", isLeader)
	}

	term := gaugeValue(reg, "bunnymq_raft_term", map[string]string{"shard_id": "10"})
	if term != 5 {
		t.Errorf("term: got %v, want 5", term)
	}
}

// TestRaftMetrics_FSMUpdate_Timed verifies that MetadataFSM.Update() observes
// fsm_update_duration_seconds exactly once per call.
func TestRaftMetrics_FSMUpdate_Timed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := cmetrics.NewRaftMetrics(reg)

	fsmMetrics := metadataFSMMetrics(m, "0")
	fsm := metadata.NewMetadataFSM(fsmMetrics)

	cmd := metadata.MetadataCommand{
		Type:         metadata.CmdRegisterNode,
		RegisterNode: &metadata.RegisterNodeCmd{NodeID: 1, Address: "x"},
	}
	data, _ := json.Marshal(cmd)
	if _, err := fsm.Update(sm.Entry{Index: 1, Cmd: data}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := histSampleCount(reg, "bunnymq_raft_fsm_update_duration_seconds",
		map[string]string{"shard_id": "0"})
	if got != 1 {
		t.Errorf("fsm_update_duration_seconds count: got %d, want 1", got)
	}
}

// TestRaftMetrics_Snapshot_Timed verifies that MetadataFSM.SaveSnapshot()
// observes snapshot_save_duration_seconds exactly once.
func TestRaftMetrics_Snapshot_Timed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := cmetrics.NewRaftMetrics(reg)

	fsmMetrics := metadataFSMMetrics(m, "0")
	fsm := metadata.NewMetadataFSM(fsmMetrics)

	var buf bytes.Buffer
	if err := fsm.SaveSnapshot(&buf, nil, make(chan struct{})); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got := histSampleCount(reg, "bunnymq_raft_snapshot_save_duration_seconds",
		map[string]string{"shard_id": "0"})
	if got != 1 {
		t.Errorf("snapshot_save_duration_seconds count: got %d, want 1", got)
	}
}

// TestRaftMetrics_NilSafe verifies that MetadataFSM operations do not panic
// when no metrics are wired (nil MetadataFSMMetrics).
func TestRaftMetrics_NilSafe(t *testing.T) {
	fsm := metadata.NewMetadataFSM()

	cmd := metadata.MetadataCommand{
		Type:         metadata.CmdRegisterNode,
		RegisterNode: &metadata.RegisterNodeCmd{NodeID: 1, Address: "x"},
	}
	data, _ := json.Marshal(cmd)
	if _, err := fsm.Update(sm.Entry{Index: 1, Cmd: data}); err != nil {
		t.Fatalf("Update with nil metrics: %v", err)
	}

	var buf bytes.Buffer
	if err := fsm.SaveSnapshot(&buf, nil, make(chan struct{})); err != nil {
		t.Fatalf("SaveSnapshot with nil metrics: %v", err)
	}

	if err := fsm.RecoverFromSnapshot(&buf, nil, make(chan struct{})); err != nil {
		t.Fatalf("RecoverFromSnapshot with nil metrics: %v", err)
	}
}
