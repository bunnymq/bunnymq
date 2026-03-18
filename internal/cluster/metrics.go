package cluster

import "github.com/prometheus/client_golang/prometheus"

var proposeBuckets = []float64{0.0005, 0.001, 0.002, 0.005, 0.010, 0.050, 0.100, 0.500}

// RaftMetrics holds all Prometheus metrics for Raft health monitoring.
// All fields may be nil; use NoopRaftMetrics to get a safe zero-value struct.
type RaftMetrics struct {
	CommittedIndex          *prometheus.GaugeVec
	AppliedIndex            *prometheus.GaugeVec
	IsLeader                *prometheus.GaugeVec
	Term                    *prometheus.GaugeVec
	ProposeDuration         *prometheus.HistogramVec
	SnapshotSaveDuration    *prometheus.HistogramVec
	SnapshotRecoverDuration *prometheus.HistogramVec
	LeaderChangesTotal      *prometheus.CounterVec
	FSMUpdateDuration       *prometheus.HistogramVec
}

// NewRaftMetrics creates and registers all 9 raft metrics with reg.
func NewRaftMetrics(reg prometheus.Registerer) *RaftMetrics {
	m := &RaftMetrics{
		CommittedIndex: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_raft_committed_index",
			Help: "Last committed Raft log index for this shard.",
		}, []string{"shard_id"}),
		AppliedIndex: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_raft_applied_index",
			Help: "Last applied Raft log index for this shard.",
		}, []string{"shard_id"}),
		IsLeader: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_raft_is_leader",
			Help: "1 if this node is the current leader for the shard, 0 otherwise.",
		}, []string{"shard_id"}),
		Term: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bunnymq_raft_term",
			Help: "Current Raft term for this shard.",
		}, []string{"shard_id"}),
		ProposeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_raft_propose_duration_seconds",
			Help:    "Wall time for a propose round-trip; acks label: all (SyncPropose) or zero (Propose).",
			Buckets: proposeBuckets,
		}, []string{"shard_id", "acks"}),
		SnapshotSaveDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_raft_snapshot_save_duration_seconds",
			Help:    "Time to serialize and write a snapshot.",
			Buckets: proposeBuckets,
		}, []string{"shard_id"}),
		SnapshotRecoverDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_raft_snapshot_recover_duration_seconds",
			Help:    "Time to install a received snapshot.",
			Buckets: proposeBuckets,
		}, []string{"shard_id"}),
		LeaderChangesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bunnymq_raft_leader_changes_total",
			Help: "Number of leadership changes observed on this node.",
		}, []string{"shard_id"}),
		FSMUpdateDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bunnymq_raft_fsm_update_duration_seconds",
			Help:    "Time spent inside Update() including Storage.Append.",
			Buckets: proposeBuckets,
		}, []string{"shard_id"}),
	}
	reg.MustRegister(
		m.CommittedIndex,
		m.AppliedIndex,
		m.IsLeader,
		m.Term,
		m.ProposeDuration,
		m.SnapshotSaveDuration,
		m.SnapshotRecoverDuration,
		m.LeaderChangesTotal,
		m.FSMUpdateDuration,
	)
	return m
}

// NoopRaftMetrics returns a RaftMetrics with nil fields. All callers nil-check
// each field before recording, so this is safe to use in tests or when no
// registry is configured.
func NoopRaftMetrics() *RaftMetrics {
	return &RaftMetrics{}
}
