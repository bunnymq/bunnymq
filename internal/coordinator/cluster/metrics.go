package cluster

import (
	cmetrics "github.com/bunnymq/bunnymq/internal/cluster"
	"github.com/bunnymq/bunnymq/internal/metadata"
)

// metadataFSMMetrics extracts per-shard labeled observers from m for use in a
// MetadataFSM. Fields in m that are nil (e.g. from NoopRaftMetrics) produce nil
// observers, which the FSM silently ignores.
func metadataFSMMetrics(m *cmetrics.RaftMetrics, shardLabel string) *metadata.MetadataFSMMetrics {
	out := &metadata.MetadataFSMMetrics{}
	if m == nil {
		return out
	}
	if m.FSMUpdateDuration != nil {
		out.FSMUpdateDuration = m.FSMUpdateDuration.WithLabelValues(shardLabel)
	}
	if m.SnapshotSaveDuration != nil {
		out.SnapshotSaveDuration = m.SnapshotSaveDuration.WithLabelValues(shardLabel)
	}
	if m.SnapshotRecoverDuration != nil {
		out.SnapshotRecoverDuration = m.SnapshotRecoverDuration.WithLabelValues(shardLabel)
	}
	if m.CommittedIndex != nil {
		out.CommittedIndex = m.CommittedIndex.WithLabelValues(shardLabel)
	}
	if m.AppliedIndex != nil {
		out.AppliedIndex = m.AppliedIndex.WithLabelValues(shardLabel)
	}
	return out
}
