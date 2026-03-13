package metadata

import (
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// MetadataFSM implements dragonboat's IStateMachine for the cluster metadata shard.
// It maintains in-memory state for topics, partitions, nodes, and consumer groups.
type MetadataFSM struct {
	state *MetadataState //nolint:unused
}

var _ sm.IStateMachine = (*MetadataFSM)(nil)

func (fsm *MetadataFSM) Update(e sm.Entry) (sm.Result, error) {
	return sm.Result{}, nil
}

func (fsm *MetadataFSM) Lookup(q interface{}) (interface{}, error) {
	return nil, nil
}

func (fsm *MetadataFSM) SaveSnapshot(w io.Writer, c sm.ISnapshotFileCollection, done <-chan struct{}) error {
	return nil
}

func (fsm *MetadataFSM) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	return nil
}

func (fsm *MetadataFSM) Close() error {
	return nil
}
