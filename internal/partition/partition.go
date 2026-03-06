package partition

import (
	"io"
	"sync/atomic"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/storage"
)

// PartitionFSM implements dragonboat's IOnDiskStateMachine for a single partition shard.
// It is a thin adapter: its state IS the Storage instance.
type PartitionFSM struct {
	stor             storage.Storage
	lastAppliedIndex atomic.Uint64
	sidecarPath      string
}

var _ sm.IOnDiskStateMachine = (*PartitionFSM)(nil)

func (fsm *PartitionFSM) Open(stopc <-chan struct{}) (uint64, error) {
	return 0, nil
}

func (fsm *PartitionFSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
	return entries, nil
}

func (fsm *PartitionFSM) Lookup(q interface{}) (interface{}, error) {
	return nil, nil
}

func (fsm *PartitionFSM) PrepareSnapshot() (interface{}, error) {
	return nil, nil
}

func (fsm *PartitionFSM) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	return nil
}

func (fsm *PartitionFSM) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	return nil
}

func (fsm *PartitionFSM) Sync() error {
	return nil
}

func (fsm *PartitionFSM) Close() error {
	return nil
}
