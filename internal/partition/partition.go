package partition

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/config"
	stor "github.com/bunnymq/bunnymq/internal/storage"
)

// PartitionFSM implements dragonboat's IOnDiskStateMachine for a single partition shard.
// It is a thin adapter: its state IS the Storage instance.
type PartitionFSM struct {
	storage          stor.Storage
	lastAppliedIndex atomic.Uint64
	closed           atomic.Bool
	sidecarPath      string
	dir              string
	cfg              *config.StorageConfig
}

var _ sm.IOnDiskStateMachine = (*PartitionFSM)(nil)

// NewPartitionFSM constructs a PartitionFSM that will open storage in dir on Open().
func NewPartitionFSM(dir, sidecarPath string, cfg *config.StorageConfig) *PartitionFSM {
	return &PartitionFSM{
		dir:         dir,
		sidecarPath: sidecarPath,
		cfg:         cfg,
	}
}

func (fsm *PartitionFSM) Open(stopc <-chan struct{}) (uint64, error) {
	s, err := stor.Open(fsm.dir, fsm.cfg)
	if err != nil {
		return 0, err
	}
	fsm.storage = s

	sidecar, err := readSidecar(fsm.sidecarPath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	if fsm.storage.LatestOffset() > sidecar.LatestOffset {
		if err := fsm.storage.TruncateTo(sidecar.LatestOffset); err != nil {
			return 0, err
		}
	}

	fsm.lastAppliedIndex.Store(sidecar.LastAppliedIndex)
	return sidecar.LastAppliedIndex, nil
}

func (fsm *PartitionFSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
	for i := range entries {
		e := &entries[i]
		switch e.Cmd[0] {
		case CmdAppendBatch:
			baseOffset, err := fsm.storage.Append(e.Cmd[1:])
			if err != nil {
				panic(fmt.Sprintf("storage.Append failed: %v", err))
			}
			e.Result = sm.Result{Value: uint64(baseOffset)}
		case CmdRetentionConfig:
			var rc RetentionConfigPayload
			if err := json.Unmarshal(e.Cmd[1:], &rc); err != nil {
				panic(fmt.Sprintf("bad retention config: %v", err))
			}
			fsm.storage.SetRetentionConfig(rc.RetentionMs, rc.RetentionBytes)
			e.Result = sm.Result{Value: 0}
		default:
			panic(fmt.Sprintf("unknown partition command type: %#x", e.Cmd[0]))
		}
	}

	if err := fsm.persistApplied(entries[len(entries)-1].Index); err != nil {
		panic(fmt.Sprintf("persistApplied failed: %v", err))
	}

	fsm.lastAppliedIndex.Store(entries[len(entries)-1].Index)
	return entries, nil
}

func (fsm *PartitionFSM) persistApplied(index uint64) error {
	if err := fsm.storage.Sync(); err != nil {
		return err
	}
	data := encodeSidecar(index, fsm.storage.LatestOffset())
	tmp := fsm.sidecarPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := syncFile(tmp); err != nil {
		return err
	}
	return os.Rename(tmp, fsm.sidecarPath)
}

func (fsm *PartitionFSM) Lookup(query any) (any, error) {
	if fsm.closed.Load() {
		return nil, stor.ErrStorageClosed
	}
	q, ok := query.(PartitionQuery)
	if !ok {
		return nil, fmt.Errorf("partition: unexpected query type %T", query)
	}
	switch q.Type {
	case QueryRead:
		batches, nextOffset, err := fsm.storage.Read(q.Offset, q.MaxBytes)
		if err != nil {
			return nil, err
		}
		return PartitionLookupResult{Batches: batches, NextOffset: nextOffset}, nil
	case QueryReadByTime:
		batches, nextOffset, err := fsm.storage.ReadByTime(q.TimestampMs, q.MaxBytes)
		if err != nil {
			return nil, err
		}
		return PartitionLookupResult{Batches: batches, NextOffset: nextOffset}, nil
	case QueryEarliestOffset:
		return fsm.storage.EarliestOffset(), nil
	case QueryLatestOffset:
		return fsm.storage.LatestOffset(), nil
	default:
		return nil, fmt.Errorf("partition: unknown query type %q", q.Type)
	}
}

func (fsm *PartitionFSM) PrepareSnapshot() (any, error) {
	return nil, nil
}

func (fsm *PartitionFSM) SaveSnapshot(_ any, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write([]byte("strategy-a-noop"))
	return err
}

func (fsm *PartitionFSM) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	_, _ = io.Copy(io.Discard, r)
	return nil
}

func (fsm *PartitionFSM) Sync() error {
	return nil
}

func (fsm *PartitionFSM) Close() error {
	if fsm.storage == nil {
		return nil
	}
	fsm.closed.Store(true)
	return fsm.storage.Close()
}

type sidecarData struct {
	LastAppliedIndex uint64
	LatestOffset     int64
}

func readSidecar(path string) (*sidecarData, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) != 16 {
		return nil, fmt.Errorf("sidecar: unexpected size %d", len(data))
	}
	return &sidecarData{
		LastAppliedIndex: binary.BigEndian.Uint64(data[0:8]),
		LatestOffset:     int64(binary.BigEndian.Uint64(data[8:16])),
	}, nil
}

func encodeSidecar(index uint64, latestOffset int64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], index)
	binary.BigEndian.PutUint64(buf[8:16], uint64(latestOffset))
	return buf
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

