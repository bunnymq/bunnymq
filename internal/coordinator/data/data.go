package data

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
	stor "github.com/bunnymq/bunnymq/internal/storage"
)

var (
	ErrPartitionNotFound = errors.New("partition not found")
	ErrUnavailable       = errors.New("unavailable")
	ErrOffsetNotFound    = errors.New("offset not found")
	ErrOffsetOutOfRange  = errors.New("offset out of range")
)

// AcksMode controls produce acknowledgement semantics.
type AcksMode int8

const (
	AcksAll  AcksMode = -1 // SyncPropose; returns assigned offset
	AcksZero AcksMode = 0  // Propose (async); returns -1
)

// NotLeaderError is returned when this node is not the partition leader.
type NotLeaderError struct {
	LeaderNodeID  uint64
	LeaderAddress string
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not leader: leader is node %d at %s", e.LeaderNodeID, e.LeaderAddress)
}

// DataCoordinatorIface is the full interface for DataCoordinator, enabling test
// doubles in the data API, cluster coordinator, and other dependents.
type DataCoordinatorIface interface {
	Produce(ctx context.Context, topic string, partitionID int32, batch []byte, acks AcksMode) (int64, error)
	Fetch(ctx context.Context, topic string, partitionID int32, offset int64, maxBytes int, maxWaitMs int64) ([]byte, int64, error)
	GetEarliestOffset(ctx context.Context, topic string, partitionID int32) (int64, error)
	GetLatestOffset(ctx context.Context, topic string, partitionID int32) (int64, error)
	GetOffsetByTimestamp(ctx context.Context, topic string, partitionID int32, timestampMs int64) (int64, error)
	StartPartitionReplica(topic string, partitionID int32, shardID uint64)
	StopPartitionReplica(topic string, partitionID int32, shardID uint64)
}

// raftHostIface is the subset of raft.Host used by DataCoordinator.
type raftHostIface interface {
	LookupMetadata(ctx context.Context, q metadata.MetadataQuery) (any, error)
	LookupPartition(ctx context.Context, shardID uint64, q partition.PartitionQuery) (any, error)
	SyncProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) (sm.Result, error)
	ProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) error
}

type partitionKey struct {
	Topic       string
	PartitionID int32
}

type shardEntry struct {
	ShardID     uint64
	Topic       string
	PartitionID int32
}

type nodeAddrEntry struct {
	address   string
	fetchedAt time.Time
}

// DataCoordinatorConfig holds configuration for DataCoordinator.
type DataCoordinatorConfig struct {
	NodeID               uint64
	NodeAddressCacheTTLMs int64
}

// DataCoordinator routes produce and fetch requests to the correct partition
// Raft shard on this node. All methods are safe to call concurrently.
type DataCoordinator struct {
	config        DataCoordinatorConfig
	raftHost      raftHostIface
	registryMu    sync.RWMutex
	shardRegistry map[partitionKey]shardEntry
	addrCacheMu   sync.Mutex
	nodeAddrCache map[uint64]nodeAddrEntry
	logger        *zap.Logger
}

// NewDataCoordinator creates a new DataCoordinator.
func NewDataCoordinator(cfg DataCoordinatorConfig, host raftHostIface, logger *zap.Logger) *DataCoordinator {
	return &DataCoordinator{
		config:        cfg,
		raftHost:      host,
		shardRegistry: make(map[partitionKey]shardEntry),
		nodeAddrCache: make(map[uint64]nodeAddrEntry),
		logger:        logger,
	}
}

// StartPartitionReplica registers a partition shard as available for routing.
// Called by ClusterCoordinator after raftHost.StartPartitionShard succeeds.
func (dc *DataCoordinator) StartPartitionReplica(topic string, partitionID int32, shardID uint64) {
	k := partitionKey{Topic: topic, PartitionID: partitionID}
	dc.registryMu.Lock()
	dc.shardRegistry[k] = shardEntry{ShardID: shardID, Topic: topic, PartitionID: partitionID}
	dc.registryMu.Unlock()
}

// StopPartitionReplica removes a partition shard from the routing registry.
// Called by ClusterCoordinator before raftHost.StopPartitionShard.
func (dc *DataCoordinator) StopPartitionReplica(topic string, partitionID int32, shardID uint64) {
	k := partitionKey{Topic: topic, PartitionID: partitionID}
	dc.registryMu.Lock()
	delete(dc.shardRegistry, k)
	dc.registryMu.Unlock()
}

// leaderCheck verifies this node is the current partition leader and returns the shard ID.
func (dc *DataCoordinator) leaderCheck(ctx context.Context, topic string, partitionID int32) (uint64, error) {
	result, err := dc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type: metadata.QueryGetPartition, TopicName: topic, PartitionID: partitionID,
	})
	if err != nil {
		return 0, ErrUnavailable
	}
	if result == nil {
		return 0, ErrPartitionNotFound
	}
	pm := result.(*metadata.PartitionMeta)

	if pm.LeaderNodeID != dc.config.NodeID {
		addr := dc.nodeAddress(ctx, pm.LeaderNodeID)
		return 0, &NotLeaderError{LeaderNodeID: pm.LeaderNodeID, LeaderAddress: addr}
	}

	dc.registryMu.RLock()
	entry, ok := dc.shardRegistry[partitionKey{topic, partitionID}]
	dc.registryMu.RUnlock()
	if !ok {
		return 0, ErrUnavailable
	}
	return entry.ShardID, nil
}

// nodeAddress returns the data API address for a node, using a TTL cache to
// avoid hitting the Metadata FSM on every non-leader request.
func (dc *DataCoordinator) nodeAddress(ctx context.Context, nodeID uint64) string {
	ttl := time.Duration(dc.config.NodeAddressCacheTTLMs) * time.Millisecond

	dc.addrCacheMu.Lock()
	if entry, ok := dc.nodeAddrCache[nodeID]; ok {
		if ttl <= 0 || time.Since(entry.fetchedAt) < ttl {
			dc.addrCacheMu.Unlock()
			return entry.address
		}
	}
	dc.addrCacheMu.Unlock()

	result, err := dc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type: metadata.QueryListNodes,
	})
	if err != nil {
		return ""
	}
	nodes, ok := result.([]*metadata.NodeInfo)
	if !ok {
		return ""
	}

	var address string
	for _, n := range nodes {
		if n.NodeID == nodeID {
			address = n.Address
			break
		}
	}

	dc.addrCacheMu.Lock()
	dc.nodeAddrCache[nodeID] = nodeAddrEntry{address: address, fetchedAt: time.Now()}
	dc.addrCacheMu.Unlock()

	return address
}

// Produce appends batch to the partition identified by (topic, partitionID).
// acks=AcksAll blocks until quorum commit and returns the assigned base_offset.
// acks=AcksZero fires and forgets, returning -1.
func (dc *DataCoordinator) Produce(
	ctx context.Context,
	topic string,
	partitionID int32,
	batch []byte,
	acks AcksMode,
) (int64, error) {
	shardID, err := dc.leaderCheck(ctx, topic, partitionID)
	if err != nil {
		return -1, err
	}

	cmd := partition.PartitionCommand{Type: partition.CmdAppendBatch, Payload: batch}

	if acks == AcksAll {
		result, err := dc.raftHost.SyncProposePartition(ctx, shardID, cmd)
		if err != nil {
			return -1, err
		}
		return int64(result.Value), nil
	}

	if err := dc.raftHost.ProposePartition(ctx, shardID, cmd); err != nil {
		return -1, err
	}
	return -1, nil
}

// Fetch returns up to maxBytes of serialised batch data starting at offset.
// If no records are available and maxWaitMs > 0, blocks until records arrive,
// the deadline elapses, a leader change is detected, or ctx is cancelled.
func (dc *DataCoordinator) Fetch(
	ctx context.Context,
	topic string,
	partitionID int32,
	offset int64,
	maxBytes int,
	maxWaitMs int64,
) ([]byte, int64, error) {
	shardID, err := dc.leaderCheck(ctx, topic, partitionID)
	if err != nil {
		return nil, 0, err
	}

	if maxWaitMs == 0 {
		result, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{
			Type: partition.QueryRead, Offset: offset, MaxBytes: maxBytes,
		})
		if err != nil {
			return nil, 0, err
		}
		r := result.(partition.PartitionLookupResult)
		return r.Batches, r.NextOffset, nil
	}

	return dc.fetchWithLongPoll(ctx, topic, partitionID, shardID, offset, maxBytes, maxWaitMs)
}

func (dc *DataCoordinator) fetchWithLongPoll(
	ctx context.Context,
	topic string,
	partitionID int32,
	shardID uint64,
	offset int64,
	maxBytes int,
	maxWaitMs int64,
) ([]byte, int64, error) {
	deadline := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, 0, nil
		}

		// Snapshot newDataCh BEFORE reading to eliminate the race where a batch
		// arrives between the Read and the channel snapshot.
		chResult, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{
			Type: partition.QueryGetNewDataCh,
		})
		if err != nil {
			return nil, 0, err
		}
		ch := chResult.(<-chan struct{})

		// Re-verify leadership on each iteration — leader may change during a wait.
		metaResult, err := dc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
			Type: metadata.QueryGetPartition, TopicName: topic, PartitionID: partitionID,
		})
		if err != nil || metaResult == nil {
			return nil, 0, ErrUnavailable
		}
		pm := metaResult.(*metadata.PartitionMeta)
		if pm.LeaderNodeID != dc.config.NodeID {
			addr := dc.nodeAddress(ctx, pm.LeaderNodeID)
			return nil, 0, &NotLeaderError{LeaderNodeID: pm.LeaderNodeID, LeaderAddress: addr}
		}

		readResult, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{
			Type: partition.QueryRead, Offset: offset, MaxBytes: maxBytes,
		})
		if err != nil {
			return nil, 0, err
		}
		r := readResult.(partition.PartitionLookupResult)
		if len(r.Batches) > 0 {
			return r.Batches, r.NextOffset, nil
		}

		select {
		case <-ch:
			continue
		case <-time.After(remaining):
			return nil, 0, nil
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
}

// GetEarliestOffset returns the base_offset of the oldest available batch.
func (dc *DataCoordinator) GetEarliestOffset(ctx context.Context, topic string, partitionID int32) (int64, error) {
	shardID, err := dc.leaderCheck(ctx, topic, partitionID)
	if err != nil {
		return 0, err
	}
	result, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{
		Type: partition.QueryEarliestOffset,
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

// GetLatestOffset returns the next offset to be assigned (one past the last written record).
func (dc *DataCoordinator) GetLatestOffset(ctx context.Context, topic string, partitionID int32) (int64, error) {
	shardID, err := dc.leaderCheck(ctx, topic, partitionID)
	if err != nil {
		return 0, err
	}
	result, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{
		Type: partition.QueryLatestOffset,
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

// GetOffsetByTimestamp returns the base_offset of the first batch whose
// max_timestamp >= timestampMs. Returns ErrOffsetNotFound when no such batch exists.
func (dc *DataCoordinator) GetOffsetByTimestamp(
	ctx context.Context,
	topic string,
	partitionID int32,
	timestampMs int64,
) (int64, error) {
	shardID, err := dc.leaderCheck(ctx, topic, partitionID)
	if err != nil {
		return 0, err
	}
	result, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{
		Type:        partition.QueryReadByTime,
		TimestampMs: timestampMs,
		MaxBytes:    512,
	})
	if errors.Is(err, stor.ErrTimestampNotFound) {
		return 0, ErrOffsetNotFound
	}
	if err != nil {
		return 0, err
	}
	r := result.(partition.PartitionLookupResult)
	if len(r.Batches) < 8 {
		return 0, ErrOffsetNotFound
	}
	return int64(binary.BigEndian.Uint64(r.Batches[0:8])), nil
}

// PartitionOffsets returns the earliest and latest offsets for the given shard
// using a stale (non-linearizable) read that works from any replica node.
// Returns -1, -1 if the shard is not running on this node.
func (dc *DataCoordinator) PartitionOffsets(ctx context.Context, shardID uint64) (earliest, latest int64, err error) {
	earlyRaw, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{Type: partition.QueryEarliestOffset})
	if err != nil {
		return -1, -1, err
	}
	latestRaw, err := dc.raftHost.LookupPartition(ctx, shardID, partition.PartitionQuery{Type: partition.QueryLatestOffset})
	if err != nil {
		return -1, -1, err
	}
	return earlyRaw.(int64), latestRaw.(int64), nil
}
