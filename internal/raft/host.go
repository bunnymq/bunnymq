package raft

import (
	"context"
	"encoding/json"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dbconfig "github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
)

const (
	metadataShardID      = uint64(0)
	defaultProposeTimeout = 5 * time.Second
)

// Config holds the configuration needed to create a Host.
type Config struct {
	DataDir     string
	RaftAddress string
	NodeID      uint64
	RaftRTTMs   uint64
	Peers       map[uint64]string
}

// nodeHostIface allows injecting a fake NodeHost in unit tests.
type nodeHostIface interface {
	SyncPropose(ctx context.Context, session *client.Session, cmd []byte) (sm.Result, error)
	Propose(session *client.Session, cmd []byte, timeout time.Duration) (*dragonboat.RequestState, error)
	StaleRead(shardID uint64, query interface{}) (interface{}, error)
	GetNoOPSession(shardID uint64) *client.Session
	StartReplica(initialMembers map[uint64]string, join bool, create sm.CreateStateMachineFunc, cfg dbconfig.Config) error
	StartOnDiskReplica(initialMembers map[uint64]string, join bool, create sm.CreateOnDiskStateMachineFunc, cfg dbconfig.Config) error
	StopShard(shardID uint64) error
	Close()
}

// Host wraps dragonboat's NodeHost and exposes typed helpers for metadata and
// partition shard operations. Other modules never import dragonboat directly.
type Host struct {
	nh     nodeHostIface
	config *Config
}

// NewHost creates a Host by initialising a dragonboat NodeHost with the given config.
func NewHost(cfg *Config) (*Host, error) {
	nhc := dbconfig.NodeHostConfig{
		DeploymentID:   1,
		WALDir:         cfg.DataDir + "/wal",
		NodeHostDir:    cfg.DataDir + "/raft",
		RTTMillisecond: cfg.RaftRTTMs,
		RaftAddress:    cfg.RaftAddress,
		EnableMetrics:  true,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	return &Host{nh: nh, config: cfg}, nil
}

// StartMetadataShard starts the metadata Raft shard (shard 0) on this node.
func (h *Host) StartMetadataShard(initialMembers map[uint64]string, join bool, factory sm.CreateStateMachineFunc) error {
	rc := h.shardConfig(metadataShardID)
	return h.nh.StartReplica(initialMembers, join, factory, rc)
}

// StartPartitionShard starts a partition Raft shard on this node.
func (h *Host) StartPartitionShard(shardID uint64, initialMembers map[uint64]string, join bool, factory sm.CreateOnDiskStateMachineFunc) error {
	rc := h.shardConfig(shardID)
	return h.nh.StartOnDiskReplica(initialMembers, join, factory, rc)
}

// StopPartitionShard stops a partition Raft shard on this node.
func (h *Host) StopPartitionShard(shardID uint64) error {
	return h.nh.StopShard(shardID)
}

// Close shuts down the NodeHost and all hosted shards.
func (h *Host) Close() error {
	h.nh.Close()
	return nil
}

// SyncProposeMetadata proposes a metadata command and blocks until quorum commit.
func (h *Host) SyncProposeMetadata(ctx context.Context, cmd metadata.MetadataCommand) (sm.Result, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return sm.Result{}, err
	}
	session := h.nh.GetNoOPSession(metadataShardID)
	return h.nh.SyncPropose(ctx, session, data)
}

// ProposeMetadata proposes a metadata command with acks=0 (fire and forget).
func (h *Host) ProposeMetadata(ctx context.Context, cmd metadata.MetadataCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	session := h.nh.GetNoOPSession(metadataShardID)
	rs, err := h.nh.Propose(session, data, proposeTimeout(ctx))
	if err != nil {
		return err
	}
	if rs != nil {
		go func() {
			<-rs.ResultC()
			rs.Release()
		}()
	}
	return nil
}

// LookupMetadata queries the local Metadata FSM without a Raft round-trip.
func (h *Host) LookupMetadata(ctx context.Context, q metadata.MetadataQuery) (interface{}, error) {
	return h.nh.StaleRead(metadataShardID, q)
}

// SyncProposePartition proposes a partition command and blocks until quorum commit.
func (h *Host) SyncProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) (sm.Result, error) {
	session := h.nh.GetNoOPSession(shardID)
	return h.nh.SyncPropose(ctx, session, cmd.Marshal())
}

// ProposePartition proposes a partition command with acks=0 (fire and forget).
func (h *Host) ProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) error {
	session := h.nh.GetNoOPSession(shardID)
	rs, err := h.nh.Propose(session, cmd.Marshal(), proposeTimeout(ctx))
	if err != nil {
		return err
	}
	if rs != nil {
		go func() {
			<-rs.ResultC()
			rs.Release()
		}()
	}
	return nil
}

// LookupPartition queries the local Partition FSM without a Raft round-trip.
func (h *Host) LookupPartition(ctx context.Context, shardID uint64, q partition.PartitionQuery) (interface{}, error) {
	return h.nh.StaleRead(shardID, q)
}

// shardConfig builds the per-shard Raft config using the values from the design spec.
func (h *Host) shardConfig(shardID uint64) dbconfig.Config {
	return dbconfig.Config{
		ReplicaID:          h.config.NodeID,
		ShardID:            shardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		MaxInMemLogSize:    32 << 20,
		SnapshotEntries:    1 << 62,
		CompactionOverhead: 1 << 62,
	}
}

// proposeTimeout returns the remaining context deadline, or defaultProposeTimeout
// if no deadline is set.
func proposeTimeout(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return defaultProposeTimeout
	}
	if rem := time.Until(deadline); rem > 0 {
		return rem
	}
	return time.Millisecond
}
