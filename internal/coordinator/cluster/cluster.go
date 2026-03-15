package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"sync"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
)

var (
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrTopicNotFound      = errors.New("topic not found")

	topicNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,255}$`)
)

// CoordinatorConfig holds configuration for a ClusterCoordinator.
type CoordinatorConfig struct {
	NodeID                 uint64
	RaftAddress            string
	DataAddr               string
	DataDir                string
	Peers                  map[uint64]string
	BootstrapTimeoutMs     int64
	ReconcileIntervalMs    int64
	LeaderCheckIntervalMs  int64
	EagerReconcileOnCreate bool
}

// shardInfo records what we know about a running partition shard.
type shardInfo struct {
	Topic       string
	PartitionID int32
	ShardID     uint64
	Peers       map[uint64]string
}

// leaderRecord is the last known leader for a shard.
type leaderRecord struct {
	nodeID uint64
	term   uint64
}

// TopicConfigOverrides carries optional per-topic retention overrides.
// A nil field means "use cluster default from config".
type TopicConfigOverrides struct {
	RetentionMs    *int64
	RetentionBytes *int64
}

// TopicInfo is a summary of a topic.
type TopicInfo struct {
	Name              string
	PartitionCount    int32
	ReplicationFactor int32
	RetentionMs       int64
	RetentionBytes    int64
	CreatedAtMs       int64
}

// PartitionInfo is metadata for a single partition.
type PartitionInfo struct {
	PartitionID    int32
	ShardID        uint64
	LeaderNodeID   uint64
	LeaderEpoch    int64
	ReplicaNodeIDs []uint64
}

// TopicDescription is full metadata for a topic including per-partition details.
type TopicDescription struct {
	TopicInfo
	Partitions []PartitionInfo
}

// ClusterDescription is the current cluster topology.
type ClusterDescription struct {
	Nodes []NodeDescriptor
}

// NodeDescriptor describes a cluster node.
type NodeDescriptor struct {
	NodeID  uint64
	Address string
}

// raftHostIface is the subset of raft.Host used by ClusterCoordinator.
type raftHostIface interface {
	StartMetadataShard(initialMembers map[uint64]string, join bool, factory sm.CreateStateMachineFunc) error
	StartPartitionShard(shardID uint64, initialMembers map[uint64]string, join bool) error
	StopPartitionShard(shardID uint64) error
	GetLeaderID(shardID uint64) (leaderID uint64, term uint64, valid bool, err error)
	SyncProposeMetadata(ctx context.Context, cmd metadata.MetadataCommand) (sm.Result, error)
	LookupMetadata(ctx context.Context, q metadata.MetadataQuery) (interface{}, error)
	ProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) error
}

// DataCoordinatorIface is the subset of DataCoordinator used by ClusterCoordinator.
type DataCoordinatorIface interface {
	StartPartitionReplica(topic string, partitionID int32, shardID uint64)
	StopPartitionReplica(topic string, partitionID int32, shardID uint64)
}

// ClusterCoordinator manages the topic and partition lifecycle for the cluster.
// All methods are safe to call concurrently from multiple gRPC handler goroutines.
type ClusterCoordinator struct {
	config          CoordinatorConfig
	raftHost        raftHostIface
	dataCoord       DataCoordinatorIface
	shardMu         sync.RWMutex
	runningShards   map[uint64]shardInfo
	leaderMu        sync.Mutex
	lastKnownLeader map[uint64]leaderRecord
	logger          *zap.Logger
}

// NewClusterCoordinator creates a new ClusterCoordinator.
func NewClusterCoordinator(cfg CoordinatorConfig, host raftHostIface, dataCoord DataCoordinatorIface, logger *zap.Logger) *ClusterCoordinator {
	return &ClusterCoordinator{
		config:          cfg,
		raftHost:        host,
		dataCoord:       dataCoord,
		runningShards:   make(map[uint64]shardInfo),
		lastKnownLeader: make(map[uint64]leaderRecord),
		logger:          logger,
	}
}

// Bootstrap starts the metadata shard, waits for leader election, registers this
// node, runs the initial reconciliation, and starts background goroutines.
func (cc *ClusterCoordinator) Bootstrap(ctx context.Context) error {
	// Step 1 — start metadata shard.
	// VERIFY: join=false for a fresh cluster; join=true when rejoining.
	factory := func(_ uint64, _ uint64) sm.IStateMachine {
		return metadata.NewMetadataFSM()
	}
	if err := cc.raftHost.StartMetadataShard(cc.config.Peers, false, factory); err != nil {
		return fmt.Errorf("start metadata shard: %w", err)
	}

	// Step 2 — wait for metadata shard leader.
	timeout := time.Duration(cc.config.BootstrapTimeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	for {
		_, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{Type: metadata.QueryListNodes})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("bootstrap: timed out waiting for metadata shard leader after %dms", cc.config.BootstrapTimeoutMs)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Step 3 — register this node with the Data API address so clients can
	// look up the leader's gRPC endpoint via DescribeCluster/DescribeTopic.
	regAddr := cc.config.DataAddr
	if regAddr == "" {
		regAddr = cc.config.RaftAddress
	}
	if _, err := cc.raftHost.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type: metadata.CmdRegisterNode,
		RegisterNode: &metadata.RegisterNodeCmd{
			NodeID:  cc.config.NodeID,
			Address: regAddr,
		},
	}); err != nil {
		return fmt.Errorf("register node: %w", err)
	}

	// Step 4 — run initial partition reconciliation.
	cc.reconcileOnce(ctx)

	// Step 5 — start background goroutines.
	go cc.reconcileLoop(ctx)
	go cc.leaderSweepLoop(ctx)

	return nil
}

// CreateTopic creates a new topic.
func (cc *ClusterCoordinator) CreateTopic(
	ctx context.Context,
	name string,
	partitionCount int32,
	replicationFactor int32,
	configOverrides TopicConfigOverrides,
) (TopicInfo, error) {
	if !topicNameRe.MatchString(name) {
		return TopicInfo{}, fmt.Errorf("%w: topic name %q is invalid", ErrInvalidArgument, name)
	}
	if partitionCount < 1 {
		return TopicInfo{}, fmt.Errorf("%w: partitionCount must be >= 1", ErrInvalidArgument)
	}
	if replicationFactor < 1 {
		return TopicInfo{}, fmt.Errorf("%w: replicationFactor must be >= 1", ErrInvalidArgument)
	}

	nodesRaw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{Type: metadata.QueryListNodes})
	if err != nil {
		return TopicInfo{}, fmt.Errorf("lookup nodes: %w", err)
	}
	nodes := nodesRaw.([]*metadata.NodeInfo)

	if int(replicationFactor) > len(nodes) {
		return TopicInfo{}, fmt.Errorf("%w: replicationFactor %d exceeds cluster size %d", ErrInvalidArgument, replicationFactor, len(nodes))
	}

	replicaNodeIDs := make([][]uint64, partitionCount)
	for p := int32(0); p < partitionCount; p++ {
		replicaNodeIDs[p] = assignReplicas(nodes, name, p, replicationFactor)
	}

	retentionMs := int64(0)
	retentionBytes := int64(-1)
	if configOverrides.RetentionMs != nil {
		retentionMs = *configOverrides.RetentionMs
	}
	if configOverrides.RetentionBytes != nil {
		retentionBytes = *configOverrides.RetentionBytes
	}

	createCmd := &metadata.CreateTopicCmd{
		Name:              name,
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		RetentionMs:       retentionMs,
		RetentionBytes:    retentionBytes,
		CreatedAtMs:       time.Now().UnixMilli(),
		ReplicaNodeIDs:    replicaNodeIDs,
	}
	result, err := cc.raftHost.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type:        metadata.CmdCreateTopic,
		CreateTopic: createCmd,
	})
	if err != nil {
		return TopicInfo{}, fmt.Errorf("propose create topic: %w", err)
	}
	if result.Value != metadata.ResultOK {
		if result.Value == metadata.ResultErrAlreadyExists {
			return TopicInfo{}, ErrTopicAlreadyExists
		}
		return TopicInfo{}, fmt.Errorf("create topic FSM error: %s", string(result.Data))
	}

	info := TopicInfo{
		Name:              name,
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		RetentionMs:       retentionMs,
		RetentionBytes:    retentionBytes,
		CreatedAtMs:       createCmd.CreatedAtMs,
	}

	if cc.config.EagerReconcileOnCreate {
		cc.reconcileOnce(ctx)
	}

	return info, nil
}

// DeleteTopic removes the topic from the Metadata FSM.
func (cc *ClusterCoordinator) DeleteTopic(ctx context.Context, name string) error {
	_, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:      metadata.QueryGetTopic,
		TopicName: name,
	})
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return ErrTopicNotFound
		}
		return fmt.Errorf("lookup topic: %w", err)
	}

	_, err = cc.raftHost.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type:        metadata.CmdDeleteTopic,
		DeleteTopic: &metadata.DeleteTopicCmd{Name: name},
	})
	return err
}

// ListTopics returns a summary of all topics in the cluster.
func (cc *ClusterCoordinator) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	raw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{Type: metadata.QueryListTopics})
	if err != nil {
		return nil, err
	}
	metas := raw.([]*metadata.TopicMeta)
	topics := make([]TopicInfo, len(metas))
	for i, m := range metas {
		topics[i] = topicInfoFromMeta(m)
	}
	return topics, nil
}

// DescribeTopic returns full metadata for a named topic.
func (cc *ClusterCoordinator) DescribeTopic(ctx context.Context, name string) (TopicDescription, error) {
	topicRaw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:      metadata.QueryGetTopic,
		TopicName: name,
	})
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return TopicDescription{}, ErrTopicNotFound
		}
		return TopicDescription{}, fmt.Errorf("lookup topic: %w", err)
	}
	tm := topicRaw.(*metadata.TopicMeta)

	partsRaw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:      metadata.QueryGetPartitions,
		TopicName: name,
	})
	if err != nil {
		return TopicDescription{}, fmt.Errorf("lookup partitions: %w", err)
	}
	pms := partsRaw.([]*metadata.PartitionMeta)

	parts := make([]PartitionInfo, len(pms))
	for i, pm := range pms {
		parts[i] = PartitionInfo{
			PartitionID:    pm.PartitionID,
			ShardID:        pm.ShardID,
			LeaderNodeID:   pm.LeaderNodeID,
			LeaderEpoch:    pm.LeaderEpoch,
			ReplicaNodeIDs: pm.ReplicaNodeIDs,
		}
	}

	return TopicDescription{
		TopicInfo:  topicInfoFromMeta(tm),
		Partitions: parts,
	}, nil
}

// AlterTopicPartitionCount increases the partition count of an existing topic.
func (cc *ClusterCoordinator) AlterTopicPartitionCount(
	ctx context.Context,
	name string,
	newCount int32,
) error {
	topicRaw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:      metadata.QueryGetTopic,
		TopicName: name,
	})
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return ErrTopicNotFound
		}
		return fmt.Errorf("lookup topic: %w", err)
	}
	tm := topicRaw.(*metadata.TopicMeta)

	if newCount <= tm.PartitionCount {
		return fmt.Errorf("%w: newCount %d must be greater than current %d", ErrInvalidArgument, newCount, tm.PartitionCount)
	}

	nodesRaw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{Type: metadata.QueryListNodes})
	if err != nil {
		return fmt.Errorf("lookup nodes: %w", err)
	}
	nodes := nodesRaw.([]*metadata.NodeInfo)

	addCount := newCount - tm.PartitionCount
	newAssignments := make([][]uint64, addCount)
	for i := int32(0); i < addCount; i++ {
		newAssignments[i] = assignReplicas(nodes, name, tm.PartitionCount+i, tm.ReplicationFactor)
	}

	_, err = cc.raftHost.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type: metadata.CmdAlterTopicPartCount,
		AlterTopicPartCount: &metadata.AlterTopicPartCountCmd{
			Name:                  name,
			NewPartitionCount:     newCount,
			NewReplicaAssignments: newAssignments,
		},
	})
	return err
}

// AlterTopicRetention updates the retention configuration for an existing topic.
func (cc *ClusterCoordinator) AlterTopicRetention(
	ctx context.Context,
	name string,
	retentionMs int64,
	retentionBytes int64,
) error {
	_, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:      metadata.QueryGetTopic,
		TopicName: name,
	})
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return ErrTopicNotFound
		}
		return fmt.Errorf("lookup topic: %w", err)
	}

	if _, err = cc.raftHost.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type: metadata.CmdAlterTopicRetention,
		AlterTopicRetention: &metadata.AlterTopicRetentionCmd{
			Name:           name,
			RetentionMs:    retentionMs,
			RetentionBytes: retentionBytes,
		},
	}); err != nil {
		return err
	}

	raw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:      metadata.QueryGetPartitions,
		TopicName: name,
	})
	if err != nil {
		cc.logger.Warn("alter retention: failed to list partitions for shard propagation",
			zap.String("topic", name), zap.Error(err))
		return nil
	}
	pms := raw.([]*metadata.PartitionMeta)

	payload, _ := json.Marshal(partition.RetentionConfigPayload{
		RetentionMs:    retentionMs,
		RetentionBytes: retentionBytes,
	})
	for _, pm := range pms {
		shardID := pm.ShardID
		go func() {
			if propErr := cc.raftHost.ProposePartition(context.Background(), shardID, partition.PartitionCommand{
				Type:    partition.CmdRetentionConfig,
				Payload: payload,
			}); propErr != nil {
				cc.logger.Warn("alter retention: failed to propagate to partition shard",
					zap.String("topic", name), zap.Uint64("shard_id", shardID), zap.Error(propErr))
			}
		}()
	}
	return nil
}

// DescribeCluster returns the current cluster topology.
func (cc *ClusterCoordinator) DescribeCluster(ctx context.Context) (ClusterDescription, error) {
	raw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{Type: metadata.QueryListNodes})
	if err != nil {
		return ClusterDescription{}, err
	}
	nodes := raw.([]*metadata.NodeInfo)
	descs := make([]NodeDescriptor, len(nodes))
	for i, n := range nodes {
		descs[i] = NodeDescriptor{NodeID: n.NodeID, Address: n.Address}
	}
	return ClusterDescription{Nodes: descs}, nil
}

// assignReplicas returns the RF node IDs for one partition using FNV-1a.
// nodes must be sorted by NodeID ascending before calling.
func assignReplicas(nodes []*metadata.NodeInfo, topicName string, partitionID int32, rf int32) []uint64 {
	sorted := make([]*metadata.NodeInfo, len(nodes))
	copy(sorted, nodes)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NodeID < sorted[j].NodeID
	})

	start := int(fnv32a(topicName)) % len(sorted)
	replicas := make([]uint64, rf)
	for r := int32(0); r < rf; r++ {
		replicas[r] = sorted[(start+int(partitionID)+int(r))%len(sorted)].NodeID
	}
	return replicas
}

func fnv32a(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func topicInfoFromMeta(m *metadata.TopicMeta) TopicInfo {
	return TopicInfo{
		Name:              m.Name,
		PartitionCount:    m.PartitionCount,
		ReplicationFactor: m.ReplicationFactor,
		RetentionMs:       m.RetentionMs,
		RetentionBytes:    m.RetentionBytes,
		CreatedAtMs:       m.CreatedAtMs,
	}
}

func (cc *ClusterCoordinator) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(cc.config.ReconcileIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cc.reconcileOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (cc *ClusterCoordinator) reconcileOnce(ctx context.Context) {
	topicsRaw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{Type: metadata.QueryListTopics})
	if err != nil {
		cc.logger.Warn("reconcile: metadata lookup topics failed", zap.Error(err))
		return
	}
	topics := topicsRaw.([]*metadata.TopicMeta)

	expected := make(map[uint64]shardInfo)
	for _, tm := range topics {
		partsRaw, err := cc.raftHost.LookupMetadata(ctx, metadata.MetadataQuery{
			Type:      metadata.QueryGetPartitions,
			TopicName: tm.Name,
		})
		if err != nil {
			cc.logger.Warn("reconcile: metadata lookup partitions failed",
				zap.String("topic", tm.Name), zap.Error(err))
			continue
		}
		for _, pm := range partsRaw.([]*metadata.PartitionMeta) {
			if slices.Contains(pm.ReplicaNodeIDs, cc.config.NodeID) {
				expected[pm.ShardID] = shardInfo{
					Topic:       pm.Topic,
					PartitionID: pm.PartitionID,
					ShardID:     pm.ShardID,
					Peers:       buildPeerMap(pm.ReplicaNodeIDs, cc.config.Peers),
				}
			}
		}
	}

	cc.shardMu.Lock()
	defer cc.shardMu.Unlock()

	for shardID, info := range expected {
		if _, running := cc.runningShards[shardID]; !running {
			go cc.startShard(shardID, info)
		}
	}
	for shardID, info := range cc.runningShards {
		if _, wanted := expected[shardID]; !wanted {
			go cc.stopShard(shardID, info)
		}
	}
}

func (cc *ClusterCoordinator) startShard(shardID uint64, info shardInfo) {
	// Per T-003: in a static cluster all replicas call StartPartitionShard with
	// join=false and the full peer map. dragonboat handles both first-start and
	// restart correctly: on first start it bootstraps the shard; on restart it
	// validates the peer map against saved bootstrap info. join=true is only for
	// dynamic membership changes (RequestAddReplica), which BunnyMQ v1 never does.
	if err := cc.raftHost.StartPartitionShard(shardID, info.Peers, false); err != nil {
		cc.logger.Warn("failed to start partition shard",
			zap.Uint64("shard_id", shardID), zap.Error(err))
		return
	}
	cc.dataCoord.StartPartitionReplica(info.Topic, info.PartitionID, shardID)
	cc.shardMu.Lock()
	cc.runningShards[shardID] = info
	cc.shardMu.Unlock()
	cc.logger.Info("partition shard started",
		zap.String("topic", info.Topic), zap.Int32("partition_id", info.PartitionID))
}

func (cc *ClusterCoordinator) stopShard(shardID uint64, info shardInfo) {
	cc.dataCoord.StopPartitionReplica(info.Topic, info.PartitionID, shardID)
	if err := cc.raftHost.StopPartitionShard(shardID); err != nil {
		cc.logger.Warn("failed to stop partition shard",
			zap.Uint64("shard_id", shardID), zap.Error(err))
	}
	cc.shardMu.Lock()
	delete(cc.runningShards, shardID)
	cc.shardMu.Unlock()
	dir := partitionDir(cc.config.DataDir, info.Topic, info.PartitionID)
	if err := os.RemoveAll(dir); err != nil {
		cc.logger.Warn("failed to delete partition storage directory",
			zap.String("dir", dir), zap.Error(err))
	}
	cc.logger.Info("partition shard stopped and storage removed",
		zap.String("topic", info.Topic), zap.Int32("partition_id", info.PartitionID))
}

func (cc *ClusterCoordinator) leaderSweepLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(cc.config.LeaderCheckIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cc.sweepLeaders(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (cc *ClusterCoordinator) sweepLeaders(ctx context.Context) {
	cc.shardMu.RLock()
	shards := maps.Clone(cc.runningShards)
	cc.shardMu.RUnlock()

	for shardID, info := range shards {
		leaderID, term, valid, err := cc.raftHost.GetLeaderID(shardID)
		if err != nil {
			cc.logger.Warn("GetLeaderID failed",
				zap.Uint64("shard_id", shardID), zap.Error(err))
			continue
		}
		if !valid {
			continue
		}
		cc.leaderMu.Lock()
		last := cc.lastKnownLeader[shardID]
		cc.leaderMu.Unlock()
		if leaderID == last.nodeID && term == last.term {
			continue
		}
		_, err = cc.raftHost.SyncProposeMetadata(ctx, metadata.MetadataCommand{
			Type: metadata.CmdAssignPartitionLeader,
			AssignPartitionLeader: &metadata.AssignPartitionLeaderCmd{
				Topic:        info.Topic,
				PartitionID:  info.PartitionID,
				LeaderNodeID: leaderID,
				LeaderEpoch:  int64(term),
			},
		})
		if err != nil {
			cc.logger.Warn("failed to update partition leader in metadata",
				zap.String("topic", info.Topic), zap.Int32("partition_id", info.PartitionID),
				zap.Uint64("leader_id", leaderID), zap.Error(err))
			continue
		}
		cc.leaderMu.Lock()
		cc.lastKnownLeader[shardID] = leaderRecord{nodeID: leaderID, term: term}
		cc.leaderMu.Unlock()
	}
}

// partitionDir returns the storage directory for a partition.
func partitionDir(dataDir, topic string, partitionID int32) string {
	return filepath.Join(dataDir, "partitions", topic, fmt.Sprintf("%d", partitionID))
}

// buildPeerMap builds a nodeID→raftAddress map for the given replica node IDs
// using the static Raft peer config. The cluster is static in v1, so Raft
// addresses are known at startup and do not need a metadata lookup.
func buildPeerMap(replicaIDs []uint64, raftPeers map[uint64]string) map[uint64]string {
	peers := make(map[uint64]string, len(replicaIDs))
	for _, id := range replicaIDs {
		if addr, ok := raftPeers[id]; ok {
			peers[id] = addr
		}
	}
	return peers
}
