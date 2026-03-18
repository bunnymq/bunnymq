package group

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/metadata"
)

var (
	ErrNotGroupMember  = errors.New("not a group member")
	ErrNotFound        = errors.New("not found")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrStaleGeneration = errors.New("stale generation")
)

// GroupCoordinatorConfig holds configuration for a GroupCoordinator.
type GroupCoordinatorConfig struct {
	MetadataShardID     uint64
	ThisNodeID          uint64
	SessionTimeoutMinMs int32 // default 1000
	SessionTimeoutMaxMs int32 // default 300000
	SweepIntervalMs     int64 // default 5000
}

// nodeHostIface is the subset of raft.Host used by GroupCoordinator.
type nodeHostIface interface {
	SyncProposeMetadata(ctx context.Context, cmd metadata.MetadataCommand) (sm.Result, error)
	LookupMetadata(ctx context.Context, q metadata.MetadataQuery) (interface{}, error)
	GetLeaderID(shardID uint64) (leaderID uint64, term uint64, valid bool, err error)
}

// JoinGroupRequest holds the parameters for a JoinGroup call.
type JoinGroupRequest struct {
	GroupID             string
	MemberID            string
	Topics              []string
	SessionTimeoutMs    int32
	HeartbeatIntervalMs int32
}

// JoinGroupResponse holds the result of a successful JoinGroup.
type JoinGroupResponse struct {
	MemberID     string
	GenerationID int32
	Assignments  []metadata.TopicPartition
}

// LeaveGroupRequest holds the parameters for a LeaveGroup call.
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// GroupCoordinatorIface is the interface for test doubles in downstream packages.
type GroupCoordinatorIface interface {
	JoinGroup(ctx context.Context, req JoinGroupRequest) (JoinGroupResponse, error)
	LeaveGroup(ctx context.Context, req LeaveGroupRequest) error
	Heartbeat(ctx context.Context, groupID, memberID string, generationID int32) (rebalanceRequired bool, err error)
	CommitOffset(ctx context.Context, groupID, memberID string, generationID int32, offsets map[metadata.TopicPartition]int64) error
	FetchCommittedOffsets(ctx context.Context, groupID string, partitions []metadata.TopicPartition) (map[metadata.TopicPartition]int64, error)
}

// GroupCoordinator manages consumer group membership and partition assignment.
// All writes are serialised by the metadata shard's Raft leader guarantee.
type GroupCoordinator struct {
	config        GroupCoordinatorConfig
	nh            nodeHostIface
	logger        *zap.Logger
	heartbeatMu   sync.RWMutex
	lastHeartbeat map[string]map[string]time.Time // group → member → last heartbeat time
}

// NewGroupCoordinator creates a new GroupCoordinator.
func NewGroupCoordinator(config GroupCoordinatorConfig, nh nodeHostIface, logger *zap.Logger) *GroupCoordinator {
	if config.SessionTimeoutMinMs == 0 {
		config.SessionTimeoutMinMs = 1000
	}
	if config.SessionTimeoutMaxMs == 0 {
		config.SessionTimeoutMaxMs = 300000
	}
	if config.SweepIntervalMs == 0 {
		config.SweepIntervalMs = 5000
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &GroupCoordinator{
		config:        config,
		nh:            nh,
		logger:        logger,
		lastHeartbeat: make(map[string]map[string]time.Time),
	}
}

// JoinGroup adds a member to the consumer group and returns the member's partition assignment.
func (gc *GroupCoordinator) JoinGroup(ctx context.Context, req JoinGroupRequest) (JoinGroupResponse, error) {
	if req.SessionTimeoutMs < gc.config.SessionTimeoutMinMs || req.SessionTimeoutMs > gc.config.SessionTimeoutMaxMs {
		return JoinGroupResponse{}, fmt.Errorf("%w: session_timeout_ms %d out of range [%d, %d]",
			ErrInvalidArgument, req.SessionTimeoutMs, gc.config.SessionTimeoutMinMs, gc.config.SessionTimeoutMaxMs)
	}

	partitionCounts := make(map[string]int32, len(req.Topics))
	for _, topic := range req.Topics {
		raw, err := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
			Type:      metadata.QueryGetTopic,
			TopicName: topic,
		})
		if err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				return JoinGroupResponse{}, fmt.Errorf("%w: topic %q", ErrNotFound, topic)
			}
			return JoinGroupResponse{}, fmt.Errorf("lookup topic %q: %w", topic, err)
		}
		tm := raw.(*metadata.TopicMeta)
		partitionCounts[topic] = tm.PartitionCount
	}

	groupRaw, err := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:    metadata.QueryGetGroupState,
		GroupID: req.GroupID,
	})
	if err != nil {
		return JoinGroupResponse{}, fmt.Errorf("lookup group: %w", err)
	}
	var currentGroup *metadata.GroupState
	if groupRaw != nil {
		currentGroup = groupRaw.(*metadata.GroupState)
	}

	if currentGroup != nil && len(currentGroup.Members) > 0 {
		for _, ms := range currentGroup.Members {
			if !topicSetsEqual(ms.SubscribedTopics, req.Topics) {
				return JoinGroupResponse{}, fmt.Errorf("%w: mixed topic subscriptions in group %q", ErrInvalidArgument, req.GroupID)
			}
			break
		}
	}

	memberID := req.MemberID
	if memberID == "" {
		memberID = uuid.NewString()
	}

	memberIDs := []string{memberID}
	if currentGroup != nil {
		for id := range currentGroup.Members {
			if id != memberID {
				memberIDs = append(memberIDs, id)
			}
		}
	}
	newAssignment := rangeAssign(memberIDs, req.Topics, partitionCounts)

	member := &metadata.MemberState{
		MemberID:            memberID,
		SubscribedTopics:    req.Topics,
		SessionTimeoutMs:    req.SessionTimeoutMs,
		HeartbeatIntervalMs: req.HeartbeatIntervalMs,
		JoinedAt:            time.Now(),
	}

	if _, err = gc.nh.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type: metadata.CmdJoinConsumerGroup,
		JoinConsumerGroup: &metadata.JoinConsumerGroupCmd{
			GroupID:       req.GroupID,
			MemberID:      memberID,
			Member:        member,
			NewAssignment: newAssignment,
		},
	}); err != nil {
		return JoinGroupResponse{}, fmt.Errorf("propose join: %w", err)
	}

	raw2, err := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:    metadata.QueryGetGroupState,
		GroupID: req.GroupID,
	})
	if err != nil {
		return JoinGroupResponse{}, fmt.Errorf("lookup group after join: %w", err)
	}
	updatedGroup := raw2.(*metadata.GroupState)

	gc.heartbeatMu.Lock()
	if gc.lastHeartbeat[req.GroupID] == nil {
		gc.lastHeartbeat[req.GroupID] = make(map[string]time.Time)
	}
	gc.lastHeartbeat[req.GroupID][memberID] = time.Now()
	gc.heartbeatMu.Unlock()

	return JoinGroupResponse{
		MemberID:     memberID,
		GenerationID: updatedGroup.GenerationID,
		Assignments:  updatedGroup.Assignments[memberID],
	}, nil
}

// LeaveGroup removes a member from the consumer group voluntarily.
func (gc *GroupCoordinator) LeaveGroup(ctx context.Context, req LeaveGroupRequest) error {
	groupRaw, err := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:    metadata.QueryGetGroupState,
		GroupID: req.GroupID,
	})
	if err != nil {
		return fmt.Errorf("lookup group: %w", err)
	}
	if groupRaw == nil {
		return ErrNotGroupMember
	}
	currentGroup := groupRaw.(*metadata.GroupState)

	ms, ok := currentGroup.Members[req.MemberID]
	if !ok {
		return ErrNotGroupMember
	}

	newMemberIDs := make([]string, 0, len(currentGroup.Members)-1)
	for id := range currentGroup.Members {
		if id != req.MemberID {
			newMemberIDs = append(newMemberIDs, id)
		}
	}

	partitionCounts := make(map[string]int32, len(ms.SubscribedTopics))
	for _, topic := range ms.SubscribedTopics {
		topicRaw, lookupErr := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
			Type:      metadata.QueryGetTopic,
			TopicName: topic,
		})
		if lookupErr != nil {
			if errors.Is(lookupErr, metadata.ErrNotFound) {
				continue
			}
			return fmt.Errorf("lookup topic %q: %w", topic, lookupErr)
		}
		tm := topicRaw.(*metadata.TopicMeta)
		partitionCounts[topic] = tm.PartitionCount
	}
	newAssignment := rangeAssign(newMemberIDs, ms.SubscribedTopics, partitionCounts)

	if _, err = gc.nh.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type: metadata.CmdLeaveConsumerGroup,
		LeaveConsumerGroup: &metadata.LeaveConsumerGroupCmd{
			GroupID:       req.GroupID,
			MemberID:      req.MemberID,
			Reason:        "voluntary",
			NewAssignment: newAssignment,
		},
	}); err != nil {
		return fmt.Errorf("propose leave: %w", err)
	}

	gc.heartbeatMu.Lock()
	if gc.lastHeartbeat[req.GroupID] != nil {
		delete(gc.lastHeartbeat[req.GroupID], req.MemberID)
	}
	gc.heartbeatMu.Unlock()

	return nil
}

// CommitOffset validates membership and generation then durably stores offsets via Raft.
func (gc *GroupCoordinator) CommitOffset(ctx context.Context, groupID, memberID string, generationID int32, offsets map[metadata.TopicPartition]int64) error {
	raw, err := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:    metadata.QueryGetGroupState,
		GroupID: groupID,
	})
	if err != nil {
		return fmt.Errorf("lookup group: %w", err)
	}
	if raw == nil {
		return ErrNotGroupMember
	}
	group := raw.(*metadata.GroupState)

	if _, ok := group.Members[memberID]; !ok {
		return ErrNotGroupMember
	}
	if generationID != group.GenerationID {
		return fmt.Errorf("%w: client generation %d, current %d", ErrStaleGeneration, generationID, group.GenerationID)
	}

	assigned := make(map[metadata.TopicPartition]struct{}, len(group.Assignments[memberID]))
	for _, tp := range group.Assignments[memberID] {
		assigned[tp] = struct{}{}
	}
	for tp := range offsets {
		if _, ok := assigned[tp]; !ok {
			return fmt.Errorf("%w: partition %v not assigned to member %q", ErrInvalidArgument, tp, memberID)
		}
	}

	if _, err = gc.nh.SyncProposeMetadata(ctx, metadata.MetadataCommand{
		Type: metadata.CmdCommitConsumerOffset,
		CommitConsumerOffset: &metadata.CommitConsumerOffsetCmd{
			GroupID:      groupID,
			GroupOffsets: offsets,
		},
	}); err != nil {
		return fmt.Errorf("propose commit offset: %w", err)
	}
	return nil
}

// FetchCommittedOffsets returns the stored committed offsets for the requested partitions.
// Missing entries return -1; no membership check is performed (monitoring-friendly).
func (gc *GroupCoordinator) FetchCommittedOffsets(ctx context.Context, groupID string, partitions []metadata.TopicPartition) (map[metadata.TopicPartition]int64, error) {
	raw, err := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
		Type:       metadata.QueryGetGroupOffsets,
		GroupID:    groupID,
		Partitions: partitions,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup group offsets: %w", err)
	}
	result, _ := raw.(map[metadata.TopicPartition]int64)
	return result, nil
}

// Heartbeat records the member's liveness and signals whether a rebalance is pending.
func (gc *GroupCoordinator) Heartbeat(_ context.Context, groupID, memberID string, generationID int32) (rebalanceRequired bool, err error) {
	raw, lookupErr := gc.nh.LookupMetadata(context.Background(), metadata.MetadataQuery{
		Type:    metadata.QueryGetGroupState,
		GroupID: groupID,
	})
	if lookupErr != nil {
		return false, fmt.Errorf("lookup group: %w", lookupErr)
	}
	if raw == nil {
		return false, ErrNotGroupMember
	}
	group := raw.(*metadata.GroupState)
	if _, ok := group.Members[memberID]; !ok {
		return false, ErrNotGroupMember
	}

	gc.heartbeatMu.Lock()
	if gc.lastHeartbeat[groupID] == nil {
		gc.lastHeartbeat[groupID] = make(map[string]time.Time)
	}
	gc.lastHeartbeat[groupID][memberID] = time.Now()
	gc.heartbeatMu.Unlock()

	return group.GenerationID > generationID, nil
}

// RebuildHeartbeatTable seeds the in-memory heartbeat table with the current
// time for all active group members, giving them a full session timeout window
// before the first possible sweep eviction.
func (gc *GroupCoordinator) RebuildHeartbeatTable() {
	raw, err := gc.nh.LookupMetadata(context.Background(), metadata.MetadataQuery{
		Type: metadata.QueryGetAllGroupStates,
	})
	if err != nil {
		return
	}
	groups, ok := raw.(map[string]*metadata.GroupState)
	if !ok || groups == nil {
		return
	}

	now := time.Now()
	gc.heartbeatMu.Lock()
	defer gc.heartbeatMu.Unlock()
	for groupID, group := range groups {
		if gc.lastHeartbeat[groupID] == nil {
			gc.lastHeartbeat[groupID] = make(map[string]time.Time)
		}
		for memberID := range group.Members {
			gc.lastHeartbeat[groupID][memberID] = now
		}
	}
}

// Start launches the session timeout sweep goroutine; it stops when ctx is cancelled.
func (gc *GroupCoordinator) Start(ctx context.Context) {
	go gc.sessionTimeoutSweep(ctx)
}

func (gc *GroupCoordinator) sessionTimeoutSweep(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(gc.config.SweepIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			gc.sweepOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (gc *GroupCoordinator) sweepOnce(ctx context.Context) {
	leaderID, _, valid, err := gc.nh.GetLeaderID(gc.config.MetadataShardID)
	if err != nil || !valid || leaderID != gc.config.ThisNodeID {
		return
	}

	raw, err := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{Type: metadata.QueryGetAllGroupStates})
	if err != nil {
		return
	}
	groups, ok := raw.(map[string]*metadata.GroupState)
	if !ok {
		return
	}

	now := time.Now()
	for groupID, group := range groups {
		for memberID, ms := range group.Members {
			gc.heartbeatMu.RLock()
			var lastHB time.Time
			var exists bool
			if gc.lastHeartbeat[groupID] != nil {
				lastHB, exists = gc.lastHeartbeat[groupID][memberID]
			}
			gc.heartbeatMu.RUnlock()

			if !exists {
				continue
			}
			sessionTimeout := time.Duration(ms.SessionTimeoutMs) * time.Millisecond
			if now.Sub(lastHB) <= sessionTimeout {
				continue
			}

			remainingMembers := make([]string, 0, len(group.Members)-1)
			for id := range group.Members {
				if id != memberID {
					remainingMembers = append(remainingMembers, id)
				}
			}
			partitionCounts := make(map[string]int32, len(ms.SubscribedTopics))
			for _, topic := range ms.SubscribedTopics {
				topicRaw, lookupErr := gc.nh.LookupMetadata(ctx, metadata.MetadataQuery{
					Type:      metadata.QueryGetTopic,
					TopicName: topic,
				})
				if lookupErr != nil || topicRaw == nil {
					continue
				}
				tm := topicRaw.(*metadata.TopicMeta)
				partitionCounts[topic] = tm.PartitionCount
			}
			newAssignment := rangeAssign(remainingMembers, ms.SubscribedTopics, partitionCounts)

			if _, proposeErr := gc.nh.SyncProposeMetadata(ctx, metadata.MetadataCommand{
				Type: metadata.CmdLeaveConsumerGroup,
				LeaveConsumerGroup: &metadata.LeaveConsumerGroupCmd{
					GroupID:       groupID,
					MemberID:      memberID,
					Reason:        "timeout",
					NewAssignment: newAssignment,
				},
			}); proposeErr != nil {
				continue
			}

			gc.heartbeatMu.Lock()
			if gc.lastHeartbeat[groupID] != nil {
				delete(gc.lastHeartbeat[groupID], memberID)
			}
			gc.heartbeatMu.Unlock()
		}
	}
}

// topicSetsEqual reports whether two topic slices contain the same elements (order-independent).
func topicSetsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := make([]string, len(a))
	copy(sa, a)
	sort.Strings(sa)
	sb := make([]string, len(b))
	copy(sb, b)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

// rangeAssign computes a range-based partition assignment across members.
// Members with zero partitions still appear in the result with an empty slice.
func rangeAssign(memberIDs []string, topics []string, partitionCounts map[string]int32) map[string][]metadata.TopicPartition {
	if len(memberIDs) == 0 {
		return map[string][]metadata.TopicPartition{}
	}

	sorted := make([]string, len(memberIDs))
	copy(sorted, memberIDs)
	sort.Strings(sorted)

	sortedTopics := make([]string, len(topics))
	copy(sortedTopics, topics)
	sort.Strings(sortedTopics)

	result := make(map[string][]metadata.TopicPartition, len(sorted))
	for _, m := range sorted {
		result[m] = []metadata.TopicPartition{}
	}

	for _, topic := range sortedTopics {
		nPartitions := int(partitionCounts[topic])
		if nPartitions == 0 {
			continue
		}
		nMembers := len(sorted)
		base := nPartitions / nMembers
		remainder := nPartitions % nMembers
		cursor := 0
		for i, memberID := range sorted {
			count := base
			if i < remainder {
				count++
			}
			for p := cursor; p < cursor+count; p++ {
				result[memberID] = append(result[memberID], metadata.TopicPartition{
					Topic:       topic,
					PartitionID: int32(p),
				})
			}
			cursor += count
		}
	}

	return result
}
