package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	iclient "github.com/bunnymq/bunnymq/internal/client"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ConsumerConfig extends Config with consumer-specific settings.
type ConsumerConfig struct {
	Config
	GroupID         string
	MaxFetchBytes   int
	MaxFetchWaitMs  int64
	AutoOffsetReset OffsetResetPolicy
}

// Consumer reads messages from BunnyMQ topics.
// In manual mode (empty GroupID) the caller manages partition selection and offsets via Seek.
// Safe for sequential use from a single goroutine.
type Consumer struct {
	config              ConsumerConfig
	pool                *iclient.ConnPool
	meta                *iclient.MetaCache
	decoder             *iclient.BatchDecoder
	fetchOffsets        map[TP]int64
	soughtPartitions    []TP
	subscribedTopics    []string
	knownAddrs          []string
	memberID            string
	generationID        int32
	coordAddr           string
	assignedPartitions  []TP
}

// NewConsumer creates a Consumer.
// Returns an error only if the configuration is invalid.
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if len(config.BootstrapServers) == 0 {
		return nil, errors.New("client: BootstrapServers must not be empty")
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 5 * time.Second
	}
	if config.RetryPolicy.MaxRetries == 0 {
		config.RetryPolicy.MaxRetries = 3
	}
	if config.RetryPolicy.InitialBackoff <= 0 {
		config.RetryPolicy.InitialBackoff = 50 * time.Millisecond
	}
	if config.RetryPolicy.MaxBackoff <= 0 {
		config.RetryPolicy.MaxBackoff = 2 * time.Second
	}
	if config.RetryPolicy.BackoffFactor == 0 {
		config.RetryPolicy.BackoffFactor = 2.0
	}
	if config.MaxFetchBytes <= 0 {
		config.MaxFetchBytes = 1 << 20
	}
	if config.MaxFetchWaitMs <= 0 {
		config.MaxFetchWaitMs = 500
	}

	var dialOpts []grpc.DialOption
	if config.TLS != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(config.TLS)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if config.AuthToken != "" {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(newAuthInterceptor(config.AuthToken)))
	}

	return &Consumer{
		config:       config,
		pool:         iclient.NewConnPool(dialOpts...),
		meta:         iclient.NewMetaCache(60 * time.Second),
		decoder:      iclient.NewBatchDecoder(),
		fetchOffsets: make(map[TP]int64),
		knownAddrs:   append([]string(nil), config.BootstrapServers...),
	}, nil
}

// Subscribe records the topics to consume from.
// In manual mode (no GroupID), no JoinGroup is performed; the caller must also
// call Seek on each partition before Poll will fetch from it.
// In group mode, Subscribe discovers the group coordinator, issues JoinGroup,
// fetches committed offsets for assigned partitions, and seeks each one.
func (c *Consumer) Subscribe(topics []string) error {
	c.subscribedTopics = topics
	if c.config.GroupID == "" {
		return nil
	}
	return c.joinGroup(context.Background(), topics)
}

func (c *Consumer) joinGroup(ctx context.Context, topics []string) error {
	if err := c.findCoordinator(ctx); err != nil {
		return err
	}

	resp, err := c.doJoinGroup(ctx, topics)
	if err != nil {
		bunnyErr, notLeader := extractErr(err)
		if bunnyErr == nil || bunnyErr.Code != pb.BunnyErrorCode_NOT_LEADER {
			return err
		}
		// Refresh coordinator address from the NOT_LEADER detail and retry once.
		if notLeader != nil && notLeader.LeaderAddress != "" {
			c.coordAddr = notLeader.LeaderAddress
		} else if refreshErr := c.findCoordinator(ctx); refreshErr != nil {
			return err
		}
		resp, err = c.doJoinGroup(ctx, topics)
		if err != nil {
			return err
		}
	}

	c.memberID = resp.MemberId
	c.generationID = resp.GenerationId
	c.assignedPartitions = protoToTP(resp.Assignments)

	return c.initFetchOffsets(ctx)
}

func (c *Consumer) doJoinGroup(ctx context.Context, topics []string) (*pb.JoinGroupResponse, error) {
	conn, err := c.pool.Get(c.coordAddr)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	resp, err := pb.NewDataServiceClient(conn).JoinGroup(callCtx, &pb.JoinGroupRequest{
		GroupId:          c.config.GroupID,
		MemberId:         c.memberID,
		SubscribedTopics: topics,
	})
	cancel()
	return resp, err
}

// findCoordinator resolves the group coordinator address via DescribeCluster and
// stores it in coordAddr. Uses the first node in the cluster response as the
// coordinator candidate; NOT_LEADER handling refines it on JoinGroup retry.
func (c *Consumer) findCoordinator(ctx context.Context) error {
	for _, addr := range c.knownAddrs {
		conn, err := c.pool.Get(addr)
		if err != nil {
			continue
		}
		mgmt := pb.NewManagementServiceClient(conn)
		callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
		resp, err := mgmt.DescribeCluster(callCtx, &pb.DescribeClusterRequest{})
		cancel()
		if err != nil {
			continue
		}
		if len(resp.Nodes) > 0 {
			c.coordAddr = resp.Nodes[0].Address
		} else {
			c.coordAddr = addr
		}
		return nil
	}
	return ErrNoReachableServer
}

// initFetchOffsets fetches committed offsets for all assigned partitions from
// the coordinator and seeks each partition to the appropriate starting offset.
func (c *Consumer) initFetchOffsets(ctx context.Context) error {
	if len(c.assignedPartitions) == 0 {
		return nil
	}

	partitions := make([]*pb.TopicPartition, len(c.assignedPartitions))
	for i, tp := range c.assignedPartitions {
		partitions[i] = &pb.TopicPartition{Topic: tp.Topic, PartitionId: tp.PartitionID}
	}

	conn, err := c.pool.Get(c.coordAddr)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	resp, err := pb.NewDataServiceClient(conn).FetchCommittedOffsets(callCtx, &pb.FetchCommittedOffsetsRequest{
		GroupId:    c.config.GroupID,
		Partitions: partitions,
	})
	cancel()
	if err != nil {
		return err
	}

	committed := make(map[TP]int64, len(resp.Offsets))
	for _, po := range resp.Offsets {
		committed[TP{Topic: po.Topic, PartitionID: po.PartitionId}] = po.Offset
	}

	for _, tp := range c.assignedPartitions {
		if off, ok := committed[tp]; ok && off > -1 {
			c.Seek(tp.Topic, tp.PartitionID, off)
			continue
		}
		switch c.config.AutoOffsetReset {
		case OffsetResetEarliest:
			c.Seek(tp.Topic, tp.PartitionID, 0)
		default: // OffsetResetLatest
			latestOff, err := c.getPartitionOffset(ctx, tp, pb.OffsetQueryType_LATEST)
			if err != nil {
				return err
			}
			c.Seek(tp.Topic, tp.PartitionID, latestOff)
		}
	}

	return nil
}

// getPartitionOffset fetches the earliest or latest offset for a partition.
func (c *Consumer) getPartitionOffset(ctx context.Context, tp TP, queryType pb.OffsetQueryType) (int64, error) {
	addr, err := c.leaderFor(ctx, tp.Topic, tp.PartitionID)
	if err != nil {
		return 0, err
	}
	conn, err := c.pool.Get(addr)
	if err != nil {
		return 0, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	offResp, err := pb.NewDataServiceClient(conn).GetOffsets(callCtx, &pb.GetOffsetsRequest{
		Topic:       tp.Topic,
		PartitionId: tp.PartitionID,
		QueryType:   queryType,
	})
	cancel()
	if err != nil {
		return 0, err
	}
	return offResp.Offset, nil
}

// Seek sets the next fetch offset for a partition and marks it for polling.
// Calling Seek again on the same partition updates the offset in place.
func (c *Consumer) Seek(topic string, partitionID int32, offset int64) {
	tp := TP{Topic: topic, PartitionID: partitionID}
	if _, ok := c.fetchOffsets[tp]; !ok {
		c.soughtPartitions = append(c.soughtPartitions, tp)
	}
	c.fetchOffsets[tp] = offset
}

// Poll fetches records from all sought partitions.
// maxWaitMs is the total time budget distributed evenly across partitions.
// On NOT_LEADER, the metadata cache is updated and the partition is skipped
// (no error returned); it will be retried on the next Poll.
func (c *Consumer) Poll(ctx context.Context, maxWaitMs int64) ([]Record, error) {
	if len(c.soughtPartitions) == 0 {
		return nil, nil
	}

	deadline := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)
	var out []Record

	for i, tp := range c.soughtPartitions {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		perPartitionWaitMs := remaining.Milliseconds() / int64(len(c.soughtPartitions)-i)
		if perPartitionWaitMs > c.config.MaxFetchWaitMs {
			perPartitionWaitMs = c.config.MaxFetchWaitMs
		}

		recs, err := c.fetchPartition(ctx, tp, perPartitionWaitMs)
		if err != nil {
			bunnyErr, notLeader := extractErr(err)
			if bunnyErr != nil && bunnyErr.Code == pb.BunnyErrorCode_NOT_LEADER {
				if notLeader != nil && notLeader.LeaderAddress != "" {
					c.meta.SetLeader(tp.Topic, tp.PartitionID, notLeader.LeaderAddress)
				} else {
					c.meta.Invalidate(tp.Topic)
				}
				continue
			}
			return nil, err
		}
		out = append(out, recs...)
	}

	return out, nil
}

// Commit commits the current fetch offsets to the group coordinator.
// No-op for manual consumers (no GroupID).
func (c *Consumer) Commit(ctx context.Context) error {
	if c.config.GroupID == "" {
		return nil
	}
	offsets := make(map[TP]int64, len(c.soughtPartitions))
	for _, tp := range c.soughtPartitions {
		offsets[tp] = c.fetchOffsets[tp]
	}
	return c.CommitOffsets(ctx, offsets)
}

// CommitOffsets commits caller-specified offsets to the group coordinator.
// No-op for manual consumers (no GroupID).
// Returns ErrStaleGeneration if the server rejects the commit due to a stale generation.
func (c *Consumer) CommitOffsets(ctx context.Context, offsets map[TP]int64) error {
	if c.config.GroupID == "" {
		return nil
	}

	protoOffsets := make([]*pb.PartitionOffset, 0, len(offsets))
	for tp, off := range offsets {
		protoOffsets = append(protoOffsets, &pb.PartitionOffset{
			Topic:       tp.Topic,
			PartitionId: tp.PartitionID,
			Offset:      off,
		})
	}

	conn, err := c.pool.Get(c.coordAddr)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	_, err = pb.NewDataServiceClient(conn).CommitOffset(callCtx, &pb.CommitOffsetRequest{
		GroupId:      c.config.GroupID,
		MemberId:     c.memberID,
		GenerationId: c.generationID,
		Offsets:      protoOffsets,
	})
	cancel()
	if err != nil {
		bunnyErr, _ := extractErr(err)
		if bunnyErr != nil && bunnyErr.Code == pb.BunnyErrorCode_STALE_GENERATION {
			return ErrStaleGeneration
		}
		return err
	}
	return nil
}

// Close releases all gRPC connections.
func (c *Consumer) Close() error { return c.pool.Close() }

// fetchPartition sends one Fetch RPC for tp and returns decoded records.
// On success, advances fetchOffsets[tp] to resp.NextOffset.
func (c *Consumer) fetchPartition(ctx context.Context, tp TP, waitMs int64) ([]Record, error) {
	addr, err := c.leaderFor(ctx, tp.Topic, tp.PartitionID)
	if err != nil {
		return nil, err
	}

	conn, err := c.pool.Get(addr)
	if err != nil {
		return nil, err
	}

	callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	resp, rpcErr := pb.NewDataServiceClient(conn).Fetch(callCtx, &pb.FetchRequest{
		Topic:       tp.Topic,
		PartitionId: tp.PartitionID,
		Offset:      c.fetchOffsets[tp],
		MaxBytes:    int32(c.config.MaxFetchBytes),
		MaxWaitMs:   waitMs,
	})
	cancel()
	if rpcErr != nil {
		return nil, rpcErr
	}

	if len(resp.Records) == 0 {
		return nil, nil
	}

	decoded, decErr := c.decoder.Decode(resp.Records)
	if decErr != nil {
		return nil, decErr
	}

	c.fetchOffsets[tp] = resp.NextOffset

	out := make([]Record, len(decoded))
	for i, dr := range decoded {
		out[i] = Record{
			Topic:       tp.Topic,
			PartitionID: tp.PartitionID,
			Offset:      dr.Offset,
			Key:         dr.Key,
			Value:       dr.Value,
			Headers:     dr.Headers,
			TimestampMs: dr.TimestampMs,
		}
	}
	return out, nil
}

// leaderFor returns the Data API address of the current leader for the given partition.
func (c *Consumer) leaderFor(ctx context.Context, topic string, partID int32) (string, error) {
	meta, err := c.metaFor(ctx, topic)
	if err != nil {
		return "", err
	}
	if addr := meta.Leaders[partID]; addr != "" {
		return addr, nil
	}
	meta, err = c.refreshMeta(ctx, topic)
	if err != nil {
		return "", err
	}
	if addr := meta.Leaders[partID]; addr != "" {
		return addr, nil
	}
	return "", fmt.Errorf("client: no leader known for %s partition %d", topic, partID)
}

// metaFor returns cached metadata for the topic or fetches it if absent/expired.
func (c *Consumer) metaFor(ctx context.Context, topic string) (*iclient.TopicMeta, error) {
	if meta := c.meta.Get(topic); meta != nil {
		return meta, nil
	}
	return c.refreshMeta(ctx, topic)
}

// refreshMeta fetches topic metadata from the first reachable bootstrap server.
func (c *Consumer) refreshMeta(ctx context.Context, topic string) (*iclient.TopicMeta, error) {
	for _, addr := range c.knownAddrs {
		conn, err := c.pool.Get(addr)
		if err != nil {
			continue
		}
		mgmt := pb.NewManagementServiceClient(conn)

		callCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
		descResp, err := mgmt.DescribeTopic(callCtx, &pb.DescribeTopicRequest{Name: topic})
		cancel()
		if err != nil {
			continue
		}

		callCtx2, cancel2 := context.WithTimeout(ctx, c.config.RequestTimeout)
		clusterResp, err := mgmt.DescribeCluster(callCtx2, &pb.DescribeClusterRequest{})
		cancel2()
		if err != nil {
			continue
		}

		nodeAddrs := make(map[uint64]string, len(clusterResp.Nodes))
		for _, n := range clusterResp.Nodes {
			nodeAddrs[n.NodeId] = n.Address
		}

		leaders := make(map[int32]string, len(descResp.Partitions))
		for _, part := range descResp.Partitions {
			if nodeAddr, ok := nodeAddrs[part.LeaderNodeId]; ok {
				leaders[part.PartitionId] = nodeAddr
			}
		}

		partitionCount := int32(len(descResp.Partitions))
		if descResp.Topic != nil && descResp.Topic.PartitionCount > 0 {
			partitionCount = descResp.Topic.PartitionCount
		}

		meta := &iclient.TopicMeta{
			PartitionCount: partitionCount,
			Leaders:        leaders,
			FetchedAt:      time.Now(),
		}
		c.meta.Put(topic, meta)
		return meta, nil
	}
	return nil, ErrNoReachableServer
}

// protoToTP converts a slice of proto TopicPartition to []TP.
func protoToTP(protos []*pb.TopicPartition) []TP {
	out := make([]TP, len(protos))
	for i, p := range protos {
		out[i] = TP{Topic: p.Topic, PartitionID: p.PartitionId}
	}
	return out
}
