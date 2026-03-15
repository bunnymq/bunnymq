package client

import (
	"context"
	"errors"
	"time"

	iclient "github.com/bunnymq/bunnymq/internal/client"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// AdminClient manages topics and describes the cluster via ManagementService.
// Safe for concurrent use by multiple goroutines.
type AdminClient struct {
	config Config
	pool   *iclient.ConnPool
	addr   string
}

// NewAdminClient creates an AdminClient targeting the first bootstrap server.
// Returns an error only if the configuration is invalid.
func NewAdminClient(config Config) (*AdminClient, error) {
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

	var dialOpts []grpc.DialOption
	if config.TLS != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(config.TLS)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if config.AuthToken != "" {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(newAuthInterceptor(config.AuthToken)))
	}

	return &AdminClient{
		config: config,
		pool:   iclient.NewConnPool(dialOpts...),
		addr:   config.BootstrapServers[0],
	}, nil
}

// Close releases all gRPC connections held by the client.
func (a *AdminClient) Close() error {
	return a.pool.Close()
}

// CreateTopic creates a new topic. Returns ErrTopicAlreadyExists if it already exists.
func (a *AdminClient) CreateTopic(ctx context.Context, req CreateTopicRequest) (TopicInfo, error) {
	var result TopicInfo
	err := a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		resp, err := mgmt.CreateTopic(callCtx, &pb.CreateTopicRequest{
			Name:              req.Name,
			PartitionCount:    req.PartitionCount,
			ReplicationFactor: req.ReplicationFactor,
			RetentionMs:       req.RetentionMs,
			RetentionBytes:    req.RetentionBytes,
		})
		if err != nil {
			return err
		}
		result = fromProtoTopicInfo(resp.Topic)
		return nil
	})
	return result, err
}

// DeleteTopic deletes a topic. Returns ErrTopicNotFound if it does not exist.
func (a *AdminClient) DeleteTopic(ctx context.Context, name string) error {
	return a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		_, err := mgmt.DeleteTopic(callCtx, &pb.DeleteTopicRequest{Name: name})
		return err
	})
}

// ListTopics returns a summary of all topics.
func (a *AdminClient) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	var result []TopicInfo
	err := a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		resp, err := mgmt.ListTopics(callCtx, &pb.ListTopicsRequest{})
		if err != nil {
			return err
		}
		result = make([]TopicInfo, len(resp.Topics))
		for i, t := range resp.Topics {
			result[i] = fromProtoTopicInfo(t)
		}
		return nil
	})
	return result, err
}

// DescribeTopic returns the full description of a topic. Returns ErrTopicNotFound if absent.
func (a *AdminClient) DescribeTopic(ctx context.Context, name string) (TopicDescription, error) {
	var result TopicDescription
	err := a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		resp, err := mgmt.DescribeTopic(callCtx, &pb.DescribeTopicRequest{Name: name})
		if err != nil {
			return err
		}
		result = TopicDescription{
			Topic:      fromProtoTopicInfo(resp.Topic),
			Partitions: fromProtoPartitions(resp.Partitions),
		}
		return nil
	})
	return result, err
}

// AlterTopicPartitions changes the partition count for a topic.
func (a *AdminClient) AlterTopicPartitions(ctx context.Context, name string, newCount int32) error {
	return a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		_, err := mgmt.AlterTopicPartitions(callCtx, &pb.AlterTopicPartitionsRequest{
			Name:              name,
			NewPartitionCount: newCount,
		})
		return err
	})
}

// AlterTopicRetention updates the retention policy for a topic.
func (a *AdminClient) AlterTopicRetention(ctx context.Context, name string, retentionMs, retentionBytes int64) error {
	return a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		_, err := mgmt.AlterTopicRetention(callCtx, &pb.AlterTopicRetentionRequest{
			Name:           name,
			RetentionMs:    retentionMs,
			RetentionBytes: retentionBytes,
		})
		return err
	})
}

// DescribeCluster returns the list of broker nodes in the cluster.
func (a *AdminClient) DescribeCluster(ctx context.Context) (ClusterDescription, error) {
	var result ClusterDescription
	err := a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		resp, err := mgmt.DescribeCluster(callCtx, &pb.DescribeClusterRequest{})
		if err != nil {
			return err
		}
		nodes := make([]NodeDescriptor, len(resp.Nodes))
		for i, n := range resp.Nodes {
			nodes[i] = NodeDescriptor{NodeID: n.NodeId, Address: n.Address}
		}
		result = ClusterDescription{Nodes: nodes}
		return nil
	})
	return result, err
}

// ListPartitions returns partition info with offsets for a topic.
func (a *AdminClient) ListPartitions(ctx context.Context, topic string) ([]PartitionInfoWithOffsets, error) {
	var result []PartitionInfoWithOffsets
	err := a.withRetry(ctx, func(callCtx context.Context, mgmt pb.ManagementServiceClient) error {
		resp, err := mgmt.ListPartitions(callCtx, &pb.ListPartitionsRequest{Topic: topic})
		if err != nil {
			return err
		}
		result = make([]PartitionInfoWithOffsets, len(resp.Partitions))
		for i, p := range resp.Partitions {
			result[i] = PartitionInfoWithOffsets{
				Info:           fromProtoPartitionInfo(p.Info),
				EarliestOffset: p.EarliestOffset,
				LatestOffset:   p.LatestOffset,
			}
		}
		return nil
	})
	return result, err
}

// withRetry executes fn, retrying on UNAVAILABLE/TIMEOUT errors with exponential
// backoff, and maps domain-level error codes to typed Go errors.
func (a *AdminClient) withRetry(ctx context.Context, fn func(context.Context, pb.ManagementServiceClient) error) error {
	conn, err := a.pool.Get(a.addr)
	if err != nil {
		return err
	}
	mgmt := pb.NewManagementServiceClient(conn)

	for attempt := 0; ; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, a.config.RequestTimeout)
		err = fn(callCtx, mgmt)
		cancel()

		if err == nil {
			return nil
		}

		bunnyErr, _ := extractErr(err)
		if bunnyErr != nil {
			switch bunnyErr.Code {
			case pb.BunnyErrorCode_UNAVAILABLE, pb.BunnyErrorCode_TIMEOUT:
				if attempt >= a.config.RetryPolicy.MaxRetries {
					return err
				}
				time.Sleep(calcBackoff(attempt, a.config.RetryPolicy))
				continue
			case pb.BunnyErrorCode_TOPIC_ALREADY_EXISTS:
				return ErrTopicAlreadyExists
			case pb.BunnyErrorCode_TOPIC_NOT_FOUND:
				return ErrTopicNotFound
			}
		}
		return err
	}
}

func fromProtoTopicInfo(t *pb.TopicInfo) TopicInfo {
	if t == nil {
		return TopicInfo{}
	}
	return TopicInfo{
		Name:              t.Name,
		PartitionCount:    t.PartitionCount,
		ReplicationFactor: t.ReplicationFactor,
		RetentionMs:       t.RetentionMs,
		RetentionBytes:    t.RetentionBytes,
		CreatedAtMs:       t.CreatedAtMs,
	}
}

func fromProtoPartitionInfo(p *pb.PartitionInfo) PartitionInfo {
	if p == nil {
		return PartitionInfo{}
	}
	return PartitionInfo{
		PartitionID:    p.PartitionId,
		ShardID:        p.ShardId,
		LeaderNodeID:   p.LeaderNodeId,
		LeaderEpoch:    p.LeaderEpoch,
		ReplicaNodeIDs: append([]uint64(nil), p.ReplicaNodeIds...),
	}
}

func fromProtoPartitions(parts []*pb.PartitionInfo) []PartitionInfo {
	result := make([]PartitionInfo, len(parts))
	for i, p := range parts {
		result[i] = fromProtoPartitionInfo(p)
	}
	return result
}
