package management

import (
	"context"

	"github.com/bunnymq/bunnymq/internal/coordinator/cluster"
)

// ClusterCoordinatorIface is the subset of ClusterCoordinator used by ManagementServer.
// It exists to allow test doubles without importing the full coordinator.
type ClusterCoordinatorIface interface {
	CreateTopic(ctx context.Context, name string, partitionCount int32, replicationFactor int32, configOverrides cluster.TopicConfigOverrides) (cluster.TopicInfo, error)
	DeleteTopic(ctx context.Context, name string) error
	ListTopics(ctx context.Context) ([]cluster.TopicInfo, error)
	DescribeTopic(ctx context.Context, name string) (cluster.TopicDescription, error)
	AlterTopicPartitionCount(ctx context.Context, name string, newCount int32) error
	AlterTopicRetention(ctx context.Context, name string, retentionMs int64, retentionBytes int64) error
	DescribeCluster(ctx context.Context) (cluster.ClusterDescription, error)
}
