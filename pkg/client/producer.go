package client

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sync/atomic"
	"time"

	iclient "github.com/bunnymq/bunnymq/internal/client"
	pb "github.com/bunnymq/bunnymq/pkg/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ErrNoReachableServer is returned when none of the bootstrap servers can be contacted.
var ErrNoReachableServer = errors.New("client: no reachable server")

// ProducerConfig extends Config with producer-specific settings.
type ProducerConfig struct {
	Config
	DefaultAcks      AcksMode
	MetadataCacheTTL time.Duration
}

// Producer publishes messages to BunnyMQ topics.
// Safe for concurrent use by multiple goroutines.
type Producer struct {
	config            ProducerConfig
	pool              *iclient.ConnPool
	meta              *iclient.MetaCache
	encoder           *iclient.BatchEncoder
	roundRobinCounter atomic.Int64
	knownAddrs        []string
}

// NewProducer creates a Producer that connects to the given bootstrap servers.
// Returns an error only if the configuration is invalid; connectivity failures
// surface at the first Send call.
func NewProducer(config ProducerConfig) (*Producer, error) {
	if len(config.BootstrapServers) == 0 {
		return nil, errors.New("client: BootstrapServers must not be empty")
	}

	ttl := config.MetadataCacheTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
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

	return &Producer{
		config:     config,
		pool:       iclient.NewConnPool(dialOpts...),
		meta:       iclient.NewMetaCache(ttl),
		encoder:    iclient.NewBatchEncoder(),
		knownAddrs: append([]string(nil), config.BootstrapServers...),
	}, nil
}

// Send encodes key+value+headers as a single-record batch and produces it to the
// given topic. Partition selection uses FNV-1a hash of key or round-robin for nil key.
// Returns the assigned base_offset, or -1 for AcksZero.
func (p *Producer) Send(
	ctx context.Context,
	topic string,
	key, value []byte,
	headers map[string][]byte,
	acks AcksMode,
) (int64, error) {
	meta, err := p.metaFor(ctx, topic)
	if err != nil {
		return -1, err
	}

	partID := selectPartition(key, meta.PartitionCount, &p.roundRobinCounter)

	nowMs := time.Now().UnixMilli()
	batchData, err := p.encoder.Encode([]iclient.BatchRecord{{
		Key:         key,
		Value:       value,
		Headers:     headers,
		TimestampMs: nowMs,
	}})
	if err != nil {
		return -1, err
	}

	return p.sendToPartition(ctx, topic, partID, batchData, acks)
}

// SendBatch produces a pre-encoded batch to a specific partition.
// The base_offset field in the batch header is ignored — Storage assigns it.
// Returns the assigned base_offset, or -1 for AcksZero.
func (p *Producer) SendBatch(
	ctx context.Context,
	topic string,
	partitionID int32,
	batchData []byte,
	acks AcksMode,
) (int64, error) {
	return p.sendToPartition(ctx, topic, partitionID, batchData, acks)
}

// Flush is a no-op in v1 (there is no internal batching buffer).
func (p *Producer) Flush(_ context.Context) error { return nil }

// Close releases all gRPC connections held by the producer.
func (p *Producer) Close() error { return p.pool.Close() }

// selectPartition returns the target partition for a record.
// nil/empty key → round-robin using the per-producer atomic counter.
// non-empty key → FNV-1a 32-bit hash mod partitionCount.
func selectPartition(key []byte, partitionCount int32, counter *atomic.Int64) int32 {
	if len(key) == 0 {
		n := counter.Add(1) - 1
		return int32(n % int64(partitionCount))
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(partitionCount))
}

// metaFor returns cached metadata for the topic or fetches it if absent/expired.
func (p *Producer) metaFor(ctx context.Context, topic string) (*iclient.TopicMeta, error) {
	if meta := p.meta.Get(topic); meta != nil {
		return meta, nil
	}
	return p.refreshMeta(ctx, topic)
}

// refreshMeta fetches topic metadata from the first reachable bootstrap server.
// It calls DescribeTopic to get partition/leader-node info, then DescribeCluster
// to resolve node IDs to Data API addresses.
func (p *Producer) refreshMeta(ctx context.Context, topic string) (*iclient.TopicMeta, error) {
	for _, addr := range p.knownAddrs {
		conn, err := p.pool.Get(addr)
		if err != nil {
			continue
		}
		mgmt := pb.NewManagementServiceClient(conn)

		callCtx, cancel := context.WithTimeout(ctx, p.config.RequestTimeout)
		descResp, err := mgmt.DescribeTopic(callCtx, &pb.DescribeTopicRequest{Name: topic})
		cancel()
		if err != nil {
			continue
		}

		callCtx2, cancel2 := context.WithTimeout(ctx, p.config.RequestTimeout)
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
		p.meta.Put(topic, meta)
		return meta, nil
	}
	return nil, ErrNoReachableServer
}

// leaderFor returns the Data API address of the current leader for the given partition.
func (p *Producer) leaderFor(ctx context.Context, topic string, partID int32) (string, error) {
	meta, err := p.metaFor(ctx, topic)
	if err != nil {
		return "", err
	}
	if addr := meta.Leaders[partID]; addr != "" {
		return addr, nil
	}
	// Refresh once if leader is unknown for this partition.
	meta, err = p.refreshMeta(ctx, topic)
	if err != nil {
		return "", err
	}
	if addr := meta.Leaders[partID]; addr != "" {
		return addr, nil
	}
	return "", fmt.Errorf("client: no leader known for %s partition %d", topic, partID)
}

// sendToPartition sends batchData to the leader of the given partition, applying
// the retry policy for NOT_LEADER and UNAVAILABLE/TIMEOUT errors.
func (p *Producer) sendToPartition(
	ctx context.Context,
	topic string,
	partID int32,
	batchData []byte,
	acks AcksMode,
) (int64, error) {
	for attempt := 0; ; attempt++ {
		addr, err := p.leaderFor(ctx, topic, partID)
		if err != nil {
			// Retry transient metadata failures (e.g., all management servers
			// temporarily unreachable during a leader election) using the same
			// backoff policy applied to transport-level errors.
			if errors.Is(err, ErrNoReachableServer) && attempt < p.config.RetryPolicy.MaxRetries {
				time.Sleep(calcBackoff(attempt, p.config.RetryPolicy))
				continue
			}
			return -1, err
		}

		conn, err := p.pool.Get(addr)
		if err != nil {
			if attempt >= p.config.RetryPolicy.MaxRetries {
				return -1, err
			}
			time.Sleep(calcBackoff(attempt, p.config.RetryPolicy))
			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, p.config.RequestTimeout)
		resp, rpcErr := pb.NewDataServiceClient(conn).Produce(callCtx, &pb.ProduceRequest{
			Topic:       topic,
			PartitionId: partID,
			Acks:        pb.AcksMode(acks),
			BatchData:   batchData,
		})
		cancel()

		if rpcErr == nil {
			return resp.Offset, nil
		}

		bunnyErr, notLeader := extractErr(rpcErr)
		if bunnyErr == nil {
			// gRPC transport-level Unavailable (e.g., connection refused after leader
			// crash): invalidate the cached leader and retry so the next attempt
			// discovers the new leader via refreshMeta.
			if st, ok := status.FromError(rpcErr); ok && st.Code() == codes.Unavailable {
				p.meta.Invalidate(topic)
				if attempt >= p.config.RetryPolicy.MaxRetries {
					return -1, rpcErr
				}
				time.Sleep(calcBackoff(attempt, p.config.RetryPolicy))
				continue
			}
			return -1, rpcErr
		}

		switch bunnyErr.Code {
		case pb.BunnyErrorCode_NOT_LEADER:
			if notLeader != nil && notLeader.LeaderAddress != "" {
				p.meta.SetLeader(topic, partID, notLeader.LeaderAddress)
			}
			if attempt >= p.config.RetryPolicy.MaxRetries {
				return -1, rpcErr
			}
			continue

		case pb.BunnyErrorCode_UNAVAILABLE, pb.BunnyErrorCode_TIMEOUT:
			if attempt >= p.config.RetryPolicy.MaxRetries {
				return -1, rpcErr
			}
			time.Sleep(calcBackoff(attempt, p.config.RetryPolicy))
			continue

		default:
			return -1, rpcErr
		}
	}
}

// extractErr extracts BunnyErrorDetail and NotLeaderDetail from a gRPC status error.
func extractErr(err error) (*pb.BunnyErrorDetail, *pb.NotLeaderDetail) {
	st, ok := status.FromError(err)
	if !ok {
		return nil, nil
	}
	var bunnyErr *pb.BunnyErrorDetail
	var notLeader *pb.NotLeaderDetail
	for _, detail := range st.Details() {
		switch d := detail.(type) {
		case *pb.BunnyErrorDetail:
			bunnyErr = d
		case *pb.NotLeaderDetail:
			notLeader = d
		}
	}
	return bunnyErr, notLeader
}

// calcBackoff returns the capped exponential backoff for the given attempt index (0-based).
func calcBackoff(attempt int, policy RetryPolicy) time.Duration {
	if policy.InitialBackoff <= 0 {
		return 0
	}
	d := float64(policy.InitialBackoff) * math.Pow(policy.BackoffFactor, float64(attempt))
	if d > float64(policy.MaxBackoff) {
		return policy.MaxBackoff
	}
	return time.Duration(d)
}

// newAuthInterceptor returns a gRPC unary client interceptor that injects the auth token.
func newAuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "bunnymq-auth-token", token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
