package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"

	"github.com/bunnymq/bunnymq/internal/api"
	cmetrics "github.com/bunnymq/bunnymq/internal/cluster"
	"github.com/bunnymq/bunnymq/internal/config"
	clustercoord "github.com/bunnymq/bunnymq/internal/coordinator/cluster"
	datacoord "github.com/bunnymq/bunnymq/internal/coordinator/data"
	groupcoord "github.com/bunnymq/bunnymq/internal/coordinator/group"
	"github.com/bunnymq/bunnymq/internal/metadata"
	"github.com/bunnymq/bunnymq/internal/partition"
	"github.com/bunnymq/bunnymq/internal/raft"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--health-check" {
		healthCheck()
	}

	var cfg *config.Config
	var err error
	if len(os.Args) >= 2 && os.Args[1] != "--health-check" {
		cfg, err = config.Load(os.Args[1])
	} else {
		cfg, err = config.LoadFromEnv()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	if err := run(cfg, logger); err != nil {
		logger.Fatal("broker exited with error", zap.Error(err))
	}
}

// healthCheck dials the management port and exits 0 if reachable, 1 otherwise.
func healthCheck() {
	addr := os.Getenv("MGMT_ADDR")
	if addr == "" {
		addr = ":9091"
	}
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
		os.Exit(1)
	}
	conn.Close() //nolint:errcheck
	os.Exit(0)
}

// brokerHost wraps raft.Host and pre-wires a partition factory so that
// ClusterCoordinator (which uses the factory-less StartPartitionShard interface)
// can start partition shards without knowing the storage details.
type brokerHost struct {
	host    *raft.Host
	factory sm.CreateOnDiskStateMachineFunc
}

func (bh *brokerHost) StartMetadataShard(members map[uint64]string, join bool, factory sm.CreateStateMachineFunc) error {
	return bh.host.StartMetadataShard(members, join, factory)
}

func (bh *brokerHost) StartPartitionShard(shardID uint64, members map[uint64]string, join bool) error {
	return bh.host.StartPartitionShard(shardID, members, join, bh.factory)
}

func (bh *brokerHost) StopPartitionShard(shardID uint64) error {
	return bh.host.StopPartitionShard(shardID)
}

func (bh *brokerHost) GetLeaderID(shardID uint64) (uint64, uint64, bool, error) {
	return bh.host.GetLeaderID(shardID)
}

func (bh *brokerHost) SyncProposeMetadata(ctx context.Context, cmd metadata.MetadataCommand) (sm.Result, error) {
	return bh.host.SyncProposeMetadata(ctx, cmd)
}

func (bh *brokerHost) LookupMetadata(ctx context.Context, q metadata.MetadataQuery) (any, error) {
	return bh.host.LookupMetadata(ctx, q)
}

func (bh *brokerHost) ProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) error {
	return bh.host.ProposePartition(ctx, shardID, cmd)
}

func (bh *brokerHost) LookupPartition(ctx context.Context, shardID uint64, q partition.PartitionQuery) (any, error) {
	return bh.host.LookupPartition(ctx, shardID, q)
}

func (bh *brokerHost) SyncProposePartition(ctx context.Context, shardID uint64, cmd partition.PartitionCommand) (sm.Result, error) {
	return bh.host.SyncProposePartition(ctx, shardID, cmd)
}

func run(cfg *config.Config, logger *zap.Logger) error {
	partFactory := func(shardID uint64, _ uint64) sm.IOnDiskStateMachine {
		dir := filepath.Join(cfg.DataDir, "partitions", fmt.Sprintf("shard-%d", shardID))
		if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
			panic(fmt.Sprintf("mkdir partition dir: %v", mkErr))
		}
		sidecarPath := filepath.Join(dir, "applied.idx")
		return partition.NewPartitionFSM(dir, sidecarPath, &cfg.Storage)
	}

	raftHost, err := raft.NewHost(&raft.Config{
		DataDir:     cfg.DataDir,
		RaftAddress: cfg.RaftAddress,
		NodeID:      cfg.NodeID,
		RaftRTTMs:   cfg.RaftRTTMs,
		Peers:       cfg.Peers,
	})
	if err != nil {
		return fmt.Errorf("create raft host: %w", err)
	}
	defer raftHost.Close() //nolint:errcheck

	bh := &brokerHost{host: raftHost, factory: partFactory}

	dc := datacoord.NewDataCoordinator(datacoord.DataCoordinatorConfig{
		NodeID:                cfg.NodeID,
		NodeAddressCacheTTLMs: 5000,
	}, bh, logger.Named("data"))

	cc := clustercoord.NewClusterCoordinator(clustercoord.CoordinatorConfig{
		NodeID:                 cfg.NodeID,
		RaftAddress:            cfg.RaftAddress,
		DataAddr:               cfg.DataAddr,
		DataDir:                cfg.DataDir,
		Peers:                  cfg.Peers,
		BootstrapTimeoutMs:     cfg.Coordinator.BootstrapTimeoutMs,
		ReconcileIntervalMs:    cfg.Coordinator.ReconcileIntervalMs,
		LeaderCheckIntervalMs:  cfg.Coordinator.LeaderCheckIntervalMs,
		EagerReconcileOnCreate: cfg.Coordinator.EagerReconcileOnCreate,
	}, bh, dc, cmetrics.NoopRaftMetrics(), logger.Named("cluster"))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := cc.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	gc := groupcoord.NewGroupCoordinator(groupcoord.GroupCoordinatorConfig{
		MetadataShardID: 0,
		ThisNodeID:      cfg.NodeID,
		SweepIntervalMs: cfg.Coordinator.GroupSweepIntervalMs,
	}, bh)
	gc.RebuildHeartbeatTable()
	gc.Start(ctx)

	isMetadataLeader := func() (bool, string) {
		leaderID, _, valid, leaderErr := bh.GetLeaderID(0)
		if leaderErr != nil || !valid {
			return false, ""
		}
		if leaderID == cfg.NodeID {
			return true, ""
		}
		raw, lookupErr := bh.LookupMetadata(context.Background(), metadata.MetadataQuery{
			Type:   metadata.QueryGetNode,
			NodeID: leaderID,
		})
		if lookupErr != nil || raw == nil {
			return false, ""
		}
		nodeInfo := raw.(*metadata.NodeInfo)
		return false, nodeInfo.Address
	}

	mgmtAddr := cfg.ManagementAddr
	if mgmtAddr == "" {
		mgmtAddr = ":9091"
	}
	dataAddr := cfg.DataAddr
	if dataAddr == "" {
		dataAddr = ":9092"
	}

	mgmtLn, err := net.Listen("tcp", mgmtAddr)
	if err != nil {
		return fmt.Errorf("listen management %s: %w", mgmtAddr, err)
	}
	dataLn, err := net.Listen("tcp", dataAddr)
	if err != nil {
		mgmtLn.Close() //nolint:errcheck
		return fmt.Errorf("listen data %s: %w", dataAddr, err)
	}

	mgmtSrv := api.NewManagementServer(api.ServerConfig{Addr: mgmtAddr, AuthTokens: cfg.AuthTokens}, cc, logger.Named("mgmt-api"))
	dataSrv := api.NewDataServer(api.ServerConfig{Addr: dataAddr, AuthTokens: cfg.AuthTokens}, dc, gc, isMetadataLeader, logger.Named("data-api"))

	go mgmtSrv.Serve(mgmtLn)  //nolint:errcheck
	go dataSrv.Serve(dataLn)  //nolint:errcheck

	logger.Info("broker ready",
		zap.Uint64("node_id", cfg.NodeID),
		zap.String("raft_addr", cfg.RaftAddress),
		zap.String("mgmt_addr", mgmtAddr),
		zap.String("data_addr", dataAddr),
	)

	<-ctx.Done()

	mgmtSrv.GracefulStop()
	dataSrv.GracefulStop()

	return nil
}
