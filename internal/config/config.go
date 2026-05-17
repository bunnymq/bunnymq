package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// rawConfig mirrors Config but uses map[string]string for Peers, which viper
// can unmarshal from YAML/TOML integer-keyed maps without type errors.
type rawConfig struct {
	NodeID         uint64            `mapstructure:"nodeid"`
	RaftAddress    string            `mapstructure:"raftaddress"`
	ManagementAddr string            `mapstructure:"managementaddr"`
	DataAddr       string            `mapstructure:"dataaddr"`
	DataDir        string            `mapstructure:"datadir"`
	RaftRTTMs      uint64            `mapstructure:"raftrttms"`
	Peers          map[string]string `mapstructure:"peers"`
	AuthTokens     []string          `mapstructure:"authtokens"`
	Storage        StorageConfig     `mapstructure:"storage"`
	Coordinator    CoordinatorConfig `mapstructure:"coordinator"`
	MetricsAddr    string            `mapstructure:"metricsaddr"`
	PprofAddr      string            `mapstructure:"pprofaddr"`
}

// Load reads broker configuration from a file and returns a Config.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	var raw rawConfig
	if err := v.Unmarshal(&raw); err != nil {
		return nil, err
	}

	peers := make(map[uint64]string, len(raw.Peers))
	for k, addr := range raw.Peers {
		id, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer node id %q: %w", k, err)
		}
		peers[id] = addr
	}

	return &Config{
		NodeID:         raw.NodeID,
		RaftAddress:    raw.RaftAddress,
		ManagementAddr: raw.ManagementAddr,
		DataAddr:       raw.DataAddr,
		DataDir:        raw.DataDir,
		RaftRTTMs:      raw.RaftRTTMs,
		Peers:          peers,
		AuthTokens:     raw.AuthTokens,
		Storage:        raw.Storage,
		Coordinator:    raw.Coordinator,
		MetricsAddr:    raw.MetricsAddr,
		PprofAddr:      raw.PprofAddr,
	}, nil
}

// Config holds the broker configuration loaded from a YAML/TOML file and CLI flags.
type Config struct {
	NodeID         uint64
	RaftAddress    string
	ManagementAddr string
	DataAddr       string
	DataDir        string
	RaftRTTMs      uint64
	Peers          map[uint64]string
	AuthTokens     []string
	Storage        StorageConfig
	Coordinator    CoordinatorConfig
	MetricsAddr    string
	PprofAddr      string
}

// StorageConfig holds per-partition storage configuration.
type StorageConfig struct {
	SegmentMaxBytes          int64
	IndexSampleBytes         int
	RetentionCheckIntervalMs int64
	DefaultRetentionMs       int64
	DefaultRetentionBytes    int64
}

// CoordinatorConfig holds coordinator timing configuration.
type CoordinatorConfig struct {
	ReconcileIntervalMs    int64
	LeaderCheckIntervalMs  int64
	BootstrapTimeoutMs     int64
	EagerReconcileOnCreate bool
	GroupSweepIntervalMs   int64
}

// LoadFromEnv builds a Config from environment variables. Required vars:
// NODE_ID, RAFT_ADDR, DATA_DIR, INITIAL_MEMBERS.
// Optional: MGMT_ADDR (default :9091), DATA_ADDR (default :9092), RAFT_RTT_MS (default 200),
// GROUP_SWEEP_INTERVAL_MS, RETENTION_CHECK_INTERVAL_MS.
func LoadFromEnv() (*Config, error) {
	nodeID, err := strconv.ParseUint(os.Getenv("NODE_ID"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("NODE_ID: %w", err)
	}

	peers, err := parseInitialMembers(os.Getenv("INITIAL_MEMBERS"))
	if err != nil {
		return nil, fmt.Errorf("INITIAL_MEMBERS: %w", err)
	}

	rttMs := uint64(200)
	if raw := os.Getenv("RAFT_RTT_MS"); raw != "" {
		if rttMs, err = strconv.ParseUint(raw, 10, 64); err != nil {
			return nil, fmt.Errorf("RAFT_RTT_MS: %w", err)
		}
	}

	mgmtAddr := os.Getenv("MGMT_ADDR")
	if mgmtAddr == "" {
		mgmtAddr = ":9091"
	}
	dataAddr := os.Getenv("DATA_ADDR")
	if dataAddr == "" {
		dataAddr = ":9092"
	}

	var groupSweepIntervalMs int64
	if raw := os.Getenv("GROUP_SWEEP_INTERVAL_MS"); raw != "" {
		v, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("GROUP_SWEEP_INTERVAL_MS: %w", parseErr)
		}
		groupSweepIntervalMs = v
	}

	var retentionCheckIntervalMs int64
	if raw := os.Getenv("RETENTION_CHECK_INTERVAL_MS"); raw != "" {
		v, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("RETENTION_CHECK_INTERVAL_MS: %w", parseErr)
		}
		retentionCheckIntervalMs = v
	}

	return &Config{
		NodeID:         nodeID,
		RaftAddress:    os.Getenv("RAFT_ADDR"),
		ManagementAddr: mgmtAddr,
		DataAddr:       dataAddr,
		DataDir:        os.Getenv("DATA_DIR"),
		RaftRTTMs:      rttMs,
		Peers:          peers,
		Coordinator: CoordinatorConfig{
			GroupSweepIntervalMs: groupSweepIntervalMs,
		},
		Storage: StorageConfig{
			RetentionCheckIntervalMs: retentionCheckIntervalMs,
		},
	}, nil
}

// parseInitialMembers parses "1=host:port,2=host:port" into a peer map.
func parseInitialMembers(s string) (map[uint64]string, error) {
	if s == "" {
		return nil, nil
	}
	peers := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid entry %q", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(kv[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid node id %q: %w", kv[0], err)
		}
		peers[id] = strings.TrimSpace(kv[1])
	}
	return peers, nil
}
