package config

import (
	"fmt"
	"strconv"

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
}
