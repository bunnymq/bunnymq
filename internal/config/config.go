package config

import "github.com/spf13/viper"

// Load reads broker configuration from a file and returns a Config.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Config holds the broker configuration loaded from a YAML/TOML file and CLI flags.
type Config struct {
	NodeID       uint64
	RaftAddress  string
	DataDir      string
	RaftRTTMs    uint64
	Peers        map[uint64]string
	AuthTokens   []string
	Storage      StorageConfig
	Coordinator  CoordinatorConfig
}

// StorageConfig holds per-partition storage configuration.
type StorageConfig struct {
	SegmentMaxBytes           int64
	IndexSampleBytes          int
	RetentionCheckIntervalMs  int64
	DefaultRetentionMs        int64
	DefaultRetentionBytes     int64
}

// CoordinatorConfig holds coordinator timing configuration.
type CoordinatorConfig struct {
	ReconcileIntervalMs    int64
	LeaderCheckIntervalMs  int64
	BootstrapTimeoutMs     int64
	EagerReconcileOnCreate bool
}
