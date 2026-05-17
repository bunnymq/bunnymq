package main

import (
	"strings"
	"time"

	"github.com/bunnymq/bunnymq/pkg/client"
)

var (
	flagBrokers string
	flagToken   string
	flagTimeout time.Duration
)

func init() {
	rootCmd.PersistentFlags().StringVar(&flagBrokers, "brokers", "localhost:9091", "comma-separated list of broker addresses")
	rootCmd.PersistentFlags().StringVar(&flagToken, "token", "", "authentication token")
	rootCmd.PersistentFlags().DurationVar(&flagTimeout, "timeout", 10*time.Second, "request timeout")
}

func buildClientConfig() client.Config {
	servers := strings.Split(flagBrokers, ",")
	for i, s := range servers {
		servers[i] = strings.TrimSpace(s)
	}
	return client.Config{
		BootstrapServers: servers,
		AuthToken:        flagToken,
		RequestTimeout:   flagTimeout,
	}
}

func newAdminClient() (*client.AdminClient, error) {
	return client.NewAdminClient(buildClientConfig())
}
