//go:build integration && docker

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/bunnymq/bunnymq/pkg/client"
)

func brokerAddrs() (mgmt []string, data []string) {
	mgmt = []string{
		"localhost:19091",
		"localhost:29091",
		"localhost:39091",
	}
	data = []string{
		"localhost:19092",
		"localhost:29092",
		"localhost:39092",
	}
	return
}

func waitDockerClusterReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	mgmt, _ := brokerAddrs()
	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{mgmt[0]},
		RequestTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		desc, err := ac.DescribeCluster(ctx)
		cancel()
		if err == nil && len(desc.Nodes) >= 3 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("docker cluster did not reach 3 nodes within %s", timeout)
}

func waitDockerPartitionsLeaders(t *testing.T, topic string, count int, timeout time.Duration) {
	t.Helper()
	mgmt, _ := brokerAddrs()
	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{mgmt[0]},
		RequestTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client for waitDockerPartitionsLeaders: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		desc, err := ac.DescribeTopic(ctx, topic)
		cancel()
		if err == nil && allLeadersElected(desc.Partitions, count) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("not all %d partitions of %q had leaders within %s", count, topic, timeout)
}
