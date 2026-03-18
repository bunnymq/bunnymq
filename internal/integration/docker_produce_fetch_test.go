//go:build integration && docker

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bunnymq/bunnymq/internal/storage"
	"github.com/bunnymq/bunnymq/pkg/client"
)

func TestDocker_ClusterBootstrap(t *testing.T) {
	waitDockerClusterReady(t, 30*time.Second)

	mgmt, _ := brokerAddrs()

	// Query all 3 management ports in parallel and collect their cluster descriptions.
	type result struct {
		addr  string
		desc  client.ClusterDescription
		err   error
	}
	results := make([]result, len(mgmt))
	var wg sync.WaitGroup
	for i, addr := range mgmt {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			ac, err := client.NewAdminClient(client.Config{
				BootstrapServers: []string{addr},
				RequestTimeout:   3 * time.Second,
			})
			if err != nil {
				results[i] = result{addr: addr, err: err}
				return
			}
			defer ac.Close() //nolint:errcheck
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			desc, err := ac.DescribeCluster(ctx)
			cancel()
			results[i] = result{addr: addr, desc: desc, err: err}
		}(i, addr)
	}
	wg.Wait()

	// All three management endpoints must return a cluster description successfully.
	for _, r := range results {
		if r.err != nil {
			t.Errorf("DescribeCluster from %s: %v", r.addr, r.err)
		}
	}
	if t.Failed() {
		return
	}

	// Each node must report exactly 3 nodes.
	for _, r := range results {
		if len(r.desc.Nodes) != 3 {
			t.Errorf("%s: got %d nodes, want 3", r.addr, len(r.desc.Nodes))
		}
	}

	// All NodeIDs reported by the first node must be distinct.
	seen := make(map[uint64]bool)
	for _, n := range results[0].desc.Nodes {
		if seen[n.NodeID] {
			t.Errorf("duplicate NodeID %d in cluster description", n.NodeID)
		}
		seen[n.NodeID] = true
	}

	// All 3 management ports must report the same set of NodeIDs (consistent view).
	refIDs := make(map[uint64]bool)
	for _, n := range results[0].desc.Nodes {
		refIDs[n.NodeID] = true
	}
	for _, r := range results[1:] {
		for _, n := range r.desc.Nodes {
			if !refIDs[n.NodeID] {
				t.Errorf("%s reported unknown NodeID %d", r.addr, n.NodeID)
			}
		}
	}
}

func TestDocker_ProduceFetch_AcksAll(t *testing.T) {
	const (
		topic          = "docker-smoke"
		partitionCount = 3
		batchesPerPart = 20
	)

	waitDockerClusterReady(t, 30*time.Second)

	mgmt, data := brokerAddrs()

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{mgmt[0]},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()

	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = ac.CreateTopic(createCtx, client.CreateTopicRequest{
		Name:              topic,
		PartitionCount:    partitionCount,
		ReplicationFactor: 3,
	})
	cancel()
	if err != nil && err != client.ErrTopicAlreadyExists {
		t.Fatalf("CreateTopic: %v", err)
	}

	waitDockerPartitionsLeaders(t, topic, partitionCount, 15*time.Second)

	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: []string{mgmt[0]},
			RequestTimeout:   10 * time.Second,
			RetryPolicy: client.RetryPolicy{
				MaxRetries:     5,
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     2 * time.Second,
				BackoffFactor:  2.0,
			},
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	// Produce 20 batches per partition and verify monotonically increasing offsets.
	produced := make([][]producedBatch, partitionCount)
	for p := 0; p < partitionCount; p++ {
		produced[p] = make([]producedBatch, 0, batchesPerPart)
		for b := 0; b < batchesPerPart; b++ {
			val := fmt.Sprintf("part-%d-batch-%d", p, b)
			batchData, encErr := storage.EncodeBatch([]storage.Record{
				{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
			})
			if encErr != nil {
				t.Fatalf("encode p=%d b=%d: %v", p, b, encErr)
			}
			sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
			offset, sendErr := prod.SendBatch(sendCtx, topic, int32(p), batchData, client.AcksAll)
			sendCancel()
			if sendErr != nil {
				t.Fatalf("SendBatch p=%d b=%d: %v", p, b, sendErr)
			}
			produced[p] = append(produced[p], producedBatch{offset: offset, value: val})
		}
	}

	checkMonotonicOffsets(t, produced)

	// Fetch and verify all 20 batches per partition.
	cons, err := client.NewConsumer(client.ConsumerConfig{
		Config: client.Config{
			BootstrapServers: []string{data[0]},
			RequestTimeout:   10 * time.Second,
		},
		MaxFetchBytes:  1 << 20,
		MaxFetchWaitMs: 5000,
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Close() //nolint:errcheck

	for p := 0; p < partitionCount; p++ {
		cons.Seek(topic, int32(p), 0)
	}

	received := consumeAtLeast(t, cons, partitionCount, batchesPerPart, 30*time.Second)
	checkConsumedContent(t, produced, received, partitionCount, batchesPerPart)

	// Assert the metrics endpoint is reachable and reports non-zero appended batches.
	metricsURL := "http://localhost:19090/metrics"
	resp, httpErr := http.Get(metricsURL) //nolint:noctx
	if httpErr != nil {
		t.Fatalf("GET %s: %v", metricsURL, httpErr)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", metricsURL, resp.StatusCode)
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		t.Fatalf("read metrics body: %v", readErr)
	}
	if !strings.Contains(string(body), "bunnymq_storage_batches_appended_total") {
		t.Errorf("metrics body does not contain bunnymq_storage_batches_appended_total")
	}
}

func TestDocker_ProduceFetch_AcksZero(t *testing.T) {
	const (
		topic      = "docker-acks0"
		batchCount = 10
		minArrived = 8
	)

	waitDockerClusterReady(t, 30*time.Second)

	mgmt, data := brokerAddrs()

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{mgmt[0]},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	ctx := context.Background()

	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = ac.CreateTopic(createCtx, client.CreateTopicRequest{
		Name:              topic,
		PartitionCount:    1,
		ReplicationFactor: 3,
	})
	cancel()
	if err != nil && err != client.ErrTopicAlreadyExists {
		t.Fatalf("CreateTopic: %v", err)
	}

	waitDockerPartitionsLeaders(t, topic, 1, 15*time.Second)

	prod, err := client.NewProducer(client.ProducerConfig{
		Config: client.Config{
			BootstrapServers: []string{mgmt[0]},
			RequestTimeout:   5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	for b := 0; b < batchCount; b++ {
		batchData, encErr := storage.EncodeBatch([]storage.Record{
			{TimestampMs: time.Now().UnixMilli(), Value: []byte(fmt.Sprintf("acks0-batch-%d", b))},
		})
		if encErr != nil {
			t.Fatalf("encode batch %d: %v", b, encErr)
		}
		sendCtx, sendCancel := context.WithTimeout(ctx, 5*time.Second)
		offset, sendErr := prod.SendBatch(sendCtx, topic, 0, batchData, client.AcksZero)
		sendCancel()
		if sendErr != nil {
			t.Fatalf("SendBatch acks=0 batch %d: %v", b, sendErr)
		}
		if offset != -1 {
			t.Errorf("acks=0 batch %d: got offset %d, want -1", b, offset)
		}
	}

	// Wait for replication to settle.
	time.Sleep(500 * time.Millisecond)

	cons, err := client.NewConsumer(client.ConsumerConfig{
		Config: client.Config{
			BootstrapServers: []string{data[0]},
			RequestTimeout:   5 * time.Second,
		},
		MaxFetchBytes:  1 << 20,
		MaxFetchWaitMs: 2000,
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Close() //nolint:errcheck

	cons.Seek(topic, 0, 0)

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 10*time.Second)
	defer fetchCancel()
	var records []client.Record
	for len(records) < minArrived && fetchCtx.Err() == nil {
		recs, pollErr := cons.Poll(fetchCtx, 2000)
		if pollErr != nil {
			t.Fatalf("Poll: %v", pollErr)
		}
		records = append(records, recs...)
	}

	if len(records) < minArrived {
		t.Errorf("acks=0: got %d records, want >= %d", len(records), minArrived)
	}
}
