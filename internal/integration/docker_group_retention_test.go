//go:build integration && docker

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bunnymq/bunnymq/internal/storage"
	"github.com/bunnymq/bunnymq/pkg/client"
)

func TestDocker_ConsumerGroupRebalance_OnKill(t *testing.T) {
	const (
		topic            = "docker-rebalance"
		partitionCount   = 4
		batchesPerPart   = 20
		sessionTimeoutMs = 5000
	)

	waitDockerClusterReady(t, 30*time.Second)

	mgmt, _ := brokerAddrs()
	ctx := context.Background()

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{mgmt[0]},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

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

	for p := 0; p < partitionCount; p++ {
		for b := 0; b < batchesPerPart; b++ {
			val := fmt.Sprintf("rebalance-p%d-b%d", p, b)
			batchData, encErr := storage.EncodeBatch([]storage.Record{
				{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
			})
			if encErr != nil {
				t.Fatalf("encode p=%d b=%d: %v", p, b, encErr)
			}
			sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
			_, sendErr := prod.SendBatch(sendCtx, topic, int32(p), batchData, client.AcksAll)
			sendCancel()
			if sendErr != nil {
				t.Fatalf("SendBatch p=%d b=%d: %v", p, b, sendErr)
			}
		}
	}

	newGroupConsumer := func() *client.Consumer {
		c, cerr := client.NewConsumer(client.ConsumerConfig{
			Config: client.Config{
				BootstrapServers: []string{mgmt[0]},
				RequestTimeout:   10 * time.Second,
			},
			GroupID:             "docker-group",
			MaxFetchBytes:       1 << 20,
			MaxFetchWaitMs:      1000,
			AutoOffsetReset:     client.OffsetResetEarliest,
			HeartbeatIntervalMs: 1000,
			SessionTimeoutMs:    sessionTimeoutMs,
		})
		if cerr != nil {
			t.Fatalf("new consumer: %v", cerr)
		}
		return c
	}

	cons1 := newGroupConsumer()
	if err = cons1.Subscribe([]string{topic}); err != nil {
		_ = cons1.Close()
		t.Fatalf("cons1.Subscribe: %v", err)
	}

	cons2 := newGroupConsumer()
	defer cons2.Close() //nolint:errcheck
	if err = cons2.Subscribe([]string{topic}); err != nil {
		t.Fatalf("cons2.Subscribe: %v", err)
	}

	// Allow both joins and the resulting rebalances to settle.
	time.Sleep(5 * time.Second)

	// Verify initial stable assignment: both consumers receive records from some partitions.
	initResults := groupCollect(t, []*client.Consumer{cons1, cons2}, partitionCount, 15*time.Second)
	if len(initResults[0]) == 0 {
		t.Fatal("cons1 received no partitions in initial assignment")
	}
	if len(initResults[1]) == 0 {
		t.Fatal("cons2 received no partitions in initial assignment")
	}

	// Kill cons1 — simulate crash without sending LeaveGroup.
	cons1.SimulateCrash()

	// Wait for cons2 to acquire all 4 partitions after cons1's session times out and
	// the coordinator sweep runs.
	// Budget: sessionTimeoutMs(5s) + GROUP_SWEEP_INTERVAL_MS(3s configured in docker-compose)
	// + heartbeat + rebalance margin = ~15s; use 30s to be safe.
	cons2AllParts := make(map[int32][]client.Record)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if len(cons2AllParts) >= partitionCount {
			break
		}
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		recs, pollErr := cons2.Poll(pollCtx, 2000)
		pollCancel()
		if pollErr != nil {
			t.Logf("cons2.Poll: %v", pollErr)
			continue
		}
		for _, r := range recs {
			cons2AllParts[r.PartitionID] = append(cons2AllParts[r.PartitionID], r)
		}
	}

	if len(cons2AllParts) < partitionCount {
		t.Errorf("cons2 covers %d partitions after cons1 crash, want %d", len(cons2AllParts), partitionCount)
	}
	for p := int32(0); p < partitionCount; p++ {
		if len(cons2AllParts[p]) == 0 {
			t.Errorf("cons2 received no records from partition %d after rebalance", p)
		}
	}

	commitOffsets := make(map[client.TP]int64, len(cons2AllParts))
	for pid, recs := range cons2AllParts {
		last := recs[len(recs)-1]
		commitOffsets[client.TP{Topic: topic, PartitionID: pid}] = last.Offset + 1
	}
	commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
	if commitErr := cons2.CommitOffsets(commitCtx, commitOffsets); commitErr != nil {
		t.Logf("CommitOffsets: %v", commitErr)
	}
	commitCancel()
}

func TestDocker_RetentionBySize(t *testing.T) {
	const (
		topic          = "retention-topic"
		retentionBytes = 2 * 1024 * 1024  // 2 MB
		targetBytes    = 6 * 1024 * 1024  // produce > 6 MB (3× retention threshold)
		payloadSize    = 1024             // 1 KB per record
	)

	waitDockerClusterReady(t, 30*time.Second)

	mgmt, _ := brokerAddrs()
	ctx := context.Background()

	ac, err := client.NewAdminClient(client.Config{
		BootstrapServers: []string{mgmt[0]},
		RequestTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new admin client: %v", err)
	}
	defer ac.Close() //nolint:errcheck

	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = ac.CreateTopic(createCtx, client.CreateTopicRequest{
		Name:              topic,
		PartitionCount:    1,
		ReplicationFactor: 3,
		RetentionBytes:    retentionBytes,
	})
	cancel()
	if err != nil && err != client.ErrTopicAlreadyExists {
		t.Fatalf("CreateTopic: %v", err)
	}

	waitDockerPartitionsLeaders(t, topic, 1, 15*time.Second)

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

	// Produce 1 KB batches until total encoded size exceeds 6 MB.
	payload := make([]byte, payloadSize)
	totalBytes := 0
	for totalBytes < targetBytes {
		batchData, encErr := storage.EncodeBatch([]storage.Record{
			{TimestampMs: time.Now().UnixMilli(), Value: payload},
		})
		if encErr != nil {
			t.Fatalf("encode batch: %v", encErr)
		}
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		_, sendErr := prod.SendBatch(sendCtx, topic, 0, batchData, client.AcksAll)
		sendCancel()
		if sendErr != nil {
			t.Fatalf("SendBatch: %v", sendErr)
		}
		totalBytes += len(batchData)
	}

	// Wait for retention enforcement to delete at least one segment (up to 30s).
	// Requires docker-compose brokers configured with RETENTION_CHECK_INTERVAL_MS=5000.
	metricsURL := "http://localhost:19090/metrics"
	retentionDeadline := time.Now().Add(30 * time.Second)
	deleted := false
	for time.Now().Before(retentionDeadline) && !deleted {
		resp, httpErr := http.Get(metricsURL) //nolint:noctx
		if httpErr != nil {
			time.Sleep(time.Second)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close() //nolint:errcheck
		if readErr != nil || resp.StatusCode != http.StatusOK {
			time.Sleep(time.Second)
			continue
		}
		if segmentsDeletedByBytes(string(body)) > 0 {
			deleted = true
		} else {
			time.Sleep(time.Second)
		}
	}
	if !deleted {
		t.Fatalf(`bunnymq_storage_segments_deleted_total{reason="bytes"} not > 0 in metrics within 30s`)
	}

	// Verify EarliestOffset has advanced past 0 via ListPartitions.
	listCtx, listCancel := context.WithTimeout(ctx, 10*time.Second)
	partitions, listErr := ac.ListPartitions(listCtx, topic)
	listCancel()
	if listErr != nil {
		t.Fatalf("ListPartitions: %v", listErr)
	}
	if len(partitions) == 0 {
		t.Fatalf("ListPartitions returned no partitions")
	}
	if partitions[0].EarliestOffset == 0 {
		t.Errorf("EarliestOffset is 0 after retention enforcement, want > 0")
	}
}

// segmentsDeletedByBytes parses Prometheus text format and returns the total value
// of bunnymq_storage_segments_deleted_total lines that carry reason="bytes".
func segmentsDeletedByBytes(body string) float64 {
	var total float64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "bunnymq_storage_segments_deleted_total{") {
			continue
		}
		if !strings.Contains(line, `reason="bytes"`) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		total += v
	}
	return total
}
