//go:build integration && docker

package integration

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/bunnymq/bunnymq/internal/storage"
	"github.com/bunnymq/bunnymq/pkg/client"
)

// nodeIDToContainer maps broker NodeID to docker-compose service name.
func nodeIDToContainer(nodeID uint64) string {
	return fmt.Sprintf("broker%d", nodeID)
}

// dockerComposeArgs returns the command prefix for docker compose (v2 or v1).
func dockerComposeArgs() []string {
	if err := exec.Command("docker", "compose", "version").Run(); err == nil {
		return []string{"docker", "compose"}
	}
	return []string{"docker-compose"}
}

// runCompose runs a docker compose subcommand with the given service arguments.
func runCompose(subCmd string, services ...string) error {
	prefix := dockerComposeArgs()
	args := append(prefix[1:], subCmd)
	args = append(args, services...)
	cmd := exec.Command(prefix[0], args...)
	return cmd.Run()
}

func TestDocker_LeaderFailover(t *testing.T) {
	const topic = "failover-docker"

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
			RequestTimeout:   10 * time.Second,
			RetryPolicy: client.RetryPolicy{
				MaxRetries:     10,
				InitialBackoff: 200 * time.Millisecond,
				MaxBackoff:     5 * time.Second,
				BackoffFactor:  2.0,
			},
		},
		DefaultAcks: client.AcksAll,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	produced := make([]producedBatch, 0, 10)

	// Produce 5 batches; expect offsets 0–4.
	for b := 0; b < 5; b++ {
		val := fmt.Sprintf("batch-%d", b)
		batchData, encErr := storage.EncodeBatch([]storage.Record{
			{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
		})
		if encErr != nil {
			t.Fatalf("encode batch %d: %v", b, encErr)
		}
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		offset, sendErr := prod.SendBatch(sendCtx, topic, 0, batchData, client.AcksAll)
		sendCancel()
		if sendErr != nil {
			t.Fatalf("SendBatch batch %d: %v", b, sendErr)
		}
		if offset != int64(b) {
			t.Errorf("batch %d: got offset %d, want %d", b, offset, b)
		}
		produced = append(produced, producedBatch{offset: offset, value: val})
	}

	// Identify the leader container for partition 0.
	descCtx, descCancel := context.WithTimeout(ctx, 5*time.Second)
	desc, err := ac.DescribeTopic(descCtx, topic)
	descCancel()
	if err != nil {
		t.Fatalf("DescribeTopic: %v", err)
	}
	var leaderNodeID uint64
	for _, p := range desc.Partitions {
		if p.PartitionID == 0 {
			leaderNodeID = p.LeaderNodeID
		}
	}
	if leaderNodeID == 0 {
		t.Fatal("partition 0 has no leader")
	}
	leaderContainer := nodeIDToContainer(leaderNodeID)

	// Stop the leader container.
	if err := runCompose("stop", leaderContainer); err != nil {
		t.Fatalf("docker compose stop %s: %v", leaderContainer, err)
	}
	// Ensure the container is restarted even if the test fails.
	t.Cleanup(func() {
		_ = runCompose("start", leaderContainer)
	})

	// Produce batch 5 — must succeed within MaxRetries after new leader election.
	val5 := "batch-5"
	batchData5, encErr5 := storage.EncodeBatch([]storage.Record{
		{TimestampMs: time.Now().UnixMilli(), Value: []byte(val5)},
	})
	if encErr5 != nil {
		t.Fatalf("encode batch 5: %v", encErr5)
	}
	sendCtx5, sendCancel5 := context.WithTimeout(ctx, 90*time.Second)
	offset5, sendErr5 := prod.SendBatch(sendCtx5, topic, 0, batchData5, client.AcksAll)
	sendCancel5()
	if sendErr5 != nil {
		t.Fatalf("SendBatch after failover: %v", sendErr5)
	}
	if offset5 != 5 {
		t.Errorf("batch 5 after failover: got offset %d, want 5", offset5)
	}
	produced = append(produced, producedBatch{offset: offset5, value: val5})

	// Produce 4 more batches (6–9); all must succeed.
	for b := 6; b < 10; b++ {
		val := fmt.Sprintf("batch-%d", b)
		batchData, encErr := storage.EncodeBatch([]storage.Record{
			{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
		})
		if encErr != nil {
			t.Fatalf("encode batch %d: %v", b, encErr)
		}
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		offset, sendErr := prod.SendBatch(sendCtx, topic, 0, batchData, client.AcksAll)
		sendCancel()
		if sendErr != nil {
			t.Fatalf("SendBatch batch %d: %v", b, sendErr)
		}
		produced = append(produced, producedBatch{offset: offset, value: val})
	}

	// Fetch from offset 0; all 10 batches must be returned in order.
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

	cons.Seek(topic, 0, 0)

	received := consumeAtLeast(t, cons, 1, 10, 30*time.Second)
	if len(received[0]) < 10 {
		t.Fatalf("got %d records, want 10", len(received[0]))
	}
	for i, rec := range received[0][:10] {
		want := produced[i].value
		if got := string(rec.Value); got != want {
			t.Errorf("record %d: got %q, want %q", i, got, want)
		}
	}

	// Restart the killed broker and verify it rejoins the cluster.
	if err := runCompose("start", leaderContainer); err != nil {
		t.Fatalf("docker compose start %s: %v", leaderContainer, err)
	}

	waitDockerClusterReady(t, 30*time.Second)

	clusterCtx, clusterCancel := context.WithTimeout(ctx, 5*time.Second)
	clusterDesc, err := ac.DescribeCluster(clusterCtx)
	clusterCancel()
	if err != nil {
		t.Fatalf("DescribeCluster after rejoin: %v", err)
	}
	if len(clusterDesc.Nodes) != 3 {
		t.Errorf("after rejoin: got %d nodes, want 3", len(clusterDesc.Nodes))
	}
}

func TestDocker_FullClusterRestart(t *testing.T) {
	const (
		topic          = "persist-topic"
		partitionCount = 2
		batchesPerPart = 20
		extraBatches   = 5
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

	// Produce 20 batches to each of 2 partitions (40 total).
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

	// Stop all 3 brokers simultaneously.
	if err := runCompose("stop", "broker1", "broker2", "broker3"); err != nil {
		t.Fatalf("docker compose stop all: %v", err)
	}
	t.Cleanup(func() {
		_ = runCompose("start", "broker1", "broker2", "broker3")
	})

	time.Sleep(2 * time.Second)

	// Restart all 3 brokers.
	if err := runCompose("start", "broker1", "broker2", "broker3"); err != nil {
		t.Fatalf("docker compose start all: %v", err)
	}

	// Allow longer timeout for log replay on all three nodes.
	waitDockerClusterReady(t, 60*time.Second)

	// Verify the topic survived the restart.
	listCtx, listCancel := context.WithTimeout(ctx, 10*time.Second)
	topics, listErr := ac.ListTopics(listCtx)
	listCancel()
	if listErr != nil {
		t.Fatalf("ListTopics after restart: %v", listErr)
	}
	found := false
	for _, ti := range topics {
		if ti.Name == topic {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("topic %q not found after cluster restart", topic)
	}

	waitDockerPartitionsLeaders(t, topic, partitionCount, 15*time.Second)

	// Fetch all 20 pre-restart batches per partition from offset 0.
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

	// Produce 5 more batches per partition; offsets must be sequential (no gap after restart).
	for p := 0; p < partitionCount; p++ {
		prevOffset := produced[p][len(produced[p])-1].offset
		for b := 0; b < extraBatches; b++ {
			val := fmt.Sprintf("post-restart-part-%d-batch-%d", p, b)
			batchData, encErr := storage.EncodeBatch([]storage.Record{
				{TimestampMs: time.Now().UnixMilli(), Value: []byte(val)},
			})
			if encErr != nil {
				t.Fatalf("encode post-restart p=%d b=%d: %v", p, b, encErr)
			}
			sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
			offset, sendErr := prod.SendBatch(sendCtx, topic, int32(p), batchData, client.AcksAll)
			sendCancel()
			if sendErr != nil {
				t.Fatalf("post-restart SendBatch p=%d b=%d: %v", p, b, sendErr)
			}
			wantOffset := prevOffset + int64(b) + 1
			if offset != wantOffset {
				t.Errorf("post-restart p=%d b=%d: offset=%d, want %d", p, b, offset, wantOffset)
			}
		}
	}
}
