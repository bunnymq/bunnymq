package main

import (
	"strings"
	"testing"

	"github.com/bunnymq/bunnymq/pkg/client"
)

func TestPrintTopicInfo(t *testing.T) {
	info := client.TopicInfo{
		Name:              "orders",
		PartitionCount:    3,
		ReplicationFactor: 2,
		RetentionMs:       86400000,
		RetentionBytes:    -1,
		CreatedAtMs:       1700000000000,
	}
	var buf strings.Builder
	printTopicInfo(&buf, info)
	out := buf.String()

	for _, want := range []string{"Name:", "orders", "86400000", "-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("printTopicInfo output missing %q; got:\n%s", want, out)
		}
	}
}

func TestPrintTopicTable(t *testing.T) {
	topics := []client.TopicInfo{
		{Name: "alpha", PartitionCount: 1, ReplicationFactor: 1, RetentionMs: -1, RetentionBytes: -1},
		{Name: "beta", PartitionCount: 4, ReplicationFactor: 3, RetentionMs: 3600000, RetentionBytes: 1024},
	}
	var buf strings.Builder
	printTopicTable(&buf, topics)
	out := buf.String()

	for _, want := range []string{"NAME", "alpha", "beta"} {
		if !strings.Contains(out, want) {
			t.Errorf("printTopicTable output missing %q; got:\n%s", want, out)
		}
	}
}

func TestPrintTopicDescription(t *testing.T) {
	desc := client.TopicDescription{
		Topic: client.TopicInfo{
			Name:              "events",
			PartitionCount:    2,
			ReplicationFactor: 2,
			RetentionMs:       -1,
			RetentionBytes:    -1,
		},
		Partitions: []client.PartitionInfo{
			{PartitionID: 0, ShardID: 10, LeaderNodeID: 1, LeaderEpoch: 5},
			{PartitionID: 1, ShardID: 11, LeaderNodeID: 2, LeaderEpoch: 3},
		},
	}
	var buf strings.Builder
	printTopicDescription(&buf, desc)
	out := buf.String()

	for _, want := range []string{"events", "PARTITION", "0", "1"} {
		if !strings.Contains(out, want) {
			t.Errorf("printTopicDescription output missing %q; got:\n%s", want, out)
		}
	}
}

func TestPrintPartitionTable(t *testing.T) {
	parts := []client.PartitionInfoWithOffsets{
		{Info: client.PartitionInfo{PartitionID: 0, LeaderNodeID: 1, ShardID: 10}, EarliestOffset: 0, LatestOffset: 100},
		{Info: client.PartitionInfo{PartitionID: 1, LeaderNodeID: 2, ShardID: 11}, EarliestOffset: 50, LatestOffset: 200},
	}
	var buf strings.Builder
	printPartitionTable(&buf, parts)
	out := buf.String()

	for _, want := range []string{"PARTITION", "100", "200", "50"} {
		if !strings.Contains(out, want) {
			t.Errorf("printPartitionTable output missing %q; got:\n%s", want, out)
		}
	}
}

func TestPrintClusterTable(t *testing.T) {
	cd := client.ClusterDescription{
		Nodes: []client.NodeDescriptor{
			{NodeID: 1, Address: "10.0.0.1:9091"},
			{NodeID: 2, Address: "10.0.0.2:9091"},
		},
	}
	var buf strings.Builder
	printClusterTable(&buf, cd)
	out := buf.String()

	for _, want := range []string{"NODE-ID", "10.0.0.1:9091", "10.0.0.2:9091"} {
		if !strings.Contains(out, want) {
			t.Errorf("printClusterTable output missing %q; got:\n%s", want, out)
		}
	}
}
