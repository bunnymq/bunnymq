package cluster

import (
	"testing"

	"github.com/bunnymq/bunnymq/internal/metadata"
)

func tp(topic string, partitionID int32) metadata.TopicPartition {
	return metadata.TopicPartition{Topic: topic, PartitionID: partitionID}
}

func TestRangeAssign_EightPartitionsThreeMembers(t *testing.T) {
	result := rangeAssign(
		[]string{"m-b", "m-a", "m-c"},
		[]string{"t"},
		map[string]int32{"t": 8},
	)
	want := map[string][]metadata.TopicPartition{
		"m-a": {tp("t", 0), tp("t", 1), tp("t", 2)},
		"m-b": {tp("t", 3), tp("t", 4), tp("t", 5)},
		"m-c": {tp("t", 6), tp("t", 7)},
	}
	assertAssignment(t, result, want)
}

func TestRangeAssign_EvenDistribution(t *testing.T) {
	result := rangeAssign(
		[]string{"m1", "m2", "m3"},
		[]string{"t"},
		map[string]int32{"t": 6},
	)
	want := map[string][]metadata.TopicPartition{
		"m1": {tp("t", 0), tp("t", 1)},
		"m2": {tp("t", 2), tp("t", 3)},
		"m3": {tp("t", 4), tp("t", 5)},
	}
	assertAssignment(t, result, want)
}

func TestRangeAssign_MoreMembersThanPartitions(t *testing.T) {
	result := rangeAssign(
		[]string{"m1", "m2", "m3"},
		[]string{"t"},
		map[string]int32{"t": 1},
	)
	if len(result) != 3 {
		t.Fatalf("expected 3 members in result, got %d", len(result))
	}
	total := 0
	for _, parts := range result {
		total += len(parts)
	}
	if total != 1 {
		t.Fatalf("expected exactly 1 partition assigned total, got %d", total)
	}
	// all members must be present (even those with 0 partitions)
	for _, m := range []string{"m1", "m2", "m3"} {
		if _, ok := result[m]; !ok {
			t.Errorf("member %q missing from result", m)
		}
	}
}

func TestRangeAssign_SingleMember(t *testing.T) {
	result := rangeAssign(
		[]string{"only"},
		[]string{"t"},
		map[string]int32{"t": 5},
	)
	if len(result["only"]) != 5 {
		t.Fatalf("expected 5 partitions for single member, got %d", len(result["only"]))
	}
}

func TestRangeAssign_ZeroMembers(t *testing.T) {
	result := rangeAssign(nil, []string{"t"}, map[string]int32{"t": 4})
	if result == nil {
		t.Fatal("expected non-nil map")
	}
	if len(result) != 0 {
		t.Fatalf("expected empty map, got %v", result)
	}
}

func TestRangeAssign_MultiTopic(t *testing.T) {
	result := rangeAssign(
		[]string{"m1", "m2"},
		[]string{"topicA", "topicB"},
		map[string]int32{"topicA": 4, "topicB": 4},
	)
	if len(result["m1"]) != 4 {
		t.Errorf("m1: expected 4 total partitions, got %d", len(result["m1"]))
	}
	if len(result["m2"]) != 4 {
		t.Errorf("m2: expected 4 total partitions, got %d", len(result["m2"]))
	}
}

func TestRangeAssign_Deterministic(t *testing.T) {
	members := []string{"m-c", "m-a", "m-b"}
	topics := []string{"z", "a"}
	counts := map[string]int32{"z": 6, "a": 3}

	r1 := rangeAssign(members, topics, counts)
	r2 := rangeAssign([]string{"m-a", "m-b", "m-c"}, []string{"a", "z"}, counts)

	for _, m := range []string{"m-a", "m-b", "m-c"} {
		p1 := r1[m]
		p2 := r2[m]
		if len(p1) != len(p2) {
			t.Errorf("member %q: r1 has %d partitions, r2 has %d", m, len(p1), len(p2))
			continue
		}
		for i := range p1 {
			if p1[i] != p2[i] {
				t.Errorf("member %q partition[%d]: r1=%v r2=%v", m, i, p1[i], p2[i])
			}
		}
	}
}

// assertAssignment checks that result matches want exactly.
func assertAssignment(t *testing.T, result, want map[string][]metadata.TopicPartition) {
	t.Helper()
	if len(result) != len(want) {
		t.Fatalf("member count mismatch: got %d want %d\nresult=%v\nwant=%v", len(result), len(want), result, want)
	}
	for m, wantParts := range want {
		gotParts, ok := result[m]
		if !ok {
			t.Errorf("member %q missing from result", m)
			continue
		}
		if len(gotParts) != len(wantParts) {
			t.Errorf("member %q: got %d parts want %d\n  got=%v\n  want=%v", m, len(gotParts), len(wantParts), gotParts, wantParts)
			continue
		}
		for i := range wantParts {
			if gotParts[i] != wantParts[i] {
				t.Errorf("member %q partition[%d]: got %v want %v", m, i, gotParts[i], wantParts[i])
			}
		}
	}
}
