package cluster

import (
	"sort"

	"github.com/bunnymq/bunnymq/internal/metadata"
)

// rangeAssign computes a range-based partition assignment.
// It does not mutate the caller's slices.
// Members with zero partitions assigned still appear in the returned map with an empty slice.
func rangeAssign(memberIDs []string, topics []string, partitionCounts map[string]int32) map[string][]metadata.TopicPartition {
	if len(memberIDs) == 0 {
		return map[string][]metadata.TopicPartition{}
	}

	sorted := make([]string, len(memberIDs))
	copy(sorted, memberIDs)
	sort.Strings(sorted)

	sortedTopics := make([]string, len(topics))
	copy(sortedTopics, topics)
	sort.Strings(sortedTopics)

	result := make(map[string][]metadata.TopicPartition, len(sorted))
	for _, m := range sorted {
		result[m] = []metadata.TopicPartition{}
	}

	for _, topic := range sortedTopics {
		nPartitions := int(partitionCounts[topic])
		if nPartitions == 0 {
			continue
		}
		nMembers := len(sorted)
		base := nPartitions / nMembers
		remainder := nPartitions % nMembers

		cursor := 0
		for i, memberID := range sorted {
			count := base
			if i < remainder {
				count++
			}
			for p := cursor; p < cursor+count; p++ {
				result[memberID] = append(result[memberID], metadata.TopicPartition{
					Topic:       topic,
					PartitionID: int32(p),
				})
			}
			cursor += count
		}
	}

	return result
}
