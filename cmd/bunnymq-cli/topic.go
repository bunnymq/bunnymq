package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/bunnymq/bunnymq/pkg/client"
	"github.com/spf13/cobra"
)

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Manage topics",
}

var topicCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		partitions, _ := cmd.Flags().GetInt32("partitions")
		rf, _ := cmd.Flags().GetInt32("replication-factor")
		retMs, _ := cmd.Flags().GetInt64("retention-ms")
		retBytes, _ := cmd.Flags().GetInt64("retention-bytes")

		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		info, err := ac.CreateTopic(context.Background(), client.CreateTopicRequest{
			Name:              name,
			PartitionCount:    partitions,
			ReplicationFactor: rf,
			RetentionMs:       retMs,
			RetentionBytes:    retBytes,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		printTopicInfo(os.Stdout, info)
		return nil
	},
}

var topicDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")

		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		if err := ac.DeleteTopic(context.Background(), name); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		fmt.Printf("deleted: %s\n", name)
		return nil
	},
}

var topicListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all topics",
	RunE: func(cmd *cobra.Command, args []string) error {
		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		topics, err := ac.ListTopics(context.Background())
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		printTopicTable(os.Stdout, topics)
		return nil
	},
}

var topicDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")

		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		desc, err := ac.DescribeTopic(context.Background(), name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		printTopicDescription(os.Stdout, desc)
		return nil
	},
}

var topicAlterPartitionsCmd = &cobra.Command{
	Use:   "alter-partitions",
	Short: "Alter the partition count of a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		partitions, _ := cmd.Flags().GetInt32("partitions")

		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		if err := ac.AlterTopicPartitions(context.Background(), name, partitions); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		fmt.Printf("updated: %s\n", name)
		return nil
	},
}

var topicAlterRetentionCmd = &cobra.Command{
	Use:   "alter-retention",
	Short: "Alter the retention policy of a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		retMs, _ := cmd.Flags().GetInt64("retention-ms")
		retBytes, _ := cmd.Flags().GetInt64("retention-bytes")

		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		if err := ac.AlterTopicRetention(context.Background(), name, retMs, retBytes); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		fmt.Printf("updated: %s\n", name)
		return nil
	},
}

var topicListPartitionsCmd = &cobra.Command{
	Use:   "list-partitions",
	Short: "List partitions of a topic with offsets",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")

		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		parts, err := ac.ListPartitions(context.Background(), name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		printPartitionTable(os.Stdout, parts)
		return nil
	},
}

func init() {
	topicCreateCmd.Flags().String("name", "", "topic name (required)")
	_ = topicCreateCmd.MarkFlagRequired("name")
	topicCreateCmd.Flags().Int32("partitions", 1, "number of partitions")
	topicCreateCmd.Flags().Int32("replication-factor", 1, "replication factor")
	topicCreateCmd.Flags().Int64("retention-ms", -1, "retention in milliseconds (-1 = unlimited)")
	topicCreateCmd.Flags().Int64("retention-bytes", -1, "retention in bytes (-1 = unlimited)")

	topicDeleteCmd.Flags().String("name", "", "topic name (required)")
	_ = topicDeleteCmd.MarkFlagRequired("name")

	topicDescribeCmd.Flags().String("name", "", "topic name (required)")
	_ = topicDescribeCmd.MarkFlagRequired("name")

	topicAlterPartitionsCmd.Flags().String("name", "", "topic name (required)")
	_ = topicAlterPartitionsCmd.MarkFlagRequired("name")
	topicAlterPartitionsCmd.Flags().Int32("partitions", 0, "new partition count (required)")
	_ = topicAlterPartitionsCmd.MarkFlagRequired("partitions")

	topicAlterRetentionCmd.Flags().String("name", "", "topic name (required)")
	_ = topicAlterRetentionCmd.MarkFlagRequired("name")
	topicAlterRetentionCmd.Flags().Int64("retention-ms", -1, "retention in milliseconds")
	topicAlterRetentionCmd.Flags().Int64("retention-bytes", -1, "retention in bytes")

	topicListPartitionsCmd.Flags().String("name", "", "topic name (required)")
	_ = topicListPartitionsCmd.MarkFlagRequired("name")

	topicCmd.AddCommand(
		topicCreateCmd,
		topicDeleteCmd,
		topicListCmd,
		topicDescribeCmd,
		topicAlterPartitionsCmd,
		topicAlterRetentionCmd,
		topicListPartitionsCmd,
	)
	rootCmd.AddCommand(topicCmd)
}

func printTopicInfo(w io.Writer, t client.TopicInfo) {
	tw := tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "Name:\t%s\n", t.Name)
	_, _ = fmt.Fprintf(tw, "Partitions:\t%d\n", t.PartitionCount)
	_, _ = fmt.Fprintf(tw, "ReplicationFactor:\t%d\n", t.ReplicationFactor)
	_, _ = fmt.Fprintf(tw, "RetentionMs:\t%d\n", t.RetentionMs)
	_, _ = fmt.Fprintf(tw, "RetentionBytes:\t%d\n", t.RetentionBytes)
	_, _ = fmt.Fprintf(tw, "CreatedAtMs:\t%d\n", t.CreatedAtMs)
	_ = tw.Flush()
}

func printTopicTable(w io.Writer, topics []client.TopicInfo) {
	tw := tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tPARTITIONS\tRF\tRETENTION-MS\tRETENTION-BYTES")
	for _, t := range topics {
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\n",
			t.Name, t.PartitionCount, t.ReplicationFactor, t.RetentionMs, t.RetentionBytes)
	}
	_ = tw.Flush()
}

func printTopicDescription(w io.Writer, d client.TopicDescription) {
	tw := tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "Name:\t%s\n", d.Topic.Name)
	_, _ = fmt.Fprintf(tw, "Partitions:\t%d\n", d.Topic.PartitionCount)
	_, _ = fmt.Fprintf(tw, "ReplicationFactor:\t%d\n", d.Topic.ReplicationFactor)
	_, _ = fmt.Fprintf(tw, "RetentionMs:\t%d\n", d.Topic.RetentionMs)
	_, _ = fmt.Fprintf(tw, "RetentionBytes:\t%d\n", d.Topic.RetentionBytes)
	_ = tw.Flush()

	_, _ = fmt.Fprintln(w)
	pw := tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
	_, _ = fmt.Fprintln(pw, "PARTITION\tSHARD\tLEADER-NODE\tEPOCH")
	for _, p := range d.Partitions {
		_, _ = fmt.Fprintf(pw, "%d\t%d\t%d\t%d\n", p.PartitionID, p.ShardID, p.LeaderNodeID, p.LeaderEpoch)
	}
	_ = pw.Flush()
}

func printPartitionTable(w io.Writer, parts []client.PartitionInfoWithOffsets) {
	tw := tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "PARTITION\tLEADER-NODE\tSHARD\tEARLIEST\tLATEST")
	for _, p := range parts {
		_, _ = fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%d\n",
			p.Info.PartitionID, p.Info.LeaderNodeID, p.Info.ShardID, p.EarliestOffset, p.LatestOffset)
	}
	_ = tw.Flush()
}
