package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/bunnymq/bunnymq/pkg/client"
	"github.com/spf13/cobra"
)

var (
	flagConsumeTopic     string
	flagConsumePartition int32
	flagConsumeOffset    int64
	flagConsumeGroup     string
	flagOffsetReset      string
	flagConsumeCount     int
	flagMaxWaitMs        int64
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Read messages from a topic",
	Long: `Read messages from a topic and print each record as a JSON line to stdout.

In manual mode (no --group), --partition is required. The command runs until
--count records have been printed or it is interrupted with SIGINT/SIGTERM.
When --count 0, it runs until interrupted.`,
	RunE: runConsume,
}

func init() {
	consumeCmd.Flags().StringVar(&flagConsumeTopic, "topic", "", "topic to consume")
	_ = consumeCmd.MarkFlagRequired("topic")
	consumeCmd.Flags().Int32Var(&flagConsumePartition, "partition", -1, "partition to read; -1 means all partitions in group mode")
	consumeCmd.Flags().Int64Var(&flagConsumeOffset, "offset", 0, "starting offset for manual mode")
	consumeCmd.Flags().StringVar(&flagConsumeGroup, "group", "", "consumer group ID; enables group mode")
	consumeCmd.Flags().StringVar(&flagOffsetReset, "offset-reset", "earliest", `group mode offset reset policy: "earliest" or "latest"`)
	consumeCmd.Flags().IntVar(&flagConsumeCount, "count", 0, "stop after this many records; 0 means consume indefinitely")
	consumeCmd.Flags().Int64Var(&flagMaxWaitMs, "max-wait-ms", 500, "max wait per Poll call in milliseconds")
	rootCmd.AddCommand(consumeCmd)
}

func parseOffsetReset(s string) client.OffsetResetPolicy {
	if s == "earliest" {
		return client.OffsetResetEarliest
	}
	return client.OffsetResetLatest
}

type recordJSON struct {
	Topic       string            `json:"topic"`
	Partition   int32             `json:"partition"`
	Offset      int64             `json:"offset"`
	Key         []byte            `json:"key"`
	Value       []byte            `json:"value"`
	Headers     map[string][]byte `json:"headers"`
	TimestampMs int64             `json:"timestamp_ms"`
}

func formatRecord(r client.Record) ([]byte, error) {
	return json.Marshal(recordJSON{
		Topic:       r.Topic,
		Partition:   r.PartitionID,
		Offset:      r.Offset,
		Key:         r.Key,
		Value:       r.Value,
		Headers:     r.Headers,
		TimestampMs: r.TimestampMs,
	})
}

func runConsume(cmd *cobra.Command, _ []string) error {
	if flagConsumeGroup == "" && flagConsumePartition == -1 {
		return fmt.Errorf("--partition is required in manual mode")
	}

	cfg := client.ConsumerConfig{
		Config:          buildClientConfig(),
		GroupID:         flagConsumeGroup,
		MaxFetchWaitMs:  flagMaxWaitMs,
		AutoOffsetReset: parseOffsetReset(flagOffsetReset),
	}
	c, err := client.NewConsumer(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if flagConsumeGroup != "" {
		if err := c.Subscribe([]string{flagConsumeTopic}); err != nil {
			return err
		}
	} else {
		c.Seek(flagConsumeTopic, flagConsumePartition, flagConsumeOffset)
	}

	count := 0
loop:
	for {
		records, err := c.Poll(ctx, flagMaxWaitMs)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}

		for _, r := range records {
			data, err := formatRecord(r)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\n", data)
			count++
			if flagConsumeCount > 0 && count >= flagConsumeCount {
				break loop
			}
		}
	}

	if flagConsumeGroup != "" {
		if commitErr := c.Commit(ctx); commitErr != nil && ctx.Err() == nil {
			return commitErr
		}
	}

	return nil
}
