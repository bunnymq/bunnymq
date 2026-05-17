package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/bunnymq/bunnymq/pkg/client"
	"github.com/spf13/cobra"
)

var (
	flagProduceTopic string
	flagProduceKey   string
	flagProduceValue string
	flagProduceAcks  string
)

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Send a message to a topic",
	RunE:  runProduce,
}

func init() {
	produceCmd.Flags().StringVar(&flagProduceTopic, "topic", "", "target topic name")
	_ = produceCmd.MarkFlagRequired("topic")
	produceCmd.Flags().StringVar(&flagProduceKey, "key", "", "message key")
	produceCmd.Flags().StringVar(&flagProduceValue, "value", "", "message value; if empty, reads lines from stdin")
	produceCmd.Flags().StringVar(&flagProduceAcks, "acks", "all", `acknowledgement mode: "all" or "zero"`)
	rootCmd.AddCommand(produceCmd)
}

func parseAcks(s string) (client.AcksMode, error) {
	switch s {
	case "all":
		return client.AcksAll, nil
	case "zero":
		return client.AcksZero, nil
	default:
		return 0, fmt.Errorf("unknown acks value %q: must be \"all\" or \"zero\"", s)
	}
}

func runProduce(cmd *cobra.Command, _ []string) error {
	acks, err := parseAcks(flagProduceAcks)
	if err != nil {
		return fmt.Errorf("flag --acks: %w", err)
	}

	cfg := client.ProducerConfig{Config: buildClientConfig()}
	p, err := client.NewProducer(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = p.Close() }()

	ctx := context.Background()

	sendAndPrint := func(key, value []byte) error {
		offset, err := p.Send(ctx, flagProduceTopic, key, value, nil, acks)
		if err != nil {
			return err
		}
		if offset == -1 {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "sent (acks=0)")
		} else {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "offset: %d\n", offset)
		}
		return nil
	}

	var keyBytes []byte
	if flagProduceKey != "" {
		keyBytes = []byte(flagProduceKey)
	}

	if flagProduceValue != "" {
		return sendAndPrint(keyBytes, []byte(flagProduceValue))
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if err := sendAndPrint(keyBytes, []byte(line)); err != nil {
			return err
		}
	}
	return scanner.Err()
}
