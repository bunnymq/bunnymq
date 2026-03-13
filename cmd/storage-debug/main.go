package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bunnymq/bunnymq/internal/config"
	"github.com/bunnymq/bunnymq/internal/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func configFlag(cmd *cobra.Command) string {
	f, _ := cmd.Root().PersistentFlags().GetString("config")
	return f
}

func buildStorageConfig(cfgFile string) (*config.StorageConfig, error) {
	cfg := &config.StorageConfig{
		SegmentMaxBytes:          128 * 1024 * 1024,
		IndexSampleBytes:         4096,
		RetentionCheckIntervalMs: 300_000,
		DefaultRetentionMs:       -1,
		DefaultRetentionBytes:    -1,
	}
	if cfgFile == "" {
		return cfg, nil
	}
	v := viper.New()
	v.SetConfigFile(cfgFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	if v.IsSet("segment_max_bytes") {
		cfg.SegmentMaxBytes = v.GetInt64("segment_max_bytes")
	}
	if v.IsSet("index_sample_bytes") {
		cfg.IndexSampleBytes = v.GetInt("index_sample_bytes")
	}
	return cfg, nil
}

func openStorage(dir, cfgFile string) (storage.Storage, error) {
	cfg, err := buildStorageConfig(cfgFile)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	s, err := storage.Open(dir, cfg)
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	return s, nil
}

func newAppendCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "append <dir>",
		Short: "Read lines from stdin and append each as a batch",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := openStorage(args[0], configFlag(cmd))
			if err != nil {
				return err
			}
			defer s.Close()

			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				rec := storage.Record{
					TimestampMs: time.Now().UnixMilli(),
					Value:       append([]byte(nil), scanner.Bytes()...),
				}
				batch, err := storage.EncodeBatch([]storage.Record{rec})
				if err != nil {
					return fmt.Errorf("encode batch: %w", err)
				}
				baseOffset, err := s.Append(batch)
				if err != nil {
					return fmt.Errorf("append: %w", err)
				}
				fmt.Printf("base_offset=%d\n", baseOffset)
			}
			return scanner.Err()
		},
	}
}

func newReadCmd() *cobra.Command {
	var maxBytes int
	cmd := &cobra.Command{
		Use:   "read <dir> <offset>",
		Short: "Read and print batches starting at offset",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			offset, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid offset: %w", err)
			}
			s, err := openStorage(args[0], configFlag(cmd))
			if err != nil {
				return err
			}
			defer s.Close()

			mb := maxBytes
			if mb <= 0 {
				mb = 4 * 1024 * 1024
			}

			data, _, err := s.Read(offset, mb)
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			if data == nil {
				fmt.Println("no data at offset")
				return nil
			}

			pos := 0
			for pos < len(data) {
				batch, next, err := storage.DecodeNextBatch(data, pos)
				if err != nil {
					return fmt.Errorf("decode batch at pos %d: %w", pos, err)
				}
				fmt.Printf("batch offset=%d records=%d\n", batch.BaseOffset, batch.RecordCount)
				for _, rec := range batch.Records {
					val := rec.Value
					if len(val) > 80 {
						val = val[:80]
					}
					fmt.Printf("  ts=%d value=%s\n", rec.TimestampMs, val)
				}
				pos = next
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&maxBytes, "max-bytes", 0, "maximum bytes to read (default 4 MiB)")
	return cmd
}

func newDumpSegmentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dump-segment <segment.log>",
		Short: "Scan a raw .log file and print per-batch stats",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read file: %w", err)
			}

			pos := 0
			for pos < len(data) {
				if pos+38 > len(data) {
					return fmt.Errorf("truncated header at pos %d", pos)
				}
				batchLen := int(binary.BigEndian.Uint32(data[pos+8 : pos+12]))
				if batchLen < 38 || pos+batchLen > len(data) {
					return fmt.Errorf("invalid batch_length=%d at pos %d", batchLen, pos)
				}

				batchData := data[pos : pos+batchLen]
				baseOffset := int64(binary.BigEndian.Uint64(batchData[0:8]))
				recordCount := int32(binary.BigEndian.Uint32(batchData[12:16]))
				storedCRC := binary.BigEndian.Uint32(batchData[16:20])
				baseTs := int64(binary.BigEndian.Uint64(batchData[22:30]))
				maxTs := int64(binary.BigEndian.Uint64(batchData[30:38]))

				computedCRC := crc32.Checksum(batchData[38:], crcTable)
				crcStatus := "OK"
				if computedCRC != storedCRC {
					crcStatus = "FAIL"
				}

				fmt.Printf("offset=%d length=%d records=%d ts=[%d..%d] crc=%s\n",
					baseOffset, batchLen, recordCount, baseTs, maxTs, crcStatus)

				pos += batchLen
			}
			return nil
		},
	}
}

func newStatsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats <dir>",
		Short: "Print segment list and offset range",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			s, err := openStorage(dir, configFlag(cmd))
			if err != nil {
				return err
			}
			defer s.Close()

			matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
			if err != nil {
				return fmt.Errorf("glob: %w", err)
			}
			sort.Strings(matches)

			for _, path := range matches {
				info, err := os.Stat(path)
				if err != nil {
					return fmt.Errorf("stat %s: %w", path, err)
				}
				base := filepath.Base(path)
				stem := strings.TrimSuffix(base, ".log")
				baseOffset, err := strconv.ParseInt(stem, 10, 64)
				if err != nil {
					return fmt.Errorf("invalid segment filename %s: %w", base, err)
				}
				fmt.Printf("%s: base_offset=%d size=%d\n", base, baseOffset, info.Size())
			}

			fmt.Printf("EarliestOffset=%d\n", s.EarliestOffset())
			fmt.Printf("LatestOffset=%d\n", s.LatestOffset())
			fmt.Printf("segments=%d\n", len(matches))
			return nil
		},
	}
}

func main() {
	root := &cobra.Command{
		Use:          "storage-debug",
		Short:        "BunnyMQ storage debug CLI",
		SilenceUsage: true,
	}
	root.PersistentFlags().String("config", "", "YAML config file (segment_max_bytes, index_sample_bytes)")

	root.AddCommand(
		newAppendCmd(),
		newReadCmd(),
		newDumpSegmentCmd(),
		newStatsCmd(),
	)

	if err := root.Execute(); err != nil {
		if !errors.Is(err, nil) {
			os.Exit(1)
		}
	}
}
