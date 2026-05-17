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

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Describe the broker cluster",
}

var clusterDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Show all broker nodes in the cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		ac, err := newAdminClient()
		if err != nil {
			return err
		}
		defer func() { _ = ac.Close() }()

		cd, err := ac.DescribeCluster(context.Background())
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		printClusterTable(os.Stdout, cd)
		return nil
	},
}

func init() {
	clusterCmd.AddCommand(clusterDescribeCmd)
	rootCmd.AddCommand(clusterCmd)
}

func printClusterTable(w io.Writer, cd client.ClusterDescription) {
	tw := tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NODE-ID\tADDRESS")
	for _, n := range cd.Nodes {
		_, _ = fmt.Fprintf(tw, "%d\t%s\n", n.NodeID, n.Address)
	}
	_ = tw.Flush()
}
