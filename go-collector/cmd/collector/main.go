package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "sentinel-collector",
		Short: "SentinelMesh Go Control Plane",
	}
	root.AddCommand(
		&cobra.Command{
			Use:   "serve",
			Short: "Start gRPC server and REST API",
			RunE: func(cmd *cobra.Command, args []string) error {
				fmt.Println("serve — not yet implemented")
				return nil
			},
		},
	)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
