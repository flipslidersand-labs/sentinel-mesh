package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/exporter"
	"github.com/flipslidersand/sentinel-mesh/internal/receiver"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func main() {
	root := &cobra.Command{
		Use:   "sentinel-collector",
		Short: "SentinelMesh Go Control Plane",
	}
	root.AddCommand(serveCmd())
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start gRPC server and REST API",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddr, _ := cmd.Flags().GetString("grpc-addr")
			httpAddr, _ := cmd.Flags().GetString("http-addr")
			dataDir, _ := cmd.Flags().GetString("data-dir")
			hbTimeout, _ := cmd.Flags().GetDuration("heartbeat-timeout")
			reapInterval, _ := cmd.Flags().GetDuration("reap-interval")

			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}
			defer logger.Sync() //nolint:errcheck

			st, err := store.New(dataDir)
			if err != nil {
				return fmt.Errorf("badger open: %w", err)
			}
			defer st.Close() //nolint:errcheck

			reg := registry.New()

			// REST API in background
			router := exporter.Router(st, reg)
			go func() {
				logger.Info("REST API listening", zap.String("addr", httpAddr))
				if err := router.Run(httpAddr); err != nil {
					logger.Error("REST API stopped", zap.Error(err))
				}
			}()

			// Heartbeat reaper: mark agents inactive once their last event is
			// older than heartbeat-timeout.
			go func() {
				ticker := time.NewTicker(reapInterval)
				defer ticker.Stop()
				for range ticker.C {
					for _, id := range reg.ReapInactive(hbTimeout) {
						logger.Info("agent marked inactive", zap.String("node_id", id))
					}
				}
			}()

			// gRPC server (blocking)
			return receiver.Serve(grpcAddr, st, reg, logger)
		},
	}
	cmd.Flags().String("grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().String("http-addr", ":8081", "REST API listen address")
	cmd.Flags().String("data-dir", "/tmp/sentinel-data", "BadgerDB data directory")
	cmd.Flags().Duration("heartbeat-timeout", 90*time.Second, "mark an agent inactive if no event within this duration")
	cmd.Flags().Duration("reap-interval", 30*time.Second, "how often to scan for inactive agents")
	return cmd
}
