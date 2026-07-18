package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			// Phase 4: mark agents inactive after 60s of silence, check every 30s
			heartbeatTimeout, _ := cmd.Flags().GetDuration("heartbeat-timeout")
			reg.StartHeartbeatChecker(ctx, heartbeatTimeout, heartbeatTimeout/2)
			logger.Info("heartbeat checker started", zap.Duration("timeout", heartbeatTimeout))

			// REST API in background
			router := exporter.Router(st, reg)
			go func() {
				logger.Info("REST API listening", zap.String("addr", httpAddr))
				if err := router.Run(httpAddr); err != nil {
					logger.Error("REST API stopped", zap.Error(err))
				}
			}()

			// gRPC server (blocking)
			return receiver.Serve(grpcAddr, st, reg, logger)
		},
	}
	cmd.Flags().String("grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().String("http-addr", ":8081", "REST API listen address")
	cmd.Flags().String("data-dir", "/tmp/sentinel-data", "BadgerDB data directory")
	cmd.Flags().Duration("heartbeat-timeout", 60*time.Second, "inactivity duration before agent is marked inactive")
	return cmd
}
