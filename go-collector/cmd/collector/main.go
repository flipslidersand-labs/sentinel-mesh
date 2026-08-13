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

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/exporter"
	"github.com/flipslidersand/sentinel-mesh/internal/otel"
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

			// Phase 5: load alerting rules and initialize engine
			rulesPath, _ := cmd.Flags().GetString("rules")
			ruleset, err := alerting.LoadRules(rulesPath)
			if err != nil {
				return fmt.Errorf("load rules: %w", err)
			}
			if err := ruleset.Validate(); err != nil {
				return fmt.Errorf("validate rules: %w", err)
			}
			engine := alerting.New(ruleset, logger)
			if len(ruleset.Rules) > 0 {
				logger.Info("alerting engine loaded", zap.Int("rules", len(ruleset.Rules)))
			}

			// Phase 6: initialize OTel metrics and traces
			metricsProvider, err := otel.NewMetricsProvider(ctx)
			if err != nil {
				return fmt.Errorf("otel metrics: %w", err)
			}
			logger.Info("Prometheus metrics endpoint enabled", zap.String("addr", httpAddr+"/metrics"))

			otelEndpoint, _ := cmd.Flags().GetString("otel-endpoint")
			tracesProvider, err := otel.NewTracesProvider(ctx, otelEndpoint)
			if err != nil {
				return fmt.Errorf("otel traces: %w", err)
			}
			defer tracesProvider.Close(ctx) //nolint:errcheck
			if otelEndpoint != "" {
				logger.Info("distributed tracing enabled", zap.String("endpoint", otelEndpoint))
			}

			staticDir, _ := cmd.Flags().GetString("static-dir")
			corsOrigins, _ := cmd.Flags().GetStringSlice("cors-origins")

			// REST API in background
			router := exporter.Router(st, reg, staticDir, corsOrigins)
			go func() {
				logger.Info("REST API listening", zap.String("addr", httpAddr))
				if err := router.Run(httpAddr); err != nil {
					logger.Error("REST API stopped", zap.Error(err))
				}
			}()

			// gRPC server (blocking)
			return receiver.Serve(grpcAddr, st, reg, engine, metricsProvider, tracesProvider.Tracer(), logger)
		},
	}
	cmd.Flags().String("grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().String("http-addr", ":8081", "REST API listen address")
	cmd.Flags().String("data-dir", "/tmp/sentinel-data", "BadgerDB data directory")
	cmd.Flags().Duration("heartbeat-timeout", 60*time.Second, "inactivity duration before agent is marked inactive")
	cmd.Flags().String("rules", "", "path to alerting rules YAML file")
	cmd.Flags().String("otel-endpoint", "", "OTLP HTTP endpoint for distributed tracing (e.g., http://localhost:4318)")
	cmd.Flags().String("static-dir", "./static", "path to static UI directory")
	cmd.Flags().StringSlice("cors-origins", nil, "allowed CORS origins (empty = allow all; production: http://localhost:8081)")
	return cmd
}
