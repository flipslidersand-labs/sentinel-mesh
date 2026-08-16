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

	"github.com/flipslidersand/sentinel-mesh/internal/aggregator"
	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/exporter"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
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

			// Aggregate mode: no gRPC/store — poll upstream region collectors and
			// serve a merged read-only view.
			if aggregate, _ := cmd.Flags().GetBool("aggregate"); aggregate {
				return runAggregate(cmd, logger)
			}

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

			// Phase 7: anomaly detector (sliding-window frequency detection)
			detector := anomaly.New(nil)
			logger.Info("anomaly detector started",
				zap.Int("windows", len(anomaly.DefaultWindows)))

			// Alert notifications (Slack/email) — configured via SENTINEL_* env vars.
			notifier := notify.DispatcherFromEnv(logger)
			if notifier.Enabled() {
				logger.Info("alert notifications enabled")
			}

			// Default region for agents that register without one.
			defaultRegion, _ := cmd.Flags().GetString("region")
			logger.Info("collector default region", zap.String("region", defaultRegion))

			staticDir, _ := cmd.Flags().GetString("static-dir")
			corsOrigins, _ := cmd.Flags().GetStringSlice("cors-origins")

			// REST API in background
			router := exporter.Router(st, reg, detector, staticDir, corsOrigins)
			go func() {
				logger.Info("REST API listening", zap.String("addr", httpAddr))
				if err := router.Run(httpAddr); err != nil {
					logger.Error("REST API stopped", zap.Error(err))
				}
			}()

			// gRPC server (blocking)
			return receiver.Serve(grpcAddr, st, reg, engine, detector, notifier, metricsProvider, tracesProvider.Tracer(), defaultRegion, logger)
		},
	}
	cmd.Flags().String("grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().String("http-addr", ":8081", "REST API listen address")
	cmd.Flags().String("data-dir", "/tmp/sentinel-data", "BadgerDB data directory")
	cmd.Flags().Duration("heartbeat-timeout", 60*time.Second, "inactivity duration before agent is marked inactive")
	cmd.Flags().String("rules", "", "path to alerting rules YAML file")
	cmd.Flags().String("otel-endpoint", "", "OTLP HTTP endpoint for distributed tracing (e.g., http://localhost:4318)")
	cmd.Flags().String("region", "default", "default region for agents that register without one")
	cmd.Flags().String("static-dir", "./static", "path to static UI directory")
	cmd.Flags().StringSlice("cors-origins", nil, "allowed CORS origins (empty = allow all; production: http://localhost:8081)")
	cmd.Flags().Bool("aggregate", false, "run as a cross-region aggregator (polls --upstreams, no gRPC)")
	cmd.Flags().StringSlice("upstreams", nil, "aggregate mode: region collectors as region=url (repeatable)")
	cmd.Flags().Duration("poll-interval", 10*time.Second, "aggregate mode: how often to poll upstreams")
	return cmd
}

// runAggregate starts the cross-region aggregator: it polls upstream region
// collectors' REST APIs and serves a merged read-only view on --http-addr.
func runAggregate(cmd *cobra.Command, logger *zap.Logger) error {
	httpAddr, _ := cmd.Flags().GetString("http-addr")
	specs, _ := cmd.Flags().GetStringSlice("upstreams")
	interval, _ := cmd.Flags().GetDuration("poll-interval")
	staticDir, _ := cmd.Flags().GetString("static-dir")
	corsOrigins, _ := cmd.Flags().GetStringSlice("cors-origins")

	upstreams, err := aggregator.ParseUpstreams(specs)
	if err != nil {
		return err
	}
	if len(upstreams) == 0 {
		return fmt.Errorf("--aggregate requires at least one --upstreams region=url")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	agg := aggregator.New(upstreams, interval, logger)
	agg.Start(ctx)
	logger.Info("aggregator started",
		zap.Int("upstreams", len(upstreams)), zap.Duration("poll_interval", interval))

	return aggregator.Router(agg, staticDir, corsOrigins).Run(httpAddr)
}
