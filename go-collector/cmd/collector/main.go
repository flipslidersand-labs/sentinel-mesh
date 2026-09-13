package main

import (
	"context"
	"fmt"
	"net/http"
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
	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/otel"
	"github.com/flipslidersand/sentinel-mesh/internal/receiver"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// REST API server timeouts. Go's http.Server has no defaults (unlimited),
// so a slow or malicious client (e.g. Slowloris) can hold a connection open
// indefinitely, leaking goroutines and file descriptors. These bound how
// long a connection may sit idle or take to read/write.
const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 15 * time.Second
	httpWriteTimeout      = 30 * time.Second
	httpIdleTimeout       = 60 * time.Second

	// httpShutdownTimeout bounds how long a graceful REST API shutdown
	// waits for in-flight requests to finish before forcibly closing
	// remaining connections (#117).
	httpShutdownTimeout = 10 * time.Second
)

// runHTTPServerUntilDone starts srv in the background and blocks until ctx
// is cancelled, at which point it gracefully shuts srv down (bounded by
// httpShutdownTimeout) so callers don't hang on SIGINT/SIGTERM (#117). Any
// ListenAndServe error other than the expected http.ErrServerClosed is
// logged as an error.
func runHTTPServerUntilDone(ctx context.Context, srv *http.Server, name string, logger *zap.Logger) {
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error(name+" stopped", zap.Error(err))
		}
	case <-ctx.Done():
		logger.Info(name + " shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error(name+" graceful shutdown failed", zap.Error(err))
		}
		<-errCh
	}
}

// newHTTPServer builds an http.Server with explicit read/write/idle
// timeouts for the given address and handler.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

func main() {
	root := rootCmd()
	root.AddCommand(serveCmd())
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sentinel-collector",
		Short: "SentinelMesh Go Control Plane",
		Long: `sentinel-collector is the Go control plane for SentinelMesh.

It receives agent telemetry over gRPC, serves a REST API and bundled UI,
evaluates alerting rules, and can run as a cross-region aggregator that
polls other collectors' REST APIs for a merged read-only view.

Run "sentinel-collector serve --help" for available modes and flags.`,
	}
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start gRPC server and REST API",
		Long: `Start the collector's gRPC server (for agent telemetry) and REST API.

Two mutually exclusive modes:

  - Normal mode (default): receives agent telemetry over gRPC, persists it
    to BadgerDB, evaluates alerting rules, and serves a REST API + UI.
  - Aggregate mode (--aggregate): no gRPC server or local store — instead
    polls one or more upstream region collectors' REST APIs (--upstreams)
    and serves a merged read-only view. Normal-mode-only flags such as
    --grpc-addr, --data-dir, --rules, and --grpc-tls-cert/--grpc-tls-key
    are ignored in this mode.

TLS for the gRPC server is optional but, when enabled, both
--grpc-tls-cert and --grpc-tls-key must be supplied together.`,
		Example: `  # Normal mode: gRPC + REST API on default addresses
  sentinel-collector serve

  # Normal mode with gRPC TLS enabled
  sentinel-collector serve --grpc-tls-cert=/etc/sentinel/tls.crt --grpc-tls-key=/etc/sentinel/tls.key

  # Aggregate mode: merge two region collectors into one read-only view
  sentinel-collector serve --aggregate \
    --upstreams=us-east=http://collector-us-east:8081 \
    --upstreams=eu-west=http://collector-eu-west:8081`,
		RunE: func(cmd *cobra.Command, args []string) error {
			aggregate, _ := cmd.Flags().GetBool("aggregate")
			upstreamsSet := cmd.Flags().Changed("upstreams") || cmd.Flags().Changed("poll-interval")
			if !aggregate && upstreamsSet {
				return fmt.Errorf("--upstreams/--poll-interval require --aggregate")
			}

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
			if aggregate {
				warnIgnoredNormalModeFlags(cmd, grpcAddr, logger)
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
			if heartbeatTimeout <= 0 {
				return fmt.Errorf("--heartbeat-timeout must be positive, got %s", heartbeatTimeout)
			}
			reg.StartHeartbeatChecker(ctx, heartbeatTimeout, heartbeatTimeout/2, registry.DefaultEvictAfter)
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
			detector.StartGC(ctx, time.Minute)
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
			apiToken := httpauth.TokenFromEnv()
			if apiToken == "" {
				logger.Warn("REST API is UNAUTHENTICATED — set " + httpauth.EnvAPIToken + " to require a bearer token")
			}

			// REST API in background, tied to ctx for graceful shutdown (#117).
			router := exporter.Router(st, reg, detector, staticDir, corsOrigins, apiToken)
			httpServer := newHTTPServer(httpAddr, router)
			go func() {
				logger.Info("REST API listening", zap.String("addr", httpAddr))
				runHTTPServerUntilDone(ctx, httpServer, "REST API", logger)
			}()

			// gRPC server (blocking, but ctx-aware so SIGINT/SIGTERM gracefully
			// stops it instead of hanging forever — #117).
			grpcTLSCert, _ := cmd.Flags().GetString("grpc-tls-cert")
			grpcTLSKey, _ := cmd.Flags().GetString("grpc-tls-key")
			if grpcTLSCert == "" || grpcTLSKey == "" {
				logger.Warn("gRPC server is running WITHOUT TLS — set --grpc-tls-cert/--grpc-tls-key to require it")
			}
			if apiToken == "" {
				logger.Warn("gRPC server is UNAUTHENTICATED — set " + httpauth.EnvAPIToken + " to require a bearer token")
			}
			return receiver.Serve(ctx, grpcAddr, st, reg, engine, detector, notifier, metricsProvider, tracesProvider.Tracer(), defaultRegion, logger,
				grpcTLSCert, grpcTLSKey, apiToken)
		},
	}
	cmd.Flags().String("grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().String("grpc-tls-cert", "", "path to gRPC server TLS certificate (PEM); requires --grpc-tls-key")
	cmd.Flags().String("grpc-tls-key", "", "path to gRPC server TLS private key (PEM); requires --grpc-tls-cert")
	cmd.Flags().String("http-addr", ":8081", "REST API listen address")
	cmd.Flags().String("data-dir", "/tmp/sentinel-data", "BadgerDB data directory")
	cmd.Flags().Duration("heartbeat-timeout", 60*time.Second, "inactivity duration before agent is marked inactive")
	cmd.Flags().String("rules", "", "path to alerting rules YAML file")
	cmd.Flags().String("otel-endpoint", "", "OTLP HTTP endpoint for distributed tracing (e.g., http://localhost:4318)")
	cmd.Flags().String("region", "default", "default region for agents that register without one")
	cmd.Flags().String("static-dir", "./static", "path to static UI directory")
	cmd.Flags().StringSlice("cors-origins", nil, "allowed CORS origins (empty = deny all cross-origin; the bundled UI is same-origin)")
	cmd.Flags().Bool("aggregate", false, "run as a cross-region aggregator (polls --upstreams, no gRPC)")
	cmd.Flags().StringSlice("upstreams", nil, "aggregate mode: region collectors as region=url (repeatable)")
	cmd.Flags().Duration("poll-interval", 10*time.Second, "aggregate mode: how often to poll upstreams")
	cmd.MarkFlagsRequiredTogether("grpc-tls-cert", "grpc-tls-key")
	return cmd
}

// warnIgnoredNormalModeFlags logs a warning for each normal-mode-only flag
// that was explicitly set by the caller but is ignored in --aggregate mode
// (#152), so a mistaken flag (e.g. --grpc-tls-cert) doesn't silently no-op.
func warnIgnoredNormalModeFlags(cmd *cobra.Command, grpcAddr string, logger *zap.Logger) {
	if cmd.Flags().Changed("grpc-addr") && grpcAddr != ":50051" {
		logger.Warn("--aggregate mode ignores --grpc-addr")
	}
	for _, name := range []string{"data-dir", "rules", "grpc-tls-cert", "grpc-tls-key", "region"} {
		if cmd.Flags().Changed(name) {
			logger.Warn("--aggregate mode ignores --" + name)
		}
	}
}

// runAggregate starts the cross-region aggregator: it polls upstream region
// collectors' REST APIs and serves a merged read-only view on --http-addr.
func runAggregate(cmd *cobra.Command, logger *zap.Logger) error {
	httpAddr, _ := cmd.Flags().GetString("http-addr")
	specs, _ := cmd.Flags().GetStringSlice("upstreams")
	interval, _ := cmd.Flags().GetDuration("poll-interval")
	staticDir, _ := cmd.Flags().GetString("static-dir")
	corsOrigins, _ := cmd.Flags().GetStringSlice("cors-origins")
	apiToken := httpauth.TokenFromEnv()
	if apiToken == "" {
		logger.Warn("aggregator REST API is UNAUTHENTICATED — set " + httpauth.EnvAPIToken + " to require a bearer token")
	}

	upstreams, err := aggregator.ParseUpstreams(specs)
	if err != nil {
		return err
	}
	if len(upstreams) == 0 {
		return fmt.Errorf("--aggregate requires at least one --upstreams region=url")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	agg := aggregator.New(upstreams, interval, apiToken, logger)
	agg.Start(ctx)
	logger.Info("aggregator started",
		zap.Int("upstreams", len(upstreams)), zap.Duration("poll_interval", interval))

	httpServer := newHTTPServer(httpAddr, aggregator.Router(agg, staticDir, corsOrigins, apiToken))
	logger.Info("aggregator REST API listening", zap.String("addr", httpAddr))
	runHTTPServerUntilDone(ctx, httpServer, "aggregator REST API", logger)
	return nil
}
