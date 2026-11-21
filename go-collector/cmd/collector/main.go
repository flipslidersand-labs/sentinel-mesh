package main

import (
	"context"
	"crypto/tls"
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
)

// httpShutdownTimeout bounds how long a graceful REST API shutdown
// waits for in-flight requests to finish before forcibly closing
// remaining connections (#117). A variable so tests can shorten it.
var httpShutdownTimeout = 10 * time.Second

// runHTTPServerUntilDone starts srv in the background and blocks until ctx
// is cancelled, at which point it gracefully shuts srv down (bounded by
// httpShutdownTimeout) so callers don't hang on SIGINT/SIGTERM (#117). Any
// ListenAndServe error other than the expected http.ErrServerClosed is
// returned (and logged) so callers can fail the process (#318). It returns
// only after the server has fully stopped, including in-flight requests
// (#316).
func runHTTPServerUntilDone(ctx context.Context, srv *http.Server, name string, logger *zap.Logger) error {
	return runHTTPServerTLSUntilDone(ctx, srv, name, "", "", logger)
}

// runHTTPServerTLSUntilDone is runHTTPServerUntilDone with optional TLS:
// when both certFile and keyFile are non-empty the server listens with
// ListenAndServeTLS, otherwise it serves plaintext HTTP (#359).
func runHTTPServerTLSUntilDone(ctx context.Context, srv *http.Server, name, certFile, keyFile string, logger *zap.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		if certFile != "" && keyFile != "" {
			errCh <- srv.ListenAndServeTLS(certFile, keyFile)
			return
		}
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error(name+" stopped", zap.Error(err))
			return fmt.Errorf("%s: %w", name, err)
		}
	case <-ctx.Done():
		logger.Info(name + " shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error(name+" graceful shutdown failed; forcing close", zap.Error(err))
			// Handlers may still be running; force-close remaining
			// connections so callers can safely close shared resources.
			if cerr := srv.Close(); cerr != nil {
				logger.Error(name+" forced close failed", zap.Error(cerr))
			}
		}
		<-errCh
	}
	return nil
}

// runRESTAndGRPC runs rest in a goroutine and grpc in the caller's goroutine,
// both bound to ctx. If either fails, ctx is cancelled (via cancel) so the
// other stops too. It returns only after BOTH have returned, so callers can
// safely close shared resources (store, tracer) afterwards (#316, #318).
// The first non-nil error is returned.
func runRESTAndGRPC(ctx context.Context, cancel context.CancelFunc,
	rest, grpc func(context.Context) error) error {
	restDone := make(chan error, 1)
	go func() {
		err := rest(ctx)
		if err != nil {
			cancel()
		}
		restDone <- err
	}()
	grpcErr := grpc(ctx)
	cancel() // gRPC returned (failure or shutdown): make sure REST stops too.
	restErr := <-restDone
	if grpcErr != nil {
		return grpcErr
	}
	return restErr
}

// newHTTPServer builds an http.Server with explicit read/write/idle
// timeouts for the given address and handler.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

func main() {
	root := rootCmd()
	root.AddCommand(serveCmd())
	root.AddCommand(tokenCmd())
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
--grpc-tls-cert and --grpc-tls-key must be supplied together. The same holds
for the REST API with --http-tls-cert and --http-tls-key (otherwise the REST
API, including the bearer token, is served in plaintext).

In aggregate mode, http:// upstreams are only accepted for loopback hosts
(127.0.0.0/8, ::1, localhost) unless --aggregate-allow-insecure-http is set,
because the bearer token would otherwise cross the network in plaintext.`,
		Example: `  # Normal mode: gRPC + REST API on default addresses (REST auth is required
  # by default, so SENTINEL_API_TOKEN must be set, at least 32 bytes, e.g.
  # $(openssl rand -hex 32); gRPC needs per-agent tokens)
  SENTINEL_API_TOKEN=<token> sentinel-collector serve

  # Local demo without any authentication (dev only)
  sentinel-collector serve --grpc-auth=disabled --http-auth=disabled

  # Normal mode with gRPC TLS enabled
  SENTINEL_API_TOKEN=<token> sentinel-collector serve --grpc-tls-cert=/etc/sentinel/tls.crt --grpc-tls-key=/etc/sentinel/tls.key

  # Normal mode with REST API TLS enabled
  SENTINEL_API_TOKEN=<token> sentinel-collector serve --http-tls-cert=/etc/sentinel/tls.crt --http-tls-key=/etc/sentinel/tls.key

  # Aggregate mode: merge two region collectors into one read-only view
  SENTINEL_API_TOKEN=<token> sentinel-collector serve --aggregate \
    --upstreams=us-east=https://collector-us-east:8081 \
    --upstreams=eu-west=https://collector-eu-west:8081

  # Aggregate mode over plaintext http:// to a non-loopback upstream (trusted network only)
  SENTINEL_API_TOKEN=<token> sentinel-collector serve --aggregate --aggregate-allow-insecure-http \
    --upstreams=us-east=http://collector-us-east:8081`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate --grpc-auth first so a typo exits before the store lock or
			// any listener is taken (#222).
			grpcAuthMode, _ := cmd.Flags().GetString("grpc-auth")
			grpcAuthEnabled, err := httpauth.ParseMode("grpc-auth", grpcAuthMode)
			if err != nil {
				return err
			}

			aggregate, _ := cmd.Flags().GetBool("aggregate")
			upstreamsSet := cmd.Flags().Changed("upstreams") || cmd.Flags().Changed("poll-interval")
			if !aggregate && upstreamsSet {
				return fmt.Errorf("--upstreams/--poll-interval require --aggregate")
			}

			// Same for --http-auth: refuse to start without SENTINEL_API_TOKEN
			// unless unauthenticated REST is explicitly requested (#254).
			httpAuthMode, _ := cmd.Flags().GetString("http-auth")
			apiToken, restUnauth, err := httpauth.ResolveToken(httpAuthMode)
			if err != nil {
				return err
			}

			grpcAddr, _ := cmd.Flags().GetString("grpc-addr")
			httpAddr, _ := cmd.Flags().GetString("http-addr")
			dataDir, _ := cmd.Flags().GetString("data-dir")

			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}
			defer logger.Sync() //nolint:errcheck

			// Validate REST TLS files before opening the store or any listener.
			httpTLSCert, httpTLSKey, err := httpTLSFlags(cmd, logger, "REST API")
			if err != nil {
				return err
			}

			// Aggregate mode: no gRPC/store — poll upstream region collectors and
			// serve a merged read-only view.
			if aggregate {
				warnIgnoredNormalModeFlags(cmd, grpcAddr, logger)
				return runAggregate(cmd, logger, apiToken, restUnauth, httpTLSCert, httpTLSKey)
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
			for _, w := range ruleset.UnknownFieldWarnings() {
				logger.Warn("rule references unknown payload field", zap.String("detail", w))
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
			notifier, err := notify.DispatcherFromEnv(logger)
			if err != nil {
				return fmt.Errorf("notify config: %w", err)
			}
			if notifier.Enabled() {
				logger.Info("alert notifications enabled")
			}

			// Default region for agents that register without one.
			defaultRegion, _ := cmd.Flags().GetString("region")
			logger.Info("collector default region", zap.String("region", defaultRegion))

			staticDir, _ := cmd.Flags().GetString("static-dir")
			corsOrigins, _ := cmd.Flags().GetStringSlice("cors-origins")
			if restUnauth {
				logger.Warn("REST API is UNAUTHENTICATED (--http-auth=disabled) — set " + httpauth.EnvAPIToken + " and use --http-auth=required")
			}

			// REST API in background, tied to ctx for graceful shutdown (#117).
			router := exporter.Router(st, reg, detector, staticDir, corsOrigins, apiToken)
			httpServer := newHTTPServer(httpAddr, router)

			// gRPC server (blocking, but ctx-aware so SIGINT/SIGTERM gracefully
			// stops it instead of hanging forever — #117).
			grpcTLSCert, _ := cmd.Flags().GetString("grpc-tls-cert")
			grpcTLSKey, _ := cmd.Flags().GetString("grpc-tls-key")
			if grpcTLSCert == "" || grpcTLSKey == "" {
				logger.Warn("gRPC server is running WITHOUT TLS — set --grpc-tls-cert/--grpc-tls-key to require it")
			}
			// gRPC auth is controlled solely by --grpc-auth (#222), independent
			// of SENTINEL_API_TOKEN (REST only). "required" (default) is
			// fail-closed: with zero issued per-agent tokens (#189/#191) every
			// RPC is rejected. Unauthenticated gRPC needs explicit "disabled".
			if !grpcAuthEnabled {
				logger.Warn("gRPC server is UNAUTHENTICATED (--grpc-auth=disabled) — use --grpc-auth=required and issue per-agent tokens via `token issue`")
			}
			// REST failures (e.g. port in use) cancel ctx and fail the process;
			// we wait for REST to finish stopping before the deferred
			// st.Close() runs (#316, #318).
			return runRESTAndGRPC(ctx, cancel, func(ctx context.Context) error {
				logger.Info("REST API listening", zap.String("addr", httpAddr))
				return runHTTPServerTLSUntilDone(ctx, httpServer, "REST API", httpTLSCert, httpTLSKey, logger)
			}, func(ctx context.Context) error {
				return receiver.Serve(ctx, receiver.Config{
					Addr: grpcAddr, TLSCertFile: grpcTLSCert, TLSKeyFile: grpcTLSKey,
					AuthEnabled: grpcAuthEnabled, DefaultRegion: defaultRegion,
				}, receiver.Deps{
					Store: st, Registry: reg, Engine: engine, Detector: detector, Notifier: notifier,
					Metrics: metricsProvider, Tracer: tracesProvider.Tracer(), Logger: logger,
				})
			})
		},
	}
	cmd.Flags().String("grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().String("grpc-tls-cert", "", "path to gRPC server TLS certificate (PEM); requires --grpc-tls-key")
	cmd.Flags().String("grpc-tls-key", "", "path to gRPC server TLS private key (PEM); requires --grpc-tls-cert")
	cmd.Flags().String("grpc-auth", "required", "gRPC authentication: required (per-agent tokens; rejects all RPCs if none issued) or disabled (unauthenticated, dev only)")
	cmd.Flags().String("http-auth", "required", "REST API authentication: required (needs SENTINEL_API_TOKEN, else startup fails) or disabled (unauthenticated, dev only)")
	cmd.Flags().String("http-addr", ":8081", "REST API listen address")
	cmd.Flags().String("http-tls-cert", "", "path to REST API TLS certificate (PEM); requires --http-tls-key")
	cmd.Flags().String("http-tls-key", "", "path to REST API TLS private key (PEM); requires --http-tls-cert")
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
	cmd.Flags().Bool("aggregate-allow-insecure-http", false, "aggregate mode: allow plaintext http:// upstreams on non-loopback hosts (bearer token is sent unencrypted)")
	cmd.MarkFlagsRequiredTogether("grpc-tls-cert", "grpc-tls-key")
	cmd.MarkFlagsRequiredTogether("http-tls-cert", "http-tls-key")
	return cmd
}

// httpTLSFlags returns the REST API TLS cert/key paths and logs a warning
// when REST is served without TLS (#359). The flags are required together
// (enforced by cobra), so both are empty or both are set.
func httpTLSFlags(cmd *cobra.Command, logger *zap.Logger, name string) (string, string, error) {
	cert, _ := cmd.Flags().GetString("http-tls-cert")
	key, _ := cmd.Flags().GetString("http-tls-key")
	if cert != "" && key != "" {
		return cert, key, validateTLSFiles(cert, key)
	}
	if cert == "" || key == "" {
		logger.Warn(name + " is running WITHOUT TLS — the bearer token is sent in plaintext; set --http-tls-cert/--http-tls-key to require it")
	}
	return cert, key, nil
}

// validateTLSFiles loads the cert/key pair so an unreadable or mismatched
// pair fails at startup, before any listener is opened (#359).
func validateTLSFiles(cert, key string) error {
	if _, err := tls.LoadX509KeyPair(cert, key); err != nil {
		return fmt.Errorf("load --http-tls-cert/--http-tls-key: %w", err)
	}
	return nil
}

// warnIgnoredNormalModeFlags logs a warning for each normal-mode-only flag
// that was explicitly set by the caller but is ignored in --aggregate mode
// (#152), so a mistaken flag (e.g. --grpc-tls-cert) doesn't silently no-op.
func warnIgnoredNormalModeFlags(cmd *cobra.Command, grpcAddr string, logger *zap.Logger) {
	if cmd.Flags().Changed("grpc-addr") && grpcAddr != ":50051" {
		logger.Warn("--aggregate mode ignores --grpc-addr")
	}
	for _, name := range []string{"data-dir", "rules", "grpc-tls-cert", "grpc-tls-key", "grpc-auth", "region"} {
		if cmd.Flags().Changed(name) {
			logger.Warn("--aggregate mode ignores --" + name)
		}
	}
}

// runAggregate starts the cross-region aggregator: it polls upstream region
// collectors' REST APIs and serves a merged read-only view on --http-addr.
func runAggregate(cmd *cobra.Command, logger *zap.Logger, apiToken string, restUnauth bool, httpTLSCert, httpTLSKey string) error {
	httpAddr, _ := cmd.Flags().GetString("http-addr")
	specs, _ := cmd.Flags().GetStringSlice("upstreams")
	interval, _ := cmd.Flags().GetDuration("poll-interval")
	staticDir, _ := cmd.Flags().GetString("static-dir")
	corsOrigins, _ := cmd.Flags().GetStringSlice("cors-origins")
	if restUnauth {
		logger.Warn("aggregator REST API is UNAUTHENTICATED (--http-auth=disabled) — set " + httpauth.EnvAPIToken + " and use --http-auth=required")
	}

	allowInsecure, _ := cmd.Flags().GetBool("aggregate-allow-insecure-http")
	upstreams, err := aggregator.ParseUpstreams(specs, allowInsecure)
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
	return runHTTPServerTLSUntilDone(ctx, httpServer, "aggregator REST API", httpTLSCert, httpTLSKey, logger)
}
