package main

import (
	"net/http"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestNewHTTPServerSetsTimeouts guards against a regression to the
// Slowloris-vulnerable default (unbounded) http.Server timeouts. See
// issue #110.
func TestNewHTTPServerSetsTimeouts(t *testing.T) {
	handler := http.NewServeMux()
	srv := newHTTPServer(":8081", handler)

	if srv.Addr != ":8081" {
		t.Errorf("Addr = %q, want %q", srv.Addr, ":8081")
	}
	if srv.Handler != handler {
		t.Error("Handler not set to the provided handler")
	}
	if srv.ReadHeaderTimeout != httpReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, httpReadHeaderTimeout)
	}
	if srv.ReadTimeout != httpReadTimeout {
		t.Errorf("ReadTimeout = %v, want %v", srv.ReadTimeout, httpReadTimeout)
	}
	if srv.WriteTimeout != httpWriteTimeout {
		t.Errorf("WriteTimeout = %v, want %v", srv.WriteTimeout, httpWriteTimeout)
	}
	if srv.IdleTimeout != httpIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", srv.IdleTimeout, httpIdleTimeout)
	}

	// None of the timeouts may be zero (zero == unbounded, the vulnerable state).
	if srv.ReadHeaderTimeout == 0 || srv.ReadTimeout == 0 || srv.WriteTimeout == 0 || srv.IdleTimeout == 0 {
		t.Error("one or more timeouts is zero (unbounded) — Slowloris regression")
	}
}

func TestRootCmd_HasLong(t *testing.T) {
	root := rootCmd()
	if root.Long == "" {
		t.Error("root command Long description must not be empty")
	}
}

func TestServeCmd_HasLongAndExample(t *testing.T) {
	cmd := serveCmd()
	if cmd.Long == "" {
		t.Error("serveCmd Long description must not be empty")
	}
	if cmd.Example == "" {
		t.Error("serveCmd Example must not be empty")
	}
}

func TestServeCmd_TLSFlagsRequiredTogether(t *testing.T) {
	cmd := serveCmd()
	if err := cmd.Flags().Set("grpc-tls-cert", "/tmp/cert.pem"); err != nil {
		t.Fatalf("set grpc-tls-cert: %v", err)
	}
	// grpc-tls-key intentionally left unset.
	if err := cmd.ValidateFlagGroups(); err == nil {
		t.Error("expected error when only --grpc-tls-cert is set without --grpc-tls-key")
	}
}

func TestServeCmd_UpstreamsWithoutAggregateRejected(t *testing.T) {
	cmd := serveCmd()
	cmd.SetArgs([]string{"--upstreams=us-east=http://example.invalid:8081", "--http-addr=:0"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --upstreams is set without --aggregate")
	}
	if got := err.Error(); got != "--upstreams/--poll-interval require --aggregate" {
		t.Errorf("unexpected error message: %q", got)
	}
}

// TestServeCmd_NonPositiveHeartbeatTimeoutRejected guards against #147: a
// zero or negative --heartbeat-timeout used to reach time.NewTicker
// unvalidated and panic, crashing the whole CLI. It must now be rejected
// with a plain error before the heartbeat checker ever starts.
func TestServeCmd_NonPositiveHeartbeatTimeoutRejected(t *testing.T) {
	cases := []struct{ flag, wantDuration string }{
		{"0", "0s"},
		{"-5s", "-5s"},
	}
	for _, c := range cases {
		cmd := serveCmd()
		cmd.SetArgs([]string{
			"--heartbeat-timeout=" + c.flag,
			"--data-dir=" + t.TempDir(),
			"--http-addr=:0",
			"--grpc-addr=:0",
		})
		err := cmd.Execute()
		if err == nil {
			t.Fatalf("--heartbeat-timeout=%s: expected error, got nil", c.flag)
		}
		if want := "--heartbeat-timeout must be positive, got " + c.wantDuration; err.Error() != want {
			t.Errorf("--heartbeat-timeout=%s: unexpected error message: %q, want %q", c.flag, err.Error(), want)
		}
	}
}

// TestWarnIgnoredNormalModeFlags guards against #152: --aggregate mode must
// warn (not silently ignore) when a normal-mode-only flag is explicitly set.
func TestWarnIgnoredNormalModeFlags(t *testing.T) {
	cmd := serveCmd()
	for _, name := range []string{"rules", "grpc-tls-cert", "grpc-tls-key", "region", "data-dir"} {
		if err := cmd.Flags().Set(name, "custom-value"); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
	}
	if err := cmd.Flags().Set("grpc-addr", ":9999"); err != nil {
		t.Fatalf("set grpc-addr: %v", err)
	}

	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	warnIgnoredNormalModeFlags(cmd, ":9999", logger)

	wantSubstrings := []string{
		"--grpc-addr",
		"--data-dir",
		"--rules",
		"--grpc-tls-cert",
		"--grpc-tls-key",
		"--region",
	}
	for _, want := range wantSubstrings {
		found := false
		for _, entry := range logs.All() {
			if entry.Level == zapcore.WarnLevel && strings.Contains(entry.Message, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected a warning mentioning %q, got: %v", want, logs.All())
		}
	}
}

// TestWarnIgnoredNormalModeFlags_NoWarningsWhenUnset ensures no false
// positives when normal-mode flags are left at their defaults.
func TestWarnIgnoredNormalModeFlags_NoWarningsWhenUnset(t *testing.T) {
	cmd := serveCmd()
	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	warnIgnoredNormalModeFlags(cmd, ":50051", logger)

	if logs.Len() != 0 {
		t.Errorf("expected no warnings, got: %v", logs.All())
	}
}
