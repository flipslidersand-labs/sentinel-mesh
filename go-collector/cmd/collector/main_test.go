package main

import (
	"net/http"
	"testing"
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
