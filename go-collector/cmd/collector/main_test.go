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
