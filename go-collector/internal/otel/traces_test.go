package otel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// scheme 付き endpoint (help の例 http://localhost:4318 と同形式) で
// 実際にその URL へ OTLP/HTTP リクエストが届くことを検証する。
func TestNewTracesProvider_EndpointWithScheme(t *testing.T) {
	paths := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case paths <- r.URL.Path:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx := context.Background()
	tp, err := NewTracesProvider(ctx, srv.URL) // http://127.0.0.1:port
	if err != nil {
		t.Fatalf("NewTracesProvider: %v", err)
	}
	_, span := tp.Tracer().Start(ctx, "test-span")
	span.End()

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = tp.Close(cctx) // flush

	select {
	case p := <-paths:
		if p != "/v1/traces" {
			t.Fatalf("path = %q, want /v1/traces", p)
		}
	default:
		t.Fatal("no trace request reached the http:// endpoint")
	}
}

// scheme なし host:port は従来どおり受理される (https 扱い)。
func TestNewTracesProvider_HostPortOnly(t *testing.T) {
	tp, err := NewTracesProvider(context.Background(), "localhost:4318")
	if err != nil {
		t.Fatalf("NewTracesProvider: %v", err)
	}
	_ = tp.Close(context.Background())
}

func TestNewTracesProvider_Empty(t *testing.T) {
	tp, err := NewTracesProvider(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := tp.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
