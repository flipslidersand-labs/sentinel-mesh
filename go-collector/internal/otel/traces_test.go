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

// シグナルでキャンセル済みの ctx を渡しても Close が未送信 span を flush する (#376)。
func TestTracesProvider_CloseFlushesWithCanceledCtx(t *testing.T) {
	got := make(chan struct{}, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case got <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tp, err := NewTracesProvider(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("NewTracesProvider: %v", err)
	}
	_, span := tp.Tracer().Start(context.Background(), "pending-span")
	span.End() // バッチ内に滞留(5s の定期 flush より前)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tp.Close(canceled); err != nil {
		t.Fatalf("Close with canceled ctx: %v", err)
	}
	select {
	case <-got:
	default:
		t.Fatal("pending span was not flushed on Close with canceled ctx")
	}
}
