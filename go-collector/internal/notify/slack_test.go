package notify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func TestSlackSendErrorOmitsWebhookURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := srv.URL
	srv.Close() // connection refused from here on

	const secret = "/services/DUMMY/SECRET/path"
	n := NewSlackNotifier(base+secret, nil)
	err := n.Send(context.Background(), store.Alert{Timestamp: time.Unix(0, 0)})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, leak := range []string{"SECRET", "/services", base} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("error leaks %q: %v", leak, err)
		}
	}
}

func TestFormatSlackTextEscapes(t *testing.T) {
	got := formatSlackText(store.Alert{
		Severity:  "high",
		Message:   "a & b <!channel>",
		NodeID:    "n`1",
		RuleID:    "r",
		EventID:   "<https://evil.example|click>",
		Timestamp: time.Unix(0, 0),
	})
	for _, bad := range []string{"<!channel>", "<https://", "n`1"} {
		if strings.Contains(got, bad) {
			t.Fatalf("unescaped %q in %q", bad, got)
		}
	}
	for _, want := range []string{"&amp;", "&lt;!channel&gt;", "n1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
}

func TestSlackSendBuildErrorOmitsWebhookURL(t *testing.T) {
	n := NewSlackNotifier("https://hooks.invalid/services/DUMMY/SECRET/path\n", nil)
	err := n.Send(context.Background(), store.Alert{Timestamp: time.Unix(0, 0)})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, leak := range []string{"SECRET", "/services", "hooks.invalid"} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("error leaks %q: %v", leak, err)
		}
	}
}
