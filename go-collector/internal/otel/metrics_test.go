package otel

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	sharedOnce sync.Once
	sharedMP   *MetricsProvider
	sharedErr  error
)

// sharedProvider creates the provider once per test process, because
// NewMetricsProvider registers on the Prometheus default registry.
func sharedProvider(t *testing.T) *MetricsProvider {
	t.Helper()
	sharedOnce.Do(func() {
		sharedMP, sharedErr = NewMetricsProvider(context.Background())
	})
	if sharedErr != nil {
		t.Fatalf("NewMetricsProvider: %v", sharedErr)
	}
	return sharedMP
}

// counterValue returns the value of the counter family `name` whose labels
// exactly equal want (ignoring otel scope labels), or 0 if absent.
func counterValue(t *testing.T, name string, want map[string]string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "otel_scope_name", "otel_scope_version", "otel_scope_schema_url":
					continue
				}
				got[lp.GetName()] = lp.GetValue()
			}
			if len(got) != len(want) {
				continue
			}
			match := true
			for k, v := range want {
				if got[k] != v {
					match = false
				}
			}
			if match {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestMetrics_RecordEvent(t *testing.T) {
	mp := sharedProvider(t)
	l := map[string]string{"type": "t-event", "node": "n-event"}
	before := counterValue(t, "sentinel_events_total", l)
	mp.RecordEvent("t-event", "n-event")
	mp.RecordEvent("t-event", "n-event")
	if d := counterValue(t, "sentinel_events_total", l) - before; d != 2 {
		t.Fatalf("delta = %v, want 2", d)
	}
}

func TestMetrics_RecordAlert(t *testing.T) {
	mp := sharedProvider(t)
	l := map[string]string{"rule": "r-1", "severity": "high", "node": "n-alert"}
	before := counterValue(t, "sentinel_alerts_total", l)
	mp.RecordAlert("r-1", "high", "n-alert")
	if d := counterValue(t, "sentinel_alerts_total", l) - before; d != 1 {
		t.Fatalf("delta = %v, want 1", d)
	}
}

func TestMetrics_RecordStoreWriteFailure(t *testing.T) {
	mp := sharedProvider(t)
	for _, kind := range []string{"event", "alert"} {
		l := map[string]string{"kind": kind}
		before := counterValue(t, "sentinel_store_write_failures_total", l)
		mp.RecordStoreWriteFailure(kind)
		if d := counterValue(t, "sentinel_store_write_failures_total", l) - before; d != 1 {
			t.Fatalf("kind=%s delta = %v, want 1", kind, d)
		}
	}
}

func TestMetrics_AuthFailureReasons(t *testing.T) {
	mp := sharedProvider(t)
	// Literal values are part of the dashboard/alerting contract.
	cases := []struct{ konst, literal string }{
		{AuthFailMissingCredentials, "missing_credentials"},
		{AuthFailInvalidToken, "invalid_token"},
		{AuthFailRateLimited, "rate_limited"},
	}
	for _, c := range cases {
		if c.konst != c.literal {
			t.Errorf("constant = %q, want %q", c.konst, c.literal)
		}
		l := map[string]string{"reason": c.literal}
		before := counterValue(t, "sentinel_auth_failures_total", l)
		mp.RecordAuthFailure(c.konst)
		if d := counterValue(t, "sentinel_auth_failures_total", l) - before; d != 1 {
			t.Fatalf("reason=%s delta = %v, want 1", c.literal, d)
		}
	}
}

// Pins current behaviour of calling NewMetricsProvider twice: neither call
// returns an error. NewMetricsProvider cannot take a registerer, but the
// exporter reads prometheus.DefaultRegisterer at call time, so the test swaps
// it for a private registry (restored on cleanup) to keep the real default
// registry, which the other tests gather from, clean. Note: gathering from a
// registry holding two such providers fails with duplicate metrics, so a
// second call in production would break /metrics; that is deliberately not
// asserted here.
func TestNewMetricsProvider_SecondCall(t *testing.T) {
	orig := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = orig })

	for i := 0; i < 2; i++ {
		mp, err := NewMetricsProvider(context.Background())
		if err != nil {
			t.Fatalf("NewMetricsProvider call %d returned error: %v", i+1, err)
		}
		if mp == nil {
			t.Fatalf("provider %d is nil", i+1)
		}
		mp.RecordEvent("t-second", "n-second") // must not panic
	}
}

func TestNodeLabeler_CapAndConsistency(t *testing.T) {
	l := newNodeLabeler(3)
	for _, id := range []string{"a", "b", "c"} {
		if got := l.label(id); got != id {
			t.Fatalf("label(%q) = %q", id, got)
		}
	}
	if got := l.label("d"); got != OtherNodeLabel {
		t.Fatalf("label(d) = %q, want %q", got, OtherNodeLabel)
	}
	if got := l.label("a"); got != "a" {
		t.Fatalf("known id must stay itself, got %q", got)
	}
	if got := l.label("d"); got != OtherNodeLabel {
		t.Fatalf("d must stay folded, got %q", got)
	}
}

func TestNodeLabeler_Concurrent(t *testing.T) {
	l := newNodeLabeler(10)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				l.label(fmt.Sprintf("n-%d-%d", g, i))
			}
		}(g)
	}
	wg.Wait()
	if len(l.seen) != 10 {
		t.Fatalf("seen = %d, want 10", len(l.seen))
	}
}

func distinctNodeLabels(t *testing.T, name string) map[string]bool {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]bool{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "node" {
					out[lp.GetValue()] = true
				}
			}
		}
	}
	return out
}

func TestMetrics_NodeCardinalityBounded(t *testing.T) {
	mp := sharedProvider(t)
	for i := 0; i < MaxNodeLabels+500; i++ {
		id := fmt.Sprintf("throwaway-%d", i)
		mp.RecordEvent("cardinality_test", id)
		mp.RecordAlert("cardinality_rule", "low", id)
	}
	for _, name := range []string{"sentinel_events_total", "sentinel_alerts_total"} {
		if n := len(distinctNodeLabels(t, name)); n > MaxNodeLabels+1 {
			t.Fatalf("%s node series = %d, want <= %d", name, n, MaxNodeLabels+1)
		}
	}
	if v := counterValue(t, "sentinel_events_total", map[string]string{"type": "cardinality_test", "node": OtherNodeLabel}); v < 500 {
		t.Fatalf("other bucket = %v, want >= 500", v)
	}
}
