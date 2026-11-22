package receiver

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/flipslidersand/sentinel-mesh/internal/otel"
)

func throttledTotal(t *testing.T) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "sentinel_events_throttled_total" {
			m := mf.GetMetric()
			if len(m) != 1 || len(m[0].GetLabel()) != 0 {
				t.Fatalf("throttled counter must be a single unlabelled series, got %v", m)
			}
			return m[0].GetCounter().GetValue()
		}
	}
	return 0
}

// #420: events dropped by the per-node rate limit must be counted, not just
// logged once per 1000.
func TestStreamEvents_RateLimitDropsAreCounted(t *testing.T) {
	s, _ := newLimiterTestServer(t)
	mp, err := otel.NewMetricsProvider(nil)
	if err != nil {
		t.Fatal(err)
	}
	s.metrics = mp
	before := throttledTotal(t)
	drainNodeBudget(t, s, "node-1") // burst+2000 events, the last 2000 are dropped
	if got := throttledTotal(t) - before; got != 2000 {
		t.Fatalf("throttled counter delta = %v, want 2000", got)
	}
}
