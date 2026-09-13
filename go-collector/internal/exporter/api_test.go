package exporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func testRouter(t *testing.T) (*store.Store, *registry.Registry) {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, registry.New()
}

func doGET(t *testing.T, st *store.Store, reg *registry.Registry, path string, out any) int {
	t.Helper()
	r := Router(st, reg, nil, t.TempDir(), nil, "")
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if out != nil && w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), out); err != nil {
			t.Fatalf("unmarshal %s: %v (body=%s)", path, err, w.Body.String())
		}
	}
	return w.Code
}

// TestMetrics_RequiresBearerTokenWhenConfigured covers #177: /metrics must
// be gated by the same bearer auth as /api/* when SENTINEL_API_TOKEN is
// set — the exposed Prometheus counters carry node/rule/severity labels,
// the same category of data /api/stats protects.
func TestMetrics_RequiresBearerTokenWhenConfigured(t *testing.T) {
	st, reg := testRouter(t)
	r := Router(st, reg, nil, t.TempDir(), nil, "s3cr3t")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("GET /metrics without token = %d, want 401", w.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer s3cr3t")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /metrics with correct token = %d, want 200", w.Code)
	}
}

// TestHealthz_StaysOpenWhenTokenConfigured verifies the liveness probe
// endpoint is unaffected by bearer auth even when a token is configured —
// only /api/* and /metrics are gated.
func TestHealthz_StaysOpenWhenTokenConfigured(t *testing.T) {
	st, reg := testRouter(t)
	r := Router(st, reg, nil, t.TempDir(), nil, "s3cr3t")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200 (must stay open for probes)", w.Code)
	}
}

func TestNodes_RegionFilter(t *testing.T) {
	st, reg := testRouter(t)
	_ = reg.Register("a1", "h", "ip", "v1", "us-east")
	_ = reg.Register("a2", "h", "ip", "v1", "us-east")
	_ = reg.Register("b1", "h", "ip", "v1", "eu-west")

	var all []registry.AgentNode
	if code := doGET(t, st, reg, "/api/nodes", &all); code != 200 || len(all) != 3 {
		t.Fatalf("nodes: code=%d len=%d, want 200/3", code, len(all))
	}

	var east []registry.AgentNode
	doGET(t, st, reg, "/api/nodes?region=us-east", &east)
	if len(east) != 2 {
		t.Errorf("us-east nodes = %d, want 2", len(east))
	}

	var unknown []registry.AgentNode
	doGET(t, st, reg, "/api/nodes?region=ap-south", &unknown)
	if len(unknown) != 0 {
		t.Errorf("unknown region = %d, want 0", len(unknown))
	}
}

func TestRegions_Summary(t *testing.T) {
	st, reg := testRouter(t)
	_ = reg.Register("a1", "h", "ip", "v1", "us-east")
	_ = reg.Register("a2", "h", "ip", "v1", "us-east")
	_ = reg.Register("b1", "h", "ip", "v1", "eu-west")
	reg.SetInactive("a2")

	var summary []regionSummary
	if code := doGET(t, st, reg, "/api/regions", &summary); code != 200 {
		t.Fatalf("regions: code=%d", code)
	}
	// Regions() is sorted: eu-west, us-east.
	if len(summary) != 2 {
		t.Fatalf("regions = %+v, want 2", summary)
	}
	byName := map[string]regionSummary{}
	for _, s := range summary {
		byName[s.Region] = s
	}
	if e := byName["us-east"]; e.NodeCount != 2 || e.ActiveCount != 1 {
		t.Errorf("us-east = %+v, want node=2 active=1", e)
	}
	if w := byName["eu-west"]; w.NodeCount != 1 || w.ActiveCount != 1 {
		t.Errorf("eu-west = %+v, want node=1 active=1", w)
	}
}

func TestEvents_RegionFilter(t *testing.T) {
	st, reg := testRouter(t)
	_ = reg.Register("a1", "h", "ip", "v1", "us-east")
	_ = reg.Register("b1", "h", "ip", "v1", "eu-west")

	now := time.Now().UTC()
	for i, node := range []string{"a1", "a1", "b1"} {
		if err := st.SaveEvent(context.Background(), store.Event{
			EventID:   node + "-" + string(rune('0'+i)),
			NodeID:    node,
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			Type:      "exec",
			Payload:   json.RawMessage(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}

	var east []store.Event
	doGET(t, st, reg, "/api/events?region=us-east", &east)
	if len(east) != 2 {
		t.Errorf("us-east events = %d, want 2", len(east))
	}
	for _, e := range east {
		if e.NodeID != "a1" {
			t.Errorf("unexpected node in us-east filter: %s", e.NodeID)
		}
	}

	// No region → all events.
	var all []store.Event
	doGET(t, st, reg, "/api/events", &all)
	if len(all) != 3 {
		t.Errorf("all events = %d, want 3", len(all))
	}
}

func TestEvents_InvalidLimit(t *testing.T) {
	st, reg := testRouter(t)

	for _, v := range []string{"abc", "-5", "0"} {
		if code := doGET(t, st, reg, "/api/events?limit="+v, nil); code != http.StatusBadRequest {
			t.Errorf("limit=%s: code=%d, want %d", v, code, http.StatusBadRequest)
		}
	}
}

func TestEvents_LimitClampedToMax(t *testing.T) {
	st, reg := testRouter(t)
	_ = reg.Register("a1", "h", "ip", "v1", "us-east")

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		if err := st.SaveEvent(context.Background(), store.Event{
			EventID:   "a1-" + string(rune('0'+i)),
			NodeID:    "a1",
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			Type:      "exec",
			Payload:   json.RawMessage(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// A limit far above maxLimit is clamped, not rejected: request succeeds
	// and simply returns everything the store has.
	var events []store.Event
	if code := doGET(t, st, reg, "/api/events?limit=999999999", &events); code != http.StatusOK {
		t.Fatalf("code=%d, want 200", code)
	}
	if len(events) != 5 {
		t.Errorf("events = %d, want 5", len(events))
	}
}

func TestAlerts_InvalidLimit(t *testing.T) {
	st, reg := testRouter(t)

	if code := doGET(t, st, reg, "/api/alerts?limit=abc", nil); code != http.StatusBadRequest {
		t.Errorf("limit=abc: code=%d, want %d", code, http.StatusBadRequest)
	}
}
