package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

// alertUpstream serves /api/alerts the way a real collector does: newest
// first, narrowed by ?node=, truncated to ?limit=. When failDirect is set, any
// request that is not the poller's plain limit=eventLimit request (i.e. one
// with node, or a larger limit) fails with 500.
func alertUpstream(t *testing.T, alerts []model.Alert, failDirect bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]registry.AgentNode{{NodeID: "n0", Region: "default", Status: "active"}})
	})
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]model.Event{})
	})
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		node := q.Get("node")
		if failDirect && (node != "" || limit > eventLimit) {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		out := make([]model.Alert, 0)
		for _, a := range alerts {
			if node != "" && a.NodeID != node {
				continue
			}
			if len(out) >= limit {
				break
			}
			out = append(out, a)
		}
		_ = json.NewEncoder(w).Encode(out)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// manyAlerts returns n alerts for node "busy", newest first, followed by one
// older alert for node "quiet" that is outside the 100-entry cache window.
func manyAlerts(n int) []model.Alert {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([]model.Alert, 0, n+1)
	for i := 0; i < n; i++ {
		out = append(out, model.Alert{
			AlertID:   fmt.Sprintf("busy-%04d", i),
			NodeID:    "busy",
			Region:    "default",
			Timestamp: base.Add(-time.Duration(i) * time.Second),
		})
	}
	out = append(out, model.Alert{
		AlertID: "quiet-old", NodeID: "quiet", Region: "default",
		Timestamp: base.Add(-time.Duration(n+1) * time.Hour),
	})
	return out
}

func getAlerts(t *testing.T, r http.Handler, path string) []model.Alert {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("%s code = %d", path, w.Code)
	}
	var out []model.Alert
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// (#553 a) limit above the cache size is served from the upstream.
func TestRouter_AlertsLimitAboveCacheFetchesUpstream(t *testing.T) {
	up := alertUpstream(t, manyAlerts(600), false)
	agg := New([]Upstream{{Region: "tokyo", URL: up.URL}}, 0, "", nil)
	agg.PollOnce(context.Background())
	r := Router(agg, t.TempDir(), nil, "")

	if got := getAlerts(t, r, "/api/alerts"); len(got) != eventLimit {
		t.Fatalf("default limit returned %d alerts, want %d (cached)", len(got), eventLimit)
	}
	got := getAlerts(t, r, "/api/alerts?limit=500")
	if len(got) != 500 {
		t.Fatalf("limit=500 returned %d alerts, want 500", len(got))
	}
	if got[0].Region != "tokyo" {
		t.Errorf("region = %q, want upstream key tokyo", got[0].Region)
	}
}

// (#553 b) a node whose alerts are older than the cache window is found.
func TestRouter_AlertsNodeOlderThanCache(t *testing.T) {
	up := alertUpstream(t, manyAlerts(150), false)
	agg := New([]Upstream{{Region: "tokyo", URL: up.URL}}, 0, "", nil)
	agg.PollOnce(context.Background())
	r := Router(agg, t.TempDir(), nil, "")

	got := getAlerts(t, r, "/api/alerts?node=quiet")
	if len(got) != 1 || got[0].AlertID != "quiet-old" || got[0].Region != "tokyo" {
		t.Fatalf("node=quiet = %+v, want quiet-old with region tokyo", got)
	}
}

// (#553 c) when the direct upstream fetch fails, the cache answers, narrowed
// by node, with region = upstream key, and the cache itself is unchanged.
func TestRouter_AlertsUpstreamFailureFallsBackToCache(t *testing.T) {
	up := alertUpstream(t, manyAlerts(150), true)
	agg := New([]Upstream{{Region: "tokyo", URL: up.URL}}, 0, "", nil)
	agg.PollOnce(context.Background())
	r := Router(agg, t.TempDir(), nil, "")

	got := getAlerts(t, r, "/api/alerts?limit=500")
	if len(got) != eventLimit || got[0].Region != "tokyo" {
		t.Fatalf("limit=500 fallback = %d alerts (region %q), want %d cached with region tokyo", len(got), got[0].Region, eventLimit)
	}
	got = getAlerts(t, r, "/api/alerts?node=busy&limit=5")
	if len(got) != 5 || got[0].NodeID != "busy" {
		t.Fatalf("node=busy fallback = %+v, want 5 cached busy alerts", got)
	}
	if got := getAlerts(t, r, "/api/alerts?node=quiet"); len(got) != 0 {
		t.Fatalf("node=quiet fallback = %+v, want none (not in cache)", got)
	}

	agg.mu.RLock()
	defer agg.mu.RUnlock()
	if c := agg.state["tokyo"].alerts; len(c) != eventLimit || c[0].Region != "default" {
		t.Errorf("cache changed: len=%d region=%q", len(c), c[0].Region)
	}
}

// Alerts from several upstreams are merged newest first, honoring ?region=.
func TestAggregator_FetchAlertsMergesRegions(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := alertUpstream(t, []model.Alert{{AlertID: "a-new", NodeID: "n", Timestamp: ts.Add(2 * time.Second)}, {AlertID: "a-old", NodeID: "n", Timestamp: ts}}, false)
	b := alertUpstream(t, []model.Alert{{AlertID: "b-mid", NodeID: "n", Timestamp: ts.Add(time.Second)}}, false)
	agg := New([]Upstream{{Region: "ra", URL: a.URL}, {Region: "rb", URL: b.URL}}, 0, "", nil)
	agg.PollOnce(context.Background())

	got := agg.FetchAlerts(context.Background(), "", "n", 10)
	if len(got) != 3 || got[0].AlertID != "a-new" || got[1].AlertID != "b-mid" || got[2].AlertID != "a-old" {
		t.Fatalf("merged = %+v, want a-new, b-mid, a-old", got)
	}
	if got[1].Region != "rb" {
		t.Errorf("region = %q, want rb", got[1].Region)
	}
	if got := agg.FetchAlerts(context.Background(), "rb", "n", 10); len(got) != 1 || got[0].AlertID != "b-mid" {
		t.Fatalf("region=rb = %+v, want only b-mid", got)
	}
}
