package aggregator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// fakeCollector serves a minimal /api/{nodes,events,alerts} for one region.
func fakeCollector(t *testing.T, nodes []registry.AgentNode, events []store.Event, alerts []store.Alert) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(nodes)
	})
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(events)
	})
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(alerts)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestParseUpstreams(t *testing.T) {
	ups, err := ParseUpstreams([]string{"us-east=http://a:8081/", "eu-west=http://b:8081"})
	if err != nil {
		t.Fatal(err)
	}
	if len(ups) != 2 || ups[0].Region != "us-east" || ups[0].URL != "http://a:8081" {
		t.Fatalf("parsed = %+v", ups)
	}
	if _, err := ParseUpstreams([]string{"bad-spec"}); err == nil {
		t.Error("expected error for spec without '='")
	}
	if _, err := ParseUpstreams([]string{"=http://x"}); err == nil {
		t.Error("expected error for empty region")
	}
	if _, err := ParseUpstreams([]string{"us-east=file:///etc/passwd"}); err == nil {
		t.Error("expected error for non-http(s) scheme")
	}
	if _, err := ParseUpstreams([]string{"us-east=http://"}); err == nil {
		t.Error("expected error for missing host")
	}
}

// TestAggregator_DoesNotFollowRedirect verifies that a redirect response from
// an upstream is treated as a failed fetch (region marked unreachable)
// rather than followed — a compromised upstream could otherwise 302 the
// aggregator's outbound request to an internal address (SSRF) (#76).
func TestAggregator_DoesNotFollowRedirect(t *testing.T) {
	var redirectTargetHit bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectTargetHit = true
		_, _ = w.Write([]byte(`{"hit": true}`))
	}))
	t.Cleanup(target.Close)

	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(redirecting.Close)

	agg := New([]Upstream{{Region: "sus", URL: redirecting.URL}}, 0, nil)
	agg.PollOnce(context.Background())

	if redirectTargetHit {
		t.Error("aggregator followed the redirect — SSRF via upstream redirect is possible")
	}
	regions := agg.Regions()
	if len(regions) != 1 || regions[0].Reachable {
		t.Errorf("region should be marked unreachable after a redirect, got %+v", regions)
	}
}

// TestAggregator_LimitsResponseSize verifies an oversized upstream response
// fails to decode (safe failure) instead of being read without bound, which
// could otherwise OOM the aggregator (#76).
func TestAggregator_LimitsResponseSize(t *testing.T) {
	huge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("[")) //nolint:errcheck
		for i := 0; i < 2; i++ {
			// One JSON array element per maxResponseBytes worth of padding —
			// two elements guarantees crossing the limit mid-stream.
			w.Write([]byte(`{"node_id":"`)) //nolint:errcheck
			pad := make([]byte, maxResponseBytes)
			for j := range pad {
				pad[j] = 'a'
			}
			w.Write(pad)           //nolint:errcheck
			w.Write([]byte(`"},`)) //nolint:errcheck
		}
		w.Write([]byte("{}]")) //nolint:errcheck
	}))
	t.Cleanup(huge.Close)

	agg := New([]Upstream{{Region: "big", URL: huge.URL}}, 0, nil)
	agg.PollOnce(context.Background())

	regions := agg.Regions()
	if len(regions) != 1 || regions[0].Reachable {
		t.Errorf("oversized response should fail (truncated JSON) and mark unreachable, got %+v", regions)
	}
}

func TestAggregator_MergeAndReachability(t *testing.T) {
	east := fakeCollector(t,
		[]registry.AgentNode{
			{NodeID: "a1", Region: "us-east", Status: "active"},
			{NodeID: "a2", Region: "us-east", Status: "inactive"},
		},
		[]store.Event{{EventID: "e1", NodeID: "a1", Type: "exec"}},
		[]store.Alert{{AlertID: "al1", NodeID: "a1", Severity: "warning"}},
	)

	// Second upstream points at a closed server → unreachable.
	down := httptest.NewServer(http.NewServeMux())
	downURL := down.URL
	down.Close()

	ups := []Upstream{
		{Region: "us-east", URL: east.URL},
		{Region: "eu-west", URL: downURL},
	}
	agg := New(ups, 0, nil)
	agg.PollOnce(context.Background())

	// Merged nodes only include the reachable region.
	if nodes := agg.Nodes(); len(nodes) != 2 {
		t.Errorf("nodes = %d, want 2 (only us-east)", len(nodes))
	}
	if events := agg.Events(); len(events) != 1 {
		t.Errorf("events = %d, want 1", len(events))
	}
	if alerts := agg.Alerts(); len(alerts) != 1 {
		t.Errorf("alerts = %d, want 1", len(alerts))
	}

	// Region status: us-east reachable with counts, eu-west unreachable.
	regions := agg.Regions()
	if len(regions) != 2 {
		t.Fatalf("regions = %+v, want 2", regions)
	}
	byName := map[string]RegionStatus{}
	for _, r := range regions {
		byName[r.Region] = r
	}
	east2 := byName["us-east"]
	if !east2.Reachable || east2.NodeCount != 2 || east2.ActiveCount != 1 {
		t.Errorf("us-east status = %+v, want reachable node=2 active=1", east2)
	}
	west := byName["eu-west"]
	if west.Reachable || west.Error == "" {
		t.Errorf("eu-west status = %+v, want unreachable with error", west)
	}
}

func TestAggregator_RouterServesMergedData(t *testing.T) {
	east := fakeCollector(t,
		[]registry.AgentNode{{NodeID: "a1", Region: "us-east", Status: "active"}},
		[]store.Event{{EventID: "e1", NodeID: "a1", Type: "exec"}},
		nil,
	)
	agg := New([]Upstream{{Region: "us-east", URL: east.URL}}, 0, nil)
	agg.PollOnce(context.Background())

	r := Router(agg, t.TempDir(), nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/regions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/regions code = %d", w.Code)
	}
	var regions []RegionStatus
	if err := json.Unmarshal(w.Body.Bytes(), &regions); err != nil {
		t.Fatal(err)
	}
	if len(regions) != 1 || regions[0].Region != "us-east" || !regions[0].Reachable {
		t.Errorf("regions = %+v", regions)
	}

	// region filter on /api/nodes
	req = httptest.NewRequest(http.MethodGet, "/api/nodes?region=eu-west", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var nodes []registry.AgentNode
	_ = json.Unmarshal(w.Body.Bytes(), &nodes)
	if len(nodes) != 0 {
		t.Errorf("nodes?region=eu-west = %d, want 0", len(nodes))
	}
}
