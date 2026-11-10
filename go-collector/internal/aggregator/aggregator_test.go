package aggregator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

	agg := New([]Upstream{{Region: "sus", URL: redirecting.URL}}, 0, "", nil)
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

	agg := New([]Upstream{{Region: "big", URL: huge.URL}}, 0, "", nil)
	agg.PollOnce(context.Background())

	regions := agg.Regions()
	if len(regions) != 1 || regions[0].Reachable {
		t.Errorf("oversized response should fail (truncated JSON) and mark unreachable, got %+v", regions)
	}
}

// TestAggregator_SendsBearerTokenToUpstream covers #176: when the collector
// is configured with SENTINEL_API_TOKEN, the aggregator must send it as an
// Authorization: Bearer header on every upstream request — those endpoints
// are gated by the same httpauth.BearerAuth as the aggregator's own REST
// API, so without this the aggregator gets 401s and marks every region
// unreachable the moment an operator sets that token.
func TestAggregator_SendsBearerTokenToUpstream(t *testing.T) {
	var gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if gotAuth != "Bearer s3cr3t" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode([]registry.AgentNode{})
	}))
	t.Cleanup(upstream.Close)

	agg := New([]Upstream{{Region: "us-east", URL: upstream.URL}}, 0, "s3cr3t", nil)
	agg.PollOnce(context.Background())

	if gotAuth != "Bearer s3cr3t" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer s3cr3t")
	}
	regions := agg.Regions()
	if len(regions) != 1 || !regions[0].Reachable {
		t.Errorf("expected region reachable once bearer token is sent, got %+v", regions)
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
	agg := New(ups, 0, "", nil)
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
	agg := New([]Upstream{{Region: "us-east", URL: east.URL}}, 0, "", nil)
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

// eventually polls cond until it returns true or timeout elapses.
func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s: %s", timeout, msg)
}

// TestAggregator_Start covers Start: the initial synchronous poll, periodic
// re-polling on the interval, and no further polling after ctx is cancelled (#220).
func TestAggregator_Start(t *testing.T) {
	const interval = 20 * time.Millisecond

	var polls atomic.Int64 // one poll == one GET /api/nodes
	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, _ *http.Request) {
		polls.Add(1)
		_ = json.NewEncoder(w).Encode([]registry.AgentNode{{NodeID: "a1", Region: "us-east", Status: "active"}})
	})
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]store.Event{{EventID: "e1", NodeID: "a1", Type: "exec"}})
	})
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]store.Alert{{AlertID: "al1", NodeID: "a1", Severity: "warning"}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	agg := New([]Upstream{{Region: "us-east", URL: srv.URL}}, interval, "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	agg.Start(ctx)

	// 1. The first poll is synchronous: state is populated when Start returns.
	if got := polls.Load(); got < 1 {
		t.Fatalf("polls after Start = %d, want >= 1", got)
	}
	if n := agg.Nodes(); len(n) != 1 || n[0].NodeID != "a1" {
		t.Fatalf("Nodes() right after Start = %+v", n)
	}
	if len(agg.Events()) != 1 || len(agg.Alerts()) != 1 {
		t.Fatalf("events/alerts right after Start = %d/%d, want 1/1", len(agg.Events()), len(agg.Alerts()))
	}
	if r := agg.Regions(); len(r) != 1 || !r[0].Reachable {
		t.Fatalf("Regions() right after Start = %+v", r)
	}

	// 2. The ticker re-polls the upstream after the interval elapses.
	first := polls.Load()
	eventually(t, 5*time.Second, func() bool { return polls.Load() >= first+2 },
		"upstream was not polled again by the ticker")

	// 3. After cancel, polling stops. Allow an in-flight poll to settle first,
	// then the count must stay flat for several intervals.
	cancel()
	time.Sleep(3 * interval)
	settled := polls.Load()
	time.Sleep(5 * interval)
	if got := polls.Load(); got != settled {
		t.Fatalf("polls increased after cancel: %d -> %d", settled, got)
	}
}

// TestAggregator_RegionErrorDoesNotLeakUpstreamDetails (#254): the Error
// string exposed via REST must be a fixed classification, never the raw
// upstream error text (which embeds URLs / internal hostnames).
func TestAggregator_RegionErrorDoesNotLeakUpstreamDetails(t *testing.T) {
	down := httptest.NewServer(http.NewServeMux())
	downURL := down.URL
	down.Close() // connection refused

	status500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(status500.Close)

	badJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	t.Cleanup(badJSON.Close)

	cases := []struct {
		region, url, want string
	}{
		{"down", downURL, "unreachable"},
		{"s500", status500.URL, "upstream error status 500"},
		{"bad", badJSON.URL, "invalid response"},
	}
	var ups []Upstream
	for _, c := range cases {
		ups = append(ups, Upstream{Region: c.region, URL: c.url})
	}
	agg := New(ups, 0, "", nil)
	agg.PollOnce(context.Background())

	got := map[string]RegionStatus{}
	for _, r := range agg.Regions() {
		got[r.Region] = r
	}
	for _, c := range cases {
		r := got[c.region]
		if r.Reachable {
			t.Errorf("%s: expected unreachable", c.region)
		}
		if r.Error != c.want {
			t.Errorf("%s: Error = %q, want %q", c.region, r.Error, c.want)
		}
		if strings.Contains(r.Error, "127.0.0.1") || strings.Contains(r.Error, "http") {
			t.Errorf("%s: Error leaks upstream detail: %q", c.region, r.Error)
		}
	}
}
