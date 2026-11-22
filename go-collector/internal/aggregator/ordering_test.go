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
)

// limitAwareCollector is a fake upstream that, like the real exporter, returns
// events in its own (receive-time, newest first) order and honors the node and
// limit query parameters.
func limitAwareCollector(t *testing.T, events []model.Event) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("[]")) })
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("[]")) })
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		node := r.URL.Query().Get("node")
		out := []model.Event{}
		for _, e := range events {
			if node != "" && e.NodeID != node {
				continue
			}
			if len(out) >= limit {
				break
			}
			out = append(out, e)
		}
		_ = json.NewEncoder(w).Encode(out)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func getEventIDs(t *testing.T, a *Aggregator, path string) []string {
	t.Helper()
	w := serve(Router(a, t.TempDir(), nil, ""), http.MethodGet, path, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("%s = %d", path, w.Code)
	}
	var evs []model.Event
	if err := json.Unmarshal(w.Body.Bytes(), &evs); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(evs))
	for _, e := range evs {
		ids = append(ids, e.EventID)
	}
	return ids
}

// A node with a fast clock must not float to the top: the upstream's own
// order (collector receive order, #78) is authoritative, not the
// agent-supplied Timestamp.
func TestEvents_SkewedAgentClockDoesNotDominate(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	srv := limitAwareCollector(t, []model.Event{
		{EventID: "fresh", NodeID: "good", Timestamp: now},
		{EventID: "skewed", NodeID: "fast", Timestamp: now.Add(time.Hour)},
		{EventID: "older", NodeID: "good", Timestamp: now.Add(-time.Minute)},
	})
	a := New([]Upstream{{Region: "r1", URL: srv.URL}}, 0, "", nil)
	a.PollOnce(context.Background())

	if got := getEventIDs(t, a, "/api/events?limit=1"); len(got) != 1 || got[0] != "fresh" {
		t.Fatalf("limit=1 = %v, want [fresh]", got)
	}
	if got := a.EventsIn(""); len(got) != 3 || got[0].EventID != "fresh" || got[1].EventID != "skewed" || got[2].EventID != "older" {
		t.Fatalf("Events() = %+v, want upstream order", got)
	}
}

// A node whose events fall outside the per-region cache window, and a limit
// above the cache size, must be served from the upstream directly.
func TestEvents_NodeAndLargeLimitBypassCache(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var events []model.Event
	for i := 0; i < 150; i++ {
		events = append(events, model.Event{EventID: fmt.Sprintf("busy-%03d", i), NodeID: "busy", Timestamp: base})
	}
	events = append(events,
		model.Event{EventID: "quiet-1", NodeID: "quiet", Timestamp: base},
		model.Event{EventID: "quiet-2", NodeID: "quiet", Timestamp: base})
	srv := limitAwareCollector(t, events)
	a := New([]Upstream{{Region: "r1", URL: srv.URL}}, 0, "", nil)
	a.PollOnce(context.Background())

	if got := getEventIDs(t, a, "/api/events?node=quiet"); len(got) != 2 || got[0] != "quiet-1" {
		t.Errorf("node=quiet = %v, want 2 events", got)
	}
	if got := getEventIDs(t, a, "/api/events?region=r1&node=quiet&limit=1"); len(got) != 1 || got[0] != "quiet-1" {
		t.Errorf("region+node+limit=1 = %v", got)
	}
	if got := getEventIDs(t, a, "/api/events?limit=120"); len(got) != 120 {
		t.Errorf("limit=120 returned %d events, want 120", len(got))
	}
	if got := getEventIDs(t, a, "/api/events?region=nope&node=quiet"); len(got) != 0 {
		t.Errorf("unknown region = %v, want empty", got)
	}
}

// When the direct upstream fetch fails, that region falls back to the cache
// narrowed by node; a region whose fetch succeeds is served from upstream.
func TestEvents_DirectFetchFailureFallsBackToCache(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cached := []model.Event{
		{EventID: "c-n1", NodeID: "n1", Timestamp: base},
		{EventID: "c-n2", NodeID: "n2", Timestamp: base.Add(-time.Second)},
	}
	failing := false
	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("[]")) })
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("[]")) })
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		if failing && r.URL.Query().Get("node") != "" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(cached)
	})
	bad := httptest.NewServer(mux)
	t.Cleanup(bad.Close)
	good := limitAwareCollector(t, []model.Event{{EventID: "g-n1", NodeID: "n1", Timestamp: base.Add(-time.Minute)}})

	a := New([]Upstream{{Region: "bad", URL: bad.URL}, {Region: "good", URL: good.URL}}, 0, "", nil)
	a.PollOnce(context.Background())
	failing = true

	got := getEventIDs(t, a, "/api/events?node=n1")
	if len(got) != 2 || got[0] != "c-n1" || got[1] != "g-n1" {
		t.Fatalf("node=n1 = %v, want [c-n1 g-n1]", got)
	}
}

// Across regions the head comparison must use the collector receive time, so a
// region holding fast-clock events does not fill the top of the result (#473).
func TestEvents_CrossRegionUsesReceivedAt(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fast := now.Add(time.Hour)
	srvA := limitAwareCollector(t, []model.Event{
		{EventID: "a1", NodeID: "fast", Timestamp: fast, ReceivedAt: now.Add(-3 * time.Second)},
		{EventID: "a2", NodeID: "fast", Timestamp: fast, ReceivedAt: now.Add(-4 * time.Second)},
		{EventID: "a3", NodeID: "fast", Timestamp: fast, ReceivedAt: now.Add(-5 * time.Second)},
	})
	srvB := limitAwareCollector(t, []model.Event{
		{EventID: "b1", NodeID: "good", Timestamp: now, ReceivedAt: now.Add(-1 * time.Second)},
		{EventID: "b2", NodeID: "good", Timestamp: now, ReceivedAt: now.Add(-2 * time.Second)},
	})
	a := New([]Upstream{{Region: "ra", URL: srvA.URL}, {Region: "rb", URL: srvB.URL}}, 0, "", nil)
	a.PollOnce(context.Background())

	want := []string{"b1", "b2", "a1"}
	got := getEventIDs(t, a, "/api/events?limit=3")
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("limit=3 = %v, want %v", got, want)
	}
}

// Upstreams that predate received_at omit it; those events fall back to Timestamp.
func TestEvents_MissingReceivedAtFallsBackToTimestamp(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	srvA := limitAwareCollector(t, []model.Event{{EventID: "old", NodeID: "n", Timestamp: now.Add(-time.Minute)}})
	srvB := limitAwareCollector(t, []model.Event{{EventID: "new", NodeID: "n", Timestamp: now}})
	a := New([]Upstream{{Region: "ra", URL: srvA.URL}, {Region: "rb", URL: srvB.URL}}, 0, "", nil)
	a.PollOnce(context.Background())
	if got := getEventIDs(t, a, "/api/events?limit=2"); fmt.Sprint(got) != "[new old]" {
		t.Fatalf("got %v, want [new old]", got)
	}
}
