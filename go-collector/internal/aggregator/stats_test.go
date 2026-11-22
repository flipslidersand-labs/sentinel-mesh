package aggregator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

func statsUpstream(t *testing.T, nodes []registry.AgentNode, stats map[string]int) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(nodes) })
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("[]")) })
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("[]")) })
	if stats != nil {
		mux.HandleFunc("/api/stats", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(stats) })
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestAggregateStatsSumsUpstreams(t *testing.T) {
	a1 := statsUpstream(t, nil, map[string]int{"tcp": 3, "dns": 1})
	a2 := statsUpstream(t, nil, map[string]int{"tcp": 4})
	// An upstream without /api/stats (older version) must stay reachable.
	a3 := statsUpstream(t, nil, nil)
	agg := New([]Upstream{{Region: "a", URL: a1.URL}, {Region: "b", URL: a2.URL}, {Region: "c", URL: a3.URL}}, time.Minute, "", nil)
	agg.PollOnce(context.Background())

	w := serve(Router(agg, t.TempDir(), nil, ""), http.MethodGet, "/api/stats", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/stats = %d", w.Code)
	}
	var got map[string]int
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["tcp"] != 7 || got["dns"] != 1 {
		t.Errorf("stats = %v, want tcp=7 dns=1", got)
	}
	for _, r := range agg.Regions() {
		if !r.Reachable {
			t.Errorf("region %s unreachable", r.Region)
		}
	}
}

func TestNodesOrderDeterministic(t *testing.T) {
	mk := func(ids ...string) []registry.AgentNode {
		var out []registry.AgentNode
		for _, id := range ids {
			out = append(out, registry.AgentNode{NodeID: id})
		}
		return out
	}
	ups := []Upstream{}
	for i, r := range []string{"r1", "r2", "r3", "r4", "r5"} {
		ids := []string{"n" + string(rune('a'+i)) + "2", "n" + string(rune('a'+i)) + "1"}
		ups = append(ups, Upstream{Region: r, URL: statsUpstream(t, mk(ids...), nil).URL})
	}
	agg := New(ups, time.Minute, "", nil)
	agg.PollOnce(context.Background())
	ids := func() []string {
		var out []string
		for _, n := range agg.Nodes() {
			out = append(out, n.NodeID)
		}
		return out
	}
	want := ids()
	for i := 0; i < 50; i++ {
		got := ids()
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("order changed: %v vs %v", got, want)
			}
		}
	}
	if want[0] != "na1" || want[1] != "na2" || want[len(want)-1] != "ne2" {
		t.Errorf("not sorted by node_id: %v", want)
	}
}
