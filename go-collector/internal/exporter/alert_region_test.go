package exporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// #531: alerts carry their region, so ?region= keeps matching after the node
// left the registry; alerts stored without a region (older versions) still
// match through the registry.
func TestAlertsRegionFilter_UsesStoredAlertRegion(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	ctx := context.Background()
	for _, a := range []model.Alert{
		{AlertID: "gone", NodeID: "evicted-node", Region: "eu-west"},  // not in registry any more
		{AlertID: "other", NodeID: "us-node", Region: "us-east"},      // other region
		{AlertID: "legacy-in", NodeID: "legacy-node"},                 // no region, registered in eu-west
		{AlertID: "legacy-out", NodeID: "unknown-legacy"},             // no region, not registered
		{AlertID: "moved", NodeID: "legacy-node", Region: "ap-south"}, // stamped region wins over the registry
	} {
		a.Timestamp = time.Now().UTC()
		if _, err := st.SaveAlertDedup(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	reg := registry.New()
	if err := reg.Register("legacy-node", "h", "192.0.2.1", "v", "eu-west"); err != nil {
		t.Fatal(err)
	}

	r := Router(RouterConfig{Events: st, Alerts: st, Stats: st, Nodes: reg, StaticDir: t.TempDir(), APIToken: "unused-test-value-0123456789abcdef"})
	ids := func(query string) map[string]bool {
		req := httptest.NewRequest(http.MethodGet, "/api/alerts?"+query, nil)
		req.Header.Set("Authorization", "Bearer unused-test-value-0123456789abcdef")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		var got []model.Alert
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusOK {
			t.Fatalf("%s: code=%d err=%v body=%s", query, w.Code, err, w.Body.String())
		}
		out := map[string]bool{}
		for _, a := range got {
			out[a.AlertID] = true
		}
		return out
	}
	if got := ids("region=eu-west"); len(got) != 2 || !got["gone"] || !got["legacy-in"] {
		t.Errorf("region=eu-west = %v, want gone + legacy-in", got)
	}
	if got := ids("region=ap-south"); len(got) != 1 || !got["moved"] {
		t.Errorf("region=ap-south = %v, want moved", got)
	}
	if got := ids("region=nowhere"); len(got) != 0 {
		t.Errorf("region=nowhere = %v, want none", got)
	}
	if got := ids("region=eu-west&node=evicted-node"); len(got) != 1 || !got["gone"] {
		t.Errorf("region+node = %v, want gone", got)
	}
}
