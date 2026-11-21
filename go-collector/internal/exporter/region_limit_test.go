package exporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// #324: the region filter must run inside the store scan so limit counts
// matching records, not records fetched before filtering.
func TestRegionFilter_LimitCountsMatches(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck

	reg := registry.New()
	if err := reg.Register("eu-node", "h1", "192.0.2.1", "v", "eu-west"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("us-node", "h2", "192.0.2.2", "v", "us-east"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// 2 old eu-west records, then 5 newer us-east records.
	save := func(i int, node string) {
		id := strconv.Itoa(i)
		ts := time.Now()
		if err := st.SaveEvent(ctx, model.Event{EventID: id, NodeID: node, Timestamp: ts, Type: "exec", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		if err := st.SaveAlert(ctx, model.Alert{AlertID: id, NodeID: node, Timestamp: ts.Add(time.Duration(i) * time.Millisecond)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 7; i++ {
		node := "us-node"
		if i < 2 {
			node = "eu-node"
		}
		save(i, node)
		time.Sleep(2 * time.Millisecond)
	}

	r := Router(RouterConfig{Events: st, Alerts: st, Stats: st, Nodes: reg, StaticDir: t.TempDir(), APIToken: "unused-test-value-0123456789abcdef"})
	for _, path := range []string{"/api/events", "/api/alerts"} {
		req := httptest.NewRequest(http.MethodGet, path+"?region=eu-west&limit=2", nil)
		req.Header.Set("Authorization", "Bearer unused-test-value-0123456789abcdef")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		var got []map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusOK {
			t.Fatalf("%s: code=%d err=%v body=%s", path, w.Code, err, w.Body.String())
		}
		if len(got) != 2 {
			t.Errorf("%s: region=eu-west&limit=2 returned %d records, want 2", path, len(got))
		}
	}
}

type countingData struct {
	fakeData
	calls int
}

func (c *countingData) ListEventsWhere(ctx context.Context, keep func(string) bool, n int) ([]model.Event, error) {
	c.calls++
	return c.fakeData.ListEventsWhere(ctx, keep, n)
}

func (c *countingData) ListAlertsWhere(ctx context.Context, keep func(string) bool, n int) ([]model.Alert, error) {
	c.calls++
	return c.fakeData.ListAlertsWhere(ctx, keep, n)
}

// An unregistered region has no nodes, so nothing can match: answer [] without
// scanning the store (up to maxNodeFilterScan records otherwise).
func TestRegionFilter_UnknownRegionSkipsStore(t *testing.T) {
	f := &countingData{}
	r := Router(RouterConfig{Events: f, Alerts: f, Stats: f, Nodes: registry.New(), StaticDir: t.TempDir(), APIToken: "unused-test-value-0123456789abcdef"})
	for _, path := range []string{"/api/events", "/api/alerts"} {
		req := httptest.NewRequest(http.MethodGet, path+"?region=nowhere", nil)
		req.Header.Set("Authorization", "Bearer unused-test-value-0123456789abcdef")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK || w.Body.String() != "[]" {
			t.Errorf("%s: code=%d body=%s, want 200 []", path, w.Code, w.Body.String())
		}
	}
	if f.calls != 0 {
		t.Errorf("store called %d times for an unknown region, want 0", f.calls)
	}
}
