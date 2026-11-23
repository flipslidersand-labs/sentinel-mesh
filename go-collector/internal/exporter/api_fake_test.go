package exporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

type fakeData struct{}

func (fakeData) ListEventsMatching(_ context.Context, keep func(model.Event) bool, _ int) ([]model.Event, error) {
	out := []model.Event{}
	for _, n := range []string{"n1", "n2"} {
		if e := (model.Event{NodeID: n}); keep == nil || keep(e) {
			out = append(out, e)
		}
	}
	return out, nil
}

func (fakeData) ListAlertsMatching(_ context.Context, keep func(model.Alert) bool, _ int) ([]model.Alert, error) {
	out := []model.Alert{}
	for _, n := range []string{"n1", "n2"} {
		if a := (model.Alert{NodeID: n}); keep == nil || keep(a) {
			out = append(out, a)
		}
	}
	return out, nil
}

func (fakeData) Stats() map[string]int { return map[string]int{"total": 7} }

// TestRouter_AcceptsFakeInterfaces verifies Router depends only on the
// exporter-side interfaces, not on a concrete *store.Store.
func TestRouter_AcceptsFakeInterfaces(t *testing.T) {
	token := strings.Repeat("x", 32)
	f := fakeData{}
	r := Router(RouterConfig{
		Events: f, Alerts: f, Stats: f, Nodes: registry.New(),
		StaticDir: t.TempDir(), APIToken: token,
	})
	for path, want := range map[string]string{
		"/api/events?node=n1": `"node_id":"n1"`,
		"/api/alerts?node=n2": `"node_id":"n2"`,
		"/api/stats":          `"total":7`,
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK || !json.Valid(w.Body.Bytes()) || !strings.Contains(w.Body.String(), want) {
			t.Errorf("%s: code=%d body=%s want contains %s", path, w.Code, w.Body.String(), want)
		}
	}
}
