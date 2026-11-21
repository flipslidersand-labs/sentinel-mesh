package aggregator

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/exporter"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

type contractData struct{}

func (contractData) ListEvents(_ context.Context, _ string, _ int) ([]model.Event, error) {
	return []model.Event{{EventID: "e1", NodeID: "n1"}}, nil
}
func (contractData) ListAlerts(_ context.Context, _ string, _ int) ([]model.Alert, error) {
	return []model.Alert{{AlertID: "a1", NodeID: "n1"}}, nil
}
func (contractData) Stats() (map[string]int, error) { return map[string]int{}, nil }

// TestContract_PollOnceAgainstRealExporterRouter polls the real exporter
// router (not a hand-written fake) so a route or shape change on the exporter
// side breaks this test instead of failing only at runtime.
func TestContract_PollOnceAgainstRealExporterRouter(t *testing.T) {
	reg := registry.New()
	if err := reg.Register("n1", "host-a", "192.0.2.1", "v1", "r1"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := reg.Register("n2", "host-b", "192.0.2.2", "v1", "r1"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	reg.SetInactive("n2")
	d := contractData{}
	srv := httptest.NewServer(exporter.Router(exporter.RouterConfig{
		Events: d, Alerts: d, Stats: d, Nodes: reg, StaticDir: t.TempDir(), APIToken: "tok",
	}))
	defer srv.Close()

	a := New([]Upstream{{Region: "r1", URL: srv.URL}}, time.Minute, "tok", nil)
	a.PollOnce(context.Background())

	rs := a.Regions()
	if len(rs) != 1 || !rs[0].Reachable || rs[0].Error != "" {
		t.Fatalf("region not reachable: %+v", rs)
	}
	if rs[0].NodeCount != 2 || rs[0].ActiveCount != 1 {
		t.Errorf("counts = %d/%d, want 2/1", rs[0].NodeCount, rs[0].ActiveCount)
	}
	if got := a.Events(); len(got) != 1 || got[0].EventID != "e1" {
		t.Errorf("events = %+v", got)
	}
	if got := a.Alerts(); len(got) != 1 || got[0].AlertID != "a1" {
		t.Errorf("alerts = %+v", got)
	}
}
