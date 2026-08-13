package anomaly

import (
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func makeEvent(nodeID, eventType string) store.Event {
	return store.Event{
		EventID: "test-id",
		NodeID:  nodeID,
		Type:    eventType,
	}
}

func TestRecord_NoAlertBelowThreshold(t *testing.T) {
	d := New([]WindowConfig{{Duration: time.Minute, Threshold: 5}})
	ev := makeEvent("node1", "exec")
	for i := 0; i < 5; i++ {
		alerts := d.Record(ev)
		if len(alerts) > 0 {
			t.Fatalf("expected no alerts below threshold, got %d on iteration %d", len(alerts), i)
		}
	}
}

func TestRecord_AlertAtThresholdBreach(t *testing.T) {
	d := New([]WindowConfig{{Duration: time.Minute, Threshold: 3}})
	ev := makeEvent("node1", "exec")

	var lastAlerts []store.Alert
	for i := 0; i < 5; i++ {
		lastAlerts = d.Record(ev)
	}
	if len(lastAlerts) == 0 {
		t.Fatal("expected anomaly alert after threshold breach")
	}
	if lastAlerts[0].Severity != "warning" {
		t.Errorf("expected severity 'warning', got %q", lastAlerts[0].Severity)
	}
}

func TestRecord_AlertContainsNodeAndType(t *testing.T) {
	d := New([]WindowConfig{{Duration: time.Minute, Threshold: 1}})
	ev := makeEvent("minipc", "tcp")
	d.Record(ev)
	alerts := d.Record(ev)
	if len(alerts) == 0 {
		t.Fatal("expected alert")
	}
	a := alerts[0]
	if a.NodeID != "minipc" {
		t.Errorf("NodeID = %q, want 'minipc'", a.NodeID)
	}
	if a.RuleID != "anomaly_tcp" {
		t.Errorf("RuleID = %q, want 'anomaly_tcp'", a.RuleID)
	}
}

func TestRecord_SeparateWindowsPerNode(t *testing.T) {
	d := New([]WindowConfig{{Duration: time.Minute, Threshold: 2}})
	ev1 := makeEvent("node1", "exec")
	ev2 := makeEvent("node2", "exec")

	// node1 breaches threshold
	for i := 0; i < 3; i++ {
		d.Record(ev1)
	}
	// node2 should not breach
	alerts := d.Record(ev2)
	if len(alerts) > 0 {
		t.Fatalf("node2 should not alert, got %d alerts", len(alerts))
	}
}

func TestRecord_ShortestWindowTriggers(t *testing.T) {
	d := New([]WindowConfig{
		{Duration: time.Minute, Threshold: 3},
		{Duration: 5 * time.Minute, Threshold: 20},
	})
	ev := makeEvent("node1", "file")
	for i := 0; i < 4; i++ {
		d.Record(ev)
	}
	alerts := d.Record(ev)
	if len(alerts) == 0 {
		t.Fatal("expected alert from 1m window")
	}
	// Only one alert even though multiple windows could fire
	if len(alerts) > 1 {
		t.Errorf("expected at most 1 alert, got %d", len(alerts))
	}
}

func TestWindowStats_AggregatesAcrossNodes(t *testing.T) {
	d := New([]WindowConfig{
		{Duration: time.Minute, Threshold: 100},
	})
	for i := 0; i < 3; i++ {
		d.Record(makeEvent("node1", "exec"))
	}
	for i := 0; i < 2; i++ {
		d.Record(makeEvent("node2", "exec"))
	}
	stats := d.WindowStats()
	if stats["exec"]["1m"] != 5 {
		t.Errorf("expected exec 1m=5, got %d", stats["exec"]["1m"])
	}
}

func TestWindowStats_EmptyReturnsNoError(t *testing.T) {
	d := New(nil)
	stats := d.WindowStats()
	if stats == nil {
		t.Fatal("expected non-nil stats map")
	}
}

func TestDurationLabel(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "30s"},
		{time.Minute, "1m"},
		{5 * time.Minute, "5m"},
		{time.Hour, "1h"},
	}
	for _, c := range cases {
		got := durationLabel(c.d)
		if got != c.want {
			t.Errorf("durationLabel(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}
