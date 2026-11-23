package store

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// One undecodable event record must not make New, ListEvents or Stats fail
// (#372); the broken record is skipped and reported in one warning per call.
func TestCorruptEventRecord_Skipped(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveEventDedup(ctx, model.Event{EventID: "good", NodeID: "n1", Type: "exec", Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	putRaw(t, st, "event:2000-01-01T00:00:00.000000000Z:bad1", `{"event_id":"bad1`)
	putRaw(t, st, "event:2000-01-01T00:00:01.000000000Z:bad2", `not json`)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	buf := captureLogs(t)
	st = openTempStore(t, dir) // must not fail
	stats := st.Stats()
	if stats["exec"] != 1 {
		t.Fatalf("Stats = %v; want exec=1", stats)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 || !strings.Contains(buf.String(), "count=2") {
		t.Errorf("want one warning with count=2 at startup, got: %q", buf.String())
	}

	buf.Reset()
	evs, err := st.ListEventsMatching(ctx, nil, 10)
	if err != nil || len(evs) != 1 || evs[0].EventID != "good" {
		t.Fatalf("ListEvents = %v, %v; want only good", evs, err)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 || !strings.Contains(buf.String(), "count=2") {
		t.Errorf("want one warning with count=2, got: %q", buf.String())
	}
}

func TestCorruptAlertRecord_Skipped(t *testing.T) {
	ctx := context.Background()
	st := openTempStore(t, t.TempDir())
	if _, err := st.SaveAlertDedup(ctx, model.Alert{AlertID: "good", NodeID: "n1", Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	putRaw(t, st, "alert:2000-01-01T00:00:00.000000000Z:bad", `{"alert_id":`)

	buf := captureLogs(t)
	al, err := st.ListAlertsMatching(ctx, nil, 10)
	if err != nil || len(al) != 1 || al[0].AlertID != "good" {
		t.Fatalf("ListAlerts = %v, %v; want only good", al, err)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 || !strings.Contains(buf.String(), "count=1") {
		t.Errorf("want one warning with count=1, got: %q", buf.String())
	}
}
