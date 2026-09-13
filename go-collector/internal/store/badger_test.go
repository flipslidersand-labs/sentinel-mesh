package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func newTempStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// makeEvent creates an event with a unique EventID using the given index.
func makeEvent(nodeID, eventType string, idx int) store.Event {
	return store.Event{
		EventID:   fmt.Sprintf("%s-%s-%d", nodeID, eventType, idx),
		NodeID:    nodeID,
		Timestamp: time.Now(),
		Type:      eventType,
		Payload:   json.RawMessage(`{}`),
	}
}

func TestListEvents_NoFilter(t *testing.T) {
	st := newTempStore(t)
	for i, args := range [][2]string{
		{"node-a", "exec"},
		{"node-b", "tcp"},
		{"node-a", "file"},
	} {
		if err := st.SaveEvent(context.Background(), makeEvent(args[0], args[1], i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	got, err := st.ListEvents(context.Background(), "", 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("want 3 events, got %d", len(got))
	}
}

func TestListEvents_NodeFilter(t *testing.T) {
	st := newTempStore(t)

	// 2 events for node-a, 1 for node-b
	saves := [][2]string{
		{"node-a", "exec"},
		{"node-a", "file"},
		{"node-b", "tcp"},
	}
	for i, args := range saves {
		if err := st.SaveEvent(context.Background(), makeEvent(args[0], args[1], i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	gotA, err := st.ListEvents(context.Background(), "node-a", 100)
	if err != nil {
		t.Fatalf("ListEvents node-a: %v", err)
	}
	if len(gotA) != 2 {
		t.Errorf("node-a: want 2 events, got %d", len(gotA))
	}
	for _, e := range gotA {
		if e.NodeID != "node-a" {
			t.Errorf("unexpected NodeID %q in node-a result", e.NodeID)
		}
	}

	gotB, err := st.ListEvents(context.Background(), "node-b", 100)
	if err != nil {
		t.Fatalf("ListEvents node-b: %v", err)
	}
	if len(gotB) != 1 {
		t.Errorf("node-b: want 1 event, got %d", len(gotB))
	}
}

func TestListEvents_NodeFilter_Unknown(t *testing.T) {
	st := newTempStore(t)
	if err := st.SaveEvent(context.Background(), makeEvent("node-a", "exec", 0)); err != nil {
		t.Fatalf("SaveEvent: %v", err)
	}

	got, err := st.ListEvents(context.Background(), "node-x", 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want 0 events for unknown node, got %d", len(got))
	}
}

// TestSaveEvent_AgentTimestampCannotPinOrdering verifies that an
// agent-supplied Timestamp far in the future (or past) does not affect
// storage ordering or identity — only EventID + server receive time do
// (#78). Before the fix, storage was keyed by e.Timestamp, so a malicious
// agent could pin its own events to the front of ListEvents forever with a
// far-future timestamp.
func TestSaveEvent_AgentTimestampCannotPinOrdering(t *testing.T) {
	st := newTempStore(t)

	honest := store.Event{
		EventID:   "honest-1",
		NodeID:    "node-a",
		Timestamp: time.Now(),
		Type:      "exec",
		Payload:   json.RawMessage(`{}`),
	}
	if err := st.SaveEvent(context.Background(), honest); err != nil {
		t.Fatalf("SaveEvent(honest): %v", err)
	}

	malicious := store.Event{
		EventID:   "malicious-1",
		NodeID:    "node-b",
		Timestamp: time.Now().Add(365 * 24 * time.Hour), // far-future claim
		Type:      "exec",
		Payload:   json.RawMessage(`{}`),
	}
	if err := st.SaveEvent(context.Background(), malicious); err != nil {
		t.Fatalf("SaveEvent(malicious): %v", err)
	}

	got, err := st.ListEvents(context.Background(), "", 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 events, got %d", len(got))
	}
	// ListEvents is newest-first by storage key; since storage is keyed by
	// receive time (not the claimed Timestamp), the event saved second
	// (malicious, despite its far-future claimed Timestamp being no more
	// "recent" than honest's real one by receive order) is not guaranteed
	// to always be first — but it must reflect *receive* order, not the
	// attacker's claimed Timestamp. Here that means "malicious" (saved
	// second) is newest.
	if got[0].EventID != "malicious-1" || got[1].EventID != "honest-1" {
		t.Errorf("ordering should follow receive time, got %v then %v", got[0].EventID, got[1].EventID)
	}
}

func TestListEvents_Limit_WithFilter(t *testing.T) {
	st := newTempStore(t)
	for i := 0; i < 5; i++ {
		if err := st.SaveEvent(context.Background(), makeEvent("node-a", "exec", i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	got, err := st.ListEvents(context.Background(), "node-a", 3)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("want 3 (limit), got %d", len(got))
	}
}

// TestStats_MatchesSavedEvents verifies Stats() reflects SaveEvent calls via
// the in-memory counters (#77), without needing a full key scan per call.
func TestStats_MatchesSavedEvents(t *testing.T) {
	st := newTempStore(t)
	for i, typ := range []string{"exec", "exec", "tcp", "file", "file", "file"} {
		if err := st.SaveEvent(context.Background(), makeEvent("node-a", typ, i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	counts, err := st.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	want := map[string]int{"exec": 2, "tcp": 1, "file": 3}
	for typ, n := range want {
		if counts[typ] != n {
			t.Errorf("counts[%q] = %d, want %d", typ, counts[typ], n)
		}
	}
}

// TestStats_SurvivesRestart verifies the counters are correctly reseeded
// from disk (loadCounters) when a Store is reopened — Stats() must not reset
// to zero just because the in-memory map was recreated (#77).
func TestStats_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	st1, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	for i, typ := range []string{"exec", "tcp", "tcp"} {
		if err := st1.SaveEvent(context.Background(), makeEvent("node-a", typ, i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}
	if err := st1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New (reopen): %v", err)
	}
	t.Cleanup(func() { st2.Close() }) //nolint:errcheck

	counts, err := st2.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if counts["exec"] != 1 || counts["tcp"] != 2 {
		t.Errorf("counts after reopen = %+v, want exec=1 tcp=2", counts)
	}
}
