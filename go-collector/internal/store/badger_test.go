package store_test

import (
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
		if err := st.SaveEvent(makeEvent(args[0], args[1], i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	got, err := st.ListEvents("", 100)
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
		if err := st.SaveEvent(makeEvent(args[0], args[1], i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	gotA, err := st.ListEvents("node-a", 100)
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

	gotB, err := st.ListEvents("node-b", 100)
	if err != nil {
		t.Fatalf("ListEvents node-b: %v", err)
	}
	if len(gotB) != 1 {
		t.Errorf("node-b: want 1 event, got %d", len(gotB))
	}
}

func TestListEvents_NodeFilter_Unknown(t *testing.T) {
	st := newTempStore(t)
	if err := st.SaveEvent(makeEvent("node-a", "exec", 0)); err != nil {
		t.Fatalf("SaveEvent: %v", err)
	}

	got, err := st.ListEvents("node-x", 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want 0 events for unknown node, got %d", len(got))
	}
}

func TestListEvents_Limit_WithFilter(t *testing.T) {
	st := newTempStore(t)
	for i := 0; i < 5; i++ {
		if err := st.SaveEvent(makeEvent("node-a", "exec", i)); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	got, err := st.ListEvents("node-a", 3)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("want 3 (limit), got %d", len(got))
	}
}
