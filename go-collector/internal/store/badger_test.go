package store

import (
	"encoding/json"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func save(t *testing.T, s *Store, id, node string, ts time.Time) {
	t.Helper()
	if err := s.SaveEvent(Event{
		EventID:   id,
		NodeID:    node,
		Timestamp: ts,
		Type:      "exec",
		Payload:   json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("save %s: %v", id, err)
	}
}

func TestListEventsByNode(t *testing.T) {
	s := newTestStore(t)
	base := time.Unix(1_700_000_000, 0).UTC()
	save(t, s, "e1", "node-a", base)
	save(t, s, "e2", "node-b", base.Add(time.Second))
	save(t, s, "e3", "node-a", base.Add(2*time.Second))

	all, err := s.ListEvents(100, "")
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all = %d, want 3", len(all))
	}

	onlyA, err := s.ListEvents(100, "node-a")
	if err != nil {
		t.Fatalf("list node-a: %v", err)
	}
	if len(onlyA) != 2 {
		t.Fatalf("node-a = %d, want 2", len(onlyA))
	}
	for _, e := range onlyA {
		if e.NodeID != "node-a" {
			t.Fatalf("got node_id %q in node-a filter", e.NodeID)
		}
	}
}

func TestListEventsLimitWithFilter(t *testing.T) {
	s := newTestStore(t)
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 5; i++ {
		save(t, s, "a"+string(rune('0'+i)), "node-a", base.Add(time.Duration(i)*time.Second))
	}
	got, err := s.ListEvents(2, "node-a")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("limit = %d, want 2", len(got))
	}
}
