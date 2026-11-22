package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

func TestNodeRegion_UnknownNode(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	if r, ok, err := st.NodeRegion(context.Background(), "nope"); err != nil || ok || r != "" {
		t.Fatalf("NodeRegion = %q, %v, %v; want empty, false, nil", r, ok, err)
	}
	if err := st.SaveNodeRegion(context.Background(), "n", ""); err == nil {
		t.Error("empty region must be rejected")
	}
}

// Reopening the store (a collector restart) must keep the node's region and the
// region stamped on stored events (#512).
func TestNodeRegion_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveEventDedup(ctx, model.Event{EventID: "e1", NodeID: "node-a", Region: "eu-west", Type: "exec", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	if r, ok, err := st.NodeRegion(ctx, "node-a"); err != nil || !ok || r != "eu-west" {
		t.Fatalf("NodeRegion after reopen = %q, %v, %v; want eu-west, true, nil", r, ok, err)
	}
	got, err := st.ListEventsMatching(ctx, func(e model.Event) bool { return e.Region == "eu-west" }, 10)
	if err != nil || len(got) != 1 || got[0].Region != "eu-west" {
		t.Fatalf("ListEventsMatching = %+v, %v; want the eu-west event", got, err)
	}
}

func TestNodeRegion_HasRetentionTTL(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	if err := st.SaveNodeRegion(context.Background(), "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	var expiresAt uint64
	if err := st.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(nodeRegionPrefix + "node-a"))
		if err == nil {
			expiresAt = item.ExpiresAt()
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	now := uint64(time.Now().Unix())
	if max := now + uint64(DefaultRetention.Seconds()) + 60; expiresAt < now || expiresAt > max {
		t.Fatalf("expiresAt = %d, want within [%d, %d]", expiresAt, now, max)
	}
}

func TestNodeRegion_ExpiresAfterRetention(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	st.retention = time.Second
	ctx := context.Background()
	if err := st.SaveNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2200 * time.Millisecond) // Badger TTLs have 1s granularity
	if r, ok, err := st.NodeRegion(ctx, "node-a"); err != nil || ok {
		t.Fatalf("NodeRegion after TTL = %q, %v, %v; want absent", r, ok, err)
	}
}
