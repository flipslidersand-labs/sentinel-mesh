package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

func newInternalStore(t *testing.T) *Store {
	t.Helper()
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	return st
}

func countStored(t *testing.T, st *Store) (stored, execCount int) {
	t.Helper()
	events, err := st.ListEventsMatching(context.Background(), nil, 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	stats := st.Stats()
	return len(events), stats["exec"]
}

// A retry after a timed-out SaveEvent whose background write still landed
// must not store the same event_id a second time (#452).
func TestSaveEvent_RetryAfterTimeoutStoresOnce(t *testing.T) {
	st := newInternalStore(t)

	started := make(chan struct{})
	release := make(chan struct{})
	st.beforeUpdateFn = func() {
		select {
		case <-started:
		default:
			close(started)
			<-release
		}
	}

	e := model.Event{EventID: "e1", NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := st.SaveEventDedup(ctx, e); errCh <- err }()
	<-started
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("SaveEvent returned nil after ctx cancel mid-write; want ctx error")
	}
	close(release)
	st.inflight.Wait() // the first write lands in the background

	// The agent sees Ack ok=false and resends the same event.
	if _, err := st.SaveEventDedup(context.Background(), e); err != nil {
		t.Fatalf("retry SaveEvent: %v", err)
	}

	if stored, n := countStored(t, st); stored != 1 || n != 1 {
		t.Fatalf("stored=%d stats[exec]=%d; want both 1", stored, n)
	}
}

func TestSaveEvent_DuplicateEventIDIsScopedPerNode(t *testing.T) {
	st := newInternalStore(t)
	ctx := context.Background()
	for _, node := range []string{"n1", "n2", "n1:x"} {
		e := model.Event{EventID: "same", NodeID: node, Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
		if _, err := st.SaveEventDedup(ctx, e); err != nil {
			t.Fatalf("SaveEvent(%s): %v", node, err)
		}
	}
	// "n1" + "x:same" must not collide with "n1:x" + "same" either.
	e := model.Event{EventID: "x:same", NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	if _, err := st.SaveEventDedup(ctx, e); err != nil {
		t.Fatalf("SaveEvent: %v", err)
	}
	if stored, n := countStored(t, st); stored != 4 || n != 4 {
		t.Fatalf("stored=%d stats[exec]=%d; want both 4", stored, n)
	}
}

// Events without an event_id cannot be deduplicated and are stored as before.
func TestSaveEvent_EmptyEventIDIsNotDeduplicated(t *testing.T) {
	st := newInternalStore(t)
	e := model.Event{NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	for i := 0; i < 2; i++ {
		if _, err := st.SaveEventDedup(context.Background(), e); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}
	if stored, n := countStored(t, st); stored != 2 || n != 2 {
		t.Fatalf("stored=%d stats[exec]=%d; want both 2", stored, n)
	}
}

// Timed-out writes keep running in the background; once maxInflight of them
// are stuck, further calls fail fast with ErrBusy instead of piling up more
// goroutines (#452).
func TestRun_FailsFastWhenInflightLimitReached(t *testing.T) {
	st := newInternalStore(t)
	st.sem = make(chan struct{}, 2)

	release := make(chan struct{})
	stuck := make(chan struct{}, 2)
	st.beforeUpdateFn = func() {
		stuck <- struct{}{}
		<-release
	}

	for i := 0; i < 2; i++ {
		// Cancel only after the write reached the seam, so a slow goroutine
		// start cannot make fn return early and leave <-stuck waiting (#466).
		ctx, cancel := context.WithCancel(context.Background())
		e := model.Event{EventID: "stuck" + string(rune('a'+i)), NodeID: "n1", Type: "exec", Payload: json.RawMessage(`{}`)}
		done := make(chan error, 1)
		go func() { _, err := st.SaveEventDedup(ctx, e); done <- err }()
		<-stuck
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("SaveEvent %d: err=%v; want Canceled", i, err)
		}
	}

	e := model.Event{EventID: "over", NodeID: "n1", Type: "exec", Payload: json.RawMessage(`{}`)}
	start := time.Now()
	if _, err := st.SaveEventDedup(context.Background(), e); !errors.Is(err, ErrBusy) {
		t.Fatalf("SaveEvent over limit: err=%v; want ErrBusy", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("SaveEvent over limit took %v; want immediate failure", d)
	}

	close(release)
	st.inflight.Wait()
	st.beforeUpdateFn = nil
	if _, err := st.SaveEventDedup(context.Background(), e); err != nil {
		t.Fatalf("SaveEvent after slots freed: %v", err)
	}
	if stored, n := countStored(t, st); stored != 3 || n != 3 {
		t.Fatalf("stored=%d stats[exec]=%d; want both 3", stored, n)
	}
}

// Reads stuck behind a stalled disk must not use up the slots writes need:
// otherwise repeated /api/events requests stop event ingestion (#466).
func TestRun_StuckReadsDoNotBlockWrites(t *testing.T) {
	st := newInternalStore(t)

	release := make(chan struct{})
	entered := make(chan struct{}, maxInflight)
	st.beforeViewFn = func() {
		entered <- struct{}{}
		<-release
	}
	defer func() {
		close(release)
		st.inflight.Wait()
	}()

	// Start reads until the store refuses one; each accepted read is left
	// blocked in the seam, like a View on a stalled disk.
	for i := 0; i <= maxInflight; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := st.ListEventsMatching(ctx, nil, 10)
			done <- err
		}()
		select {
		case <-entered:
			cancel()
			<-done
			continue
		case err := <-done:
			cancel()
			if !errors.Is(err, ErrBusy) {
				t.Fatalf("read %d: err=%v; want ErrBusy once read slots are full", i, err)
			}
		}
		break
	}

	e := model.Event{EventID: "w1", NodeID: "n1", Type: "exec", Payload: json.RawMessage(`{}`)}
	if _, err := st.SaveEventDedup(context.Background(), e); err != nil {
		t.Fatalf("SaveEvent with read slots full: %v; want writes to keep their own slots", err)
	}
}

// SaveEventDedup reports duplicates to the caller (#457).
func TestSaveEventDedup_ReportsDuplicate(t *testing.T) {
	st := newInternalStore(t)
	ctx := context.Background()
	e := model.Event{EventID: "e1", NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	if dup, err := st.SaveEventDedup(ctx, e); err != nil || dup {
		t.Fatalf("first save = dup %v, err %v; want false, nil", dup, err)
	}
	if dup, err := st.SaveEventDedup(ctx, e); err != nil || !dup {
		t.Fatalf("resend = dup %v, err %v; want true, nil", dup, err)
	}
	e.EventID = ""
	for i := 0; i < 2; i++ {
		if dup, err := st.SaveEventDedup(ctx, e); err != nil || dup {
			t.Fatalf("empty event_id save %d = dup %v, err %v; want false, nil", i, dup, err)
		}
	}
}

// #465: alerts are idempotent per (node, event_id, rule).
func TestSaveAlertDedup(t *testing.T) {
	st := newInternalStore(t)
	ctx := context.Background()
	mk := func(id, node, ev, rule string) model.Alert {
		return model.Alert{AlertID: id, NodeID: node, EventID: ev, RuleID: rule, Timestamp: time.Now()}
	}
	cases := []struct {
		a   model.Alert
		dup bool
	}{
		{mk("1", "a:b", "c", "r"), false},
		{mk("2", "a:b", "c", "r"), true},  // same triple
		{mk("3", "a", "b:c", "r"), false}, // ':' must not collide
		{mk("4", "a:b", "c", "r2"), false},
		{mk("5", "a:b", "", "r"), false}, // empty event_id: never deduped
		{mk("6", "a:b", "", "r"), false},
	}
	for i, c := range cases {
		dup, err := st.SaveAlertDedup(ctx, c.a)
		if err != nil || dup != c.dup {
			t.Fatalf("case %d: dup=%v err=%v, want dup=%v", i, dup, err, c.dup)
		}
	}
	got, err := st.ListAlertsMatching(ctx, nil, 100)
	if err != nil || len(got) != 5 {
		t.Fatalf("stored alerts = %d (err %v), want 5", len(got), err)
	}
}
