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
	events, err := st.ListEvents(context.Background(), "", 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	stats, err := st.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
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
	go func() { errCh <- st.SaveEvent(ctx, e) }()
	<-started
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("SaveEvent returned nil after ctx cancel mid-write; want ctx error")
	}
	close(release)
	st.inflight.Wait() // the first write lands in the background

	// The agent sees Ack ok=false and resends the same event.
	if err := st.SaveEvent(context.Background(), e); err != nil {
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
		if err := st.SaveEvent(ctx, e); err != nil {
			t.Fatalf("SaveEvent(%s): %v", node, err)
		}
	}
	// "n1" + "x:same" must not collide with "n1:x" + "same" either.
	e := model.Event{EventID: "x:same", NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	if err := st.SaveEvent(ctx, e); err != nil {
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
		if err := st.SaveEvent(context.Background(), e); err != nil {
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
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		e := model.Event{EventID: "stuck" + string(rune('a'+i)), NodeID: "n1", Type: "exec", Payload: json.RawMessage(`{}`)}
		if err := st.SaveEvent(ctx, e); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("SaveEvent %d: err=%v; want DeadlineExceeded", i, err)
		}
		cancel()
		<-stuck
	}

	e := model.Event{EventID: "over", NodeID: "n1", Type: "exec", Payload: json.RawMessage(`{}`)}
	start := time.Now()
	if err := st.SaveEvent(context.Background(), e); !errors.Is(err, ErrBusy) {
		t.Fatalf("SaveEvent over limit: err=%v; want ErrBusy", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("SaveEvent over limit took %v; want immediate failure", d)
	}

	close(release)
	st.inflight.Wait()
	st.beforeUpdateFn = nil
	if err := st.SaveEvent(context.Background(), e); err != nil {
		t.Fatalf("SaveEvent after slots freed: %v", err)
	}
	if stored, n := countStored(t, st); stored != 3 || n != 3 {
		t.Fatalf("stored=%d stats[exec]=%d; want both 3", stored, n)
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
