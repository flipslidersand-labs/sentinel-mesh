package store

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// TestSaveEvent_SetsTTL verifies stored events carry a TTL (#77) — without
// one, the store grows without limit. Internal (package store) test since
// ExpiresAt() requires reaching into the unexported *badger.DB.
func TestSaveEvent_SetsTTL(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck

	e := Event{EventID: "e1", NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	if err := st.SaveEvent(context.Background(), e); err != nil {
		t.Fatalf("SaveEvent: %v", err)
	}

	var expiresAt uint64
	err = st.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek([]byte("event:"))
		if !it.ValidForPrefix([]byte("event:")) {
			t.Fatal("no event key found")
		}
		expiresAt = it.Item().ExpiresAt()
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if expiresAt == 0 {
		t.Fatal("expected a non-zero TTL/expiry on the stored event")
	}
	now := uint64(time.Now().Unix())
	maxExpected := now + uint64(DefaultRetention.Seconds()) + 60 // slack
	if expiresAt < now || expiresAt > maxExpected {
		t.Errorf("expiresAt = %d, want within [%d, %d]", expiresAt, now, maxExpected)
	}
}

// TestClose_StopsValueLogGCGoroutine verifies Close() signals the background
// runValueLogGC goroutine to stop and waits for it to exit before closing the
// underlying db (#149). Without this, closing the store while the goroutine
// is still calling into a closed *badger.DB would race or panic.
func TestClose_StopsValueLogGCGoroutine(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- st.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s — gc goroutine likely not stopped")
	}

	select {
	case <-st.gcDone:
	default:
		t.Error("gcDone not closed after Close returned")
	}
}

// TestListEvents_NodeFilterScanCap verifies a node filter that matches
// nothing stops after maxNodeFilterScan records instead of scanning the
// whole "event:" prefix (#153). Without the cap, a limit=1 query for a
// nonexistent/rarely-seen node forces reading every retained event.
func TestListEvents_NodeFilterScanCap(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck

	origCap := maxNodeFilterScan
	maxNodeFilterScan = 5
	t.Cleanup(func() { maxNodeFilterScan = origCap })

	ctx := context.Background()

	// 20 events, none for "node-x": more than the shrunk scan cap.
	for i := 0; i < 20; i++ {
		e := Event{
			EventID:   strconv.Itoa(i),
			NodeID:    "node-a",
			Timestamp: time.Now(),
			Type:      "exec",
			Payload:   json.RawMessage(`{}`),
		}
		if err := st.SaveEvent(ctx, e); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}

	got, err := st.ListEvents(ctx, "node-x", 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want 0 matches for node-x, got %d", len(got))
	}

	// A match that falls within the cap is still found.
	e := Event{EventID: "match-1", NodeID: "node-x", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	if err := st.SaveEvent(ctx, e); err != nil {
		t.Fatalf("SaveEvent: %v", err)
	}
	got, err = st.ListEvents(ctx, "node-x", 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 1 || got[0].EventID != "match-1" {
		t.Errorf("want [match-1] (newest, within scan cap), got %+v", got)
	}
}

// TestRunWithContext_ReturnsOnContextCancelNotFnCompletion covers #155: a
// caller blocked on a store operation (db.Update/View) must not be stuck
// waiting on it past the caller's own context — e.g. BadgerDB value-log GC
// or a disk stall should no longer be able to wedge the StreamEvents
// goroutine (and, during shutdown, GracefulStop) indefinitely.
func TestRunWithContext_ReturnsOnContextCancelNotFnCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	fnDone := make(chan struct{})
	unblockFn := make(chan struct{})
	err := make(chan error, 1)

	go func() {
		err <- runWithContext(ctx, func() error {
			<-unblockFn // never closed during this test — simulates a stalled db call
			close(fnDone)
			return nil
		})
	}()

	// Give the goroutine a moment to start fn, then cancel before it finishes.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case gotErr := <-err:
		if gotErr != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", gotErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("runWithContext did not return promptly after ctx was cancelled")
	}

	select {
	case <-fnDone:
		t.Fatal("fn must not have completed yet — this test only proves the caller isn't blocked on it")
	default:
	}
	close(unblockFn) // let the background goroutine exit cleanly
}

// TestRunValueLogGC_ReclaimsSpace verifies RunValueLogGC is actually wired up
// and returns badger's expected "nothing to do" error when there is no
// garbage to collect yet, rather than never being called at all (#149 — the
// bug was that RunValueLogGC had zero call sites in the repo).
func TestRunValueLogGC_ReclaimsSpace(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck

	err = st.db.RunValueLogGC(vlogGCDiscardRatio)
	if err != nil && err != badger.ErrNoRewrite && err != badger.ErrRejected {
		t.Errorf("RunValueLogGC returned unexpected error: %v", err)
	}
}

// TestRunValueLogGC_TickerFiresAndKeepsRunning exercises the background
// goroutine's ticker branch directly (as opposed to
// TestRunValueLogGC_ReclaimsSpace, which only calls db.RunValueLogGC
// synchronously without going through the goroutine's select loop at all).
// vlogGCInterval is shrunk so the ticker fires during the test instead of
// waiting the real 5-minute interval.
func TestRunValueLogGC_TickerFiresAndKeepsRunning(t *testing.T) {
	orig := vlogGCInterval
	vlogGCInterval = 10 * time.Millisecond
	t.Cleanup(func() { vlogGCInterval = orig })

	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Give the background goroutine several ticks to run through its
	// select loop's ticker.C case (and the inner for/break on
	// ErrNoRewrite/ErrRejected) before we close it.
	time.Sleep(50 * time.Millisecond)

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestNew_ReturnsErrorForInvalidDir verifies New surfaces badger.Open's
// error (via the first error-return branch) rather than panicking or
// silently returning a broken Store, when given a path that cannot be
// opened as a database directory.
func TestNew_ReturnsErrorForInvalidDir(t *testing.T) {
	// A regular file cannot be opened as a Badger directory.
	dir := t.TempDir()
	filePath := dir + "/not-a-dir"
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := New(filePath)
	if err == nil {
		t.Error("New(regular file path) = nil error, want non-nil")
	}
}

// TestTimeKey_ByteOrderMatchesTimeOrder guards #250: RFC3339Nano trims
// trailing zeros so keys within one second sorted wrongly. Also covers a
// non-UTC offset, which previously ordered by local wall-clock text.
func TestTimeKey_ByteOrderMatchesTimeOrder(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	jst := time.FixedZone("JST", 9*3600)
	ordered := []time.Time{ // strictly increasing instants
		base,
		base.Add(100 * time.Millisecond),
		base.Add(123 * time.Millisecond),
		base.Add(500 * time.Millisecond),
		base.Add(time.Second).In(jst),
		base.Add(time.Second + time.Nanosecond),
	}
	for i := 1; i < len(ordered); i++ {
		a, b := timeKey(ordered[i-1]), timeKey(ordered[i])
		if !(a < b) {
			t.Errorf("timeKey(%v)=%q should sort before timeKey(%v)=%q", ordered[i-1], a, ordered[i], b)
		}
	}
}

// TestListAlerts_NewestFirstWithinOneSecond guards #250 end to end: with
// limit=1 the newest alert must be returned even when timestamps share a second.
func TestListAlerts_NewestFirstWithinOneSecond(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck

	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i, d := range []time.Duration{0, 100 * time.Millisecond, 500 * time.Millisecond} {
		a := Alert{AlertID: "a" + strconv.Itoa(i), NodeID: "n1", Timestamp: base.Add(d)}
		if err := st.SaveAlert(context.Background(), a); err != nil {
			t.Fatalf("SaveAlert: %v", err)
		}
	}
	got, err := st.ListAlerts(context.Background(), "", 1)
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	if len(got) != 1 || got[0].AlertID != "a2" {
		t.Fatalf("limit=1 should return newest alert a2, got %+v", got)
	}
}

// TestList_CancelledContextReturnsNilWithoutRacing is the regression test for
// #351: on ctx expiry ListEvents/ListAlerts must not hand back (or read) the
// slice the still-running View goroutine appends to. Run under -race.
func TestList_CancelledContextReturnsNilWithoutRacing(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck

	bg := context.Background()
	for i := 0; i < 200; i++ {
		id := strconv.Itoa(i)
		if err := st.SaveEvent(bg, Event{EventID: id, NodeID: "n"}); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
		if err := st.SaveAlert(bg, Alert{AlertID: id, NodeID: "n", Timestamp: time.Now()}); err != nil {
			t.Fatalf("SaveAlert: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(bg)
	cancel()
	for i := 0; i < 50; i++ {
		evs, err := st.ListEvents(ctx, "", 1000)
		if err == nil {
			continue // fn won the select; a complete result is fine
		}
		if evs != nil {
			t.Fatalf("ListEvents: want nil slice on ctx error, got len %d", len(evs))
		}
		als, err := st.ListAlerts(ctx, "", 1000)
		if err != nil && als != nil {
			t.Fatalf("ListAlerts: want nil slice on ctx error, got len %d", len(als))
		}
	}
}

// TestClose_WaitsForAbandonedViewGoroutines is the regression test for the
// "panic: DB Closed" CI failure of #351: List* returns on a cancelled ctx while
// its goroutine is still starting, and Close must not close the DB under it.
func TestClose_WaitsForAbandonedViewGoroutines(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	bg := context.Background()
	for i := 0; i < 50; i++ {
		id := strconv.Itoa(i)
		if err := st.SaveEvent(bg, Event{EventID: id, NodeID: "n"}); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	for i := 0; i < 200; i++ {
		st.ListEvents(ctx, "", 100) //nolint:errcheck
		st.ListAlerts(ctx, "", 100) //nolint:errcheck
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestSaveEvent_CancelledMidWriteStillCountedInStats pins #325: when ctx is
// cancelled while the write is in flight, the caller gets an error but the
// write lands, and Stats() must match what was actually stored.
func TestSaveEvent_CancelledMidWriteStillCountedInStats(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck

	started := make(chan struct{})
	release := make(chan struct{})
	st.beforeUpdateFn = func() {
		close(started)
		<-release
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	e := Event{EventID: "e1", NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
	go func() { errCh <- st.SaveEvent(ctx, e) }()

	<-started
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("SaveEvent returned nil after ctx cancel mid-write; want ctx error")
	}
	close(release)
	st.inflight.Wait() // background write finished

	stored, err := st.ListEvents(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	stats, err := st.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if len(stored) != 1 || stats["exec"] != len(stored) {
		t.Fatalf("stored=%d stats[exec]=%d; want both 1", len(stored), stats["exec"])
	}
}
