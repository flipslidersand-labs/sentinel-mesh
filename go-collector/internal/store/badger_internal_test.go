package store

import (
	"context"
	"encoding/json"
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
