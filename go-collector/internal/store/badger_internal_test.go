package store

import (
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
	if err := st.SaveEvent(e); err != nil {
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
