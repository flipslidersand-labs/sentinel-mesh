package store

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"
)

// openStoreDrained opens a temp store and, before it is closed, waits for the
// goroutines runWithContext leaves behind after an early ctx return; closing
// badger under a running iterator panics.
func openStoreDrained(t *testing.T) *Store {
	t.Helper()
	st := openTempStore(t, t.TempDir())
	base := runtime.NumGoroutine()
	t.Cleanup(func() { // runs before the store's Close (LIFO)
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	})
	return st
}

// ListTokens/getAgentToken must not read closure state that a still-running
// goroutine writes once ctx has ended (#382). Run with -race.
func TestListTokens_CanceledCtxNoRace(t *testing.T) {
	st := openStoreDrained(t)
	for i := 0; i < 20; i++ {
		id := "node-" + strings.Repeat("a", i+1)
		if _, err := st.IssueToken(context.Background(), id); err != nil {
			t.Fatalf("IssueToken: %v", err)
		}
	}
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		toks, err := st.ListTokens(ctx)
		if err != nil && toks != nil {
			t.Fatalf("expected nil tokens on error, got %d", len(toks))
		}
	}
}

func TestGetAgentToken_CanceledCtxNoRace(t *testing.T) {
	st := openStoreDrained(t)
	if _, err := st.IssueToken(context.Background(), "node-a"); err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rec, err := st.getAgentToken(ctx, "node-a")
		if err != nil && rec != nil {
			t.Fatalf("expected nil record on error, got %+v", rec)
		}
	}
}
