package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newConcurrencyStore(t *testing.T) *Store {
	t.Helper()
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// Run with -race: issue/revoke on several nodes while lookups hammer the index.
func TestTokenIndex_ConcurrentIssueRevokeLookup(t *testing.T) {
	st := newConcurrencyStore(t)
	ctx := context.Background()
	const nodes = 4
	const rounds = 20

	stable, err := st.IssueToken(ctx, "stable")
	if err != nil {
		t.Fatal(err)
	}

	var tokens sync.Map // last token per node, for lookers
	stop := make(chan struct{})
	var lookers sync.WaitGroup
	for i := 0; i < 4; i++ {
		lookers.Add(1)
		go func() {
			defer lookers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if id, ok, err := st.LookupNodeID(ctx, stable); err != nil || !ok || id != "stable" {
					t.Errorf("stable lookup = (%q, %v, %v)", id, ok, err)
					return
				}
				tokens.Range(func(k, v any) bool {
					if id, ok, err := st.LookupNodeID(ctx, v.(string)); err != nil || (ok && id != k.(string)) {
						t.Errorf("lookup for %v = (%q, %v, %v)", k, id, ok, err)
					}
					return true
				})
			}
		}()
	}

	var writers sync.WaitGroup
	for n := 0; n < nodes; n++ {
		writers.Add(1)
		go func(node string) {
			defer writers.Done()
			for r := 0; r < rounds; r++ {
				tok, err := st.IssueToken(ctx, node)
				if err != nil {
					t.Errorf("issue %s: %v", node, err)
					return
				}
				tokens.Store(node, tok)
				if err := st.RevokeToken(ctx, node); err != nil {
					t.Errorf("revoke %s: %v", node, err)
					return
				}
				if _, ok, _ := st.LookupNodeID(ctx, tok); ok {
					t.Errorf("%s: revoked token still resolves", node)
					return
				}
			}
		}(fmt.Sprintf("node-%d", n))
	}
	writers.Wait()
	close(stop)
	lookers.Wait()
}

func TestIssueToken_WriteFailureLeavesNoIndexEntry(t *testing.T) {
	st := newConcurrencyStore(t)
	ctx := context.Background()
	boom := errors.New("injected write failure")
	st.putTokenFn = func(context.Context, AgentToken) error { return boom }

	if _, err := st.IssueToken(ctx, "node-a"); !errors.Is(err, boom) {
		t.Fatalf("IssueToken err = %v, want injected failure", err)
	}
	st.tokensMu.RLock()
	n := len(st.tokenIndex)
	st.tokensMu.RUnlock()
	if n != 0 {
		t.Fatalf("index has %d entries after failed issue, want 0", n)
	}
	// Recovers once writes work again (nothing was persisted).
	st.putTokenFn = nil
	if _, err := st.IssueToken(ctx, "node-a"); err != nil {
		t.Fatalf("IssueToken after recovery: %v", err)
	}
}

func TestRevokeToken_WriteFailureRestoresIndex(t *testing.T) {
	st := newConcurrencyStore(t)
	ctx := context.Background()
	tok, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("injected write failure")
	st.putTokenFn = func(context.Context, AgentToken) error { return boom }

	if err := st.RevokeToken(ctx, "node-a"); !errors.Is(err, boom) {
		t.Fatalf("RevokeToken err = %v, want injected failure", err)
	}
	// Record on disk is still active, so the token must still authenticate.
	if id, ok, err := st.LookupNodeID(ctx, tok); err != nil || !ok || id != "node-a" {
		t.Fatalf("after failed revoke lookup = (%q, %v, %v), want (node-a, true, nil)", id, ok, err)
	}
	st.putTokenFn = nil
	if err := st.RevokeToken(ctx, "node-a"); err != nil {
		t.Fatalf("RevokeToken after recovery: %v", err)
	}
	if _, ok, _ := st.LookupNodeID(ctx, tok); ok {
		t.Fatal("token still valid after successful revoke")
	}
}

// Concurrent IssueToken for one node_id: exactly one wins, the rest get
// ErrTokenExists, and the winner's token is the one that authenticates (#323).
func TestIssueToken_ConcurrentSameNodeSingleWinner(t *testing.T) {
	st := newConcurrencyStore(t)
	ctx := context.Background()
	const n = 16

	type res struct {
		tok string
		err error
	}
	results := make([]res, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i].tok, results[i].err = st.IssueToken(ctx, "node-x")
		}(i)
	}
	close(start)
	wg.Wait()

	var winner string
	wins := 0
	for _, r := range results {
		switch {
		case r.err == nil:
			wins++
			winner = r.tok
		case !errors.Is(r.err, ErrTokenExists):
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if wins != 1 {
		t.Fatalf("successful issues = %d, want 1", wins)
	}
	if id, ok, err := st.LookupNodeID(ctx, winner); err != nil || !ok || id != "node-x" {
		t.Fatalf("winner token lookup = (%q, %v, %v)", id, ok, err)
	}
}

// Reissue after revoke overwrites the revoked record (same key); the revoke
// history is not kept. Pinned so the IssueToken doc comment stays accurate.
func TestIssueToken_ReissueOverwritesRevokedRecord(t *testing.T) {
	st := newConcurrencyStore(t)
	ctx := context.Background()
	if _, err := st.IssueToken(ctx, "node-y"); err != nil {
		t.Fatal(err)
	}
	if err := st.RevokeToken(ctx, "node-y"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.IssueToken(ctx, "node-y"); err != nil {
		t.Fatal(err)
	}
	recs, err := st.ListTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].RevokedAt != nil {
		t.Fatalf("records = %+v, want a single active record", recs)
	}
}

// A slow token write must not stall LookupNodeID: the index lock is held only
// for index updates, not across Badger I/O (#243).
func TestLookupNodeID_NotBlockedBySlowTokenWrite(t *testing.T) {
	st := newConcurrencyStore(t)
	ctx := context.Background()
	tok, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	st.putTokenFn = func(context.Context, AgentToken) error {
		close(entered)
		<-release
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- st.RevokeToken(ctx, "node-a") }()
	<-entered

	looked := make(chan struct{})
	go func() {
		st.LookupNodeID(ctx, tok)
		close(looked)
	}()
	select {
	case <-looked:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("LookupNodeID blocked while a token write was in flight")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if _, ok, _ := st.LookupNodeID(ctx, tok); ok {
		t.Fatal("token still valid after revoke")
	}
}
