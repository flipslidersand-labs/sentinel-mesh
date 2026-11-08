package store

import (
	"context"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func openTempStore(t testing.TB, dir string) *Store {
	t.Helper()
	st, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	return st
}

// putRaw writes a raw value under key, bypassing IssueToken/RevokeToken.
func putRaw(t testing.TB, st *Store, key, val string) {
	t.Helper()
	if err := st.db.Update(func(tx *badger.Txn) error {
		return tx.Set([]byte(key), []byte(val))
	}); err != nil {
		t.Fatalf("raw put: %v", err)
	}
}

// LookupNodeID must not scan/decode every token record (#232): an
// undecodable record written after startup would make a full scan fail, so
// a successful lookup proves the lookup is index-based.
func TestLookupNodeID_DoesNotScanAllRecords(t *testing.T) {
	ctx := context.Background()
	st := openTempStore(t, t.TempDir())
	token, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	putRaw(t, st, "agenttoken:zz-corrupt", "{not json")

	nodeID, ok, err := st.LookupNodeID(ctx, token)
	if err != nil || !ok || nodeID != "node-a" {
		t.Fatalf("LookupNodeID = (%q, %v, %v), want (node-a, true, nil)", nodeID, ok, err)
	}
}

func TestLookupNodeID_RevokeIsReflectedImmediately(t *testing.T) {
	ctx := context.Background()
	st := openTempStore(t, t.TempDir())
	token, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if _, ok, _ := st.LookupNodeID(ctx, token); !ok {
		t.Fatal("freshly issued token should resolve")
	}
	if err := st.RevokeToken(ctx, "node-a"); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if _, ok, err := st.LookupNodeID(ctx, token); ok || err != nil {
		t.Fatalf("revoked token: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	second, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("reissue: %v", err)
	}
	if id, ok, _ := st.LookupNodeID(ctx, second); !ok || id != "node-a" {
		t.Fatalf("reissued token = (%q, %v), want (node-a, true)", id, ok)
	}
	if _, ok, _ := st.LookupNodeID(ctx, token); ok {
		t.Fatal("old token must stay rejected after reissue")
	}
}

// The index is rebuilt from disk on open: active tokens survive a restart,
// revoked ones stay rejected.
func TestLookupNodeID_IndexRebuiltOnReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	active, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken a: %v", err)
	}
	revoked, err := st.IssueToken(ctx, "node-b")
	if err != nil {
		t.Fatalf("IssueToken b: %v", err)
	}
	if err := st.RevokeToken(ctx, "node-b"); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2 := openTempStore(t, dir)
	if id, ok, err := st2.LookupNodeID(ctx, active); err != nil || !ok || id != "node-a" {
		t.Fatalf("active after reopen = (%q, %v, %v)", id, ok, err)
	}
	if _, ok, err := st2.LookupNodeID(ctx, revoked); ok || err != nil {
		t.Fatalf("revoked after reopen: ok=%v err=%v", ok, err)
	}
}

// A token record that can't be decoded at startup must fail auth closed
// (error, not silently "unknown token"), matching the old full-scan
// behaviour — while other store users (e.g. `token list`) can still open.
func TestLookupNodeID_CorruptRecordAtOpenFailsClosed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	token, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	putRaw(t, st, "agenttoken:zz-corrupt", "{not json")
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2 := openTempStore(t, dir)
	if _, ok, err := st2.LookupNodeID(ctx, token); err == nil || ok {
		t.Fatalf("LookupNodeID with corrupt index: ok=%v err=%v, want error", ok, err)
	}
}

func BenchmarkLookupNodeID_10kTokens(b *testing.B) {
	ctx := context.Background()
	st := openTempStore(b, b.TempDir())
	var token string
	for i := 0; i < 10000; i++ {
		tok, err := st.IssueToken(ctx, fmt.Sprintf("node-%05d", i))
		if err != nil {
			b.Fatalf("IssueToken: %v", err)
		}
		token = tok
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := st.LookupNodeID(ctx, token); !ok || err != nil {
			b.Fatalf("lookup failed: ok=%v err=%v", ok, err)
		}
	}
}
