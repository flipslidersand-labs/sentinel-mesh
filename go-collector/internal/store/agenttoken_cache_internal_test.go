package store

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

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

// cancelOnSecondDone is a context whose Done channel is open on the first
// Done() call and closed from the second on, so a RevokeToken's read
// (getAgentToken) sees a live context and its write sees a cancelled one.
type cancelOnSecondDone struct {
	context.Context
	mu     sync.Mutex
	calls  int
	closed chan struct{}
}

func newCancelOnSecondDone() *cancelOnSecondDone {
	c := make(chan struct{})
	close(c)
	return &cancelOnSecondDone{Context: context.Background(), closed: c}
}

func (c *cancelOnSecondDone) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls >= 2 {
		return c.closed
	}
	return nil
}

func (c *cancelOnSecondDone) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls >= 2 {
		return context.Canceled
	}
	return nil
}

// A revoke whose context is cancelled mid-write must never leave a token
// that is revoked on disk still accepted by the index (fail-open).
func TestRevokeToken_CancelDuringWriteDoesNotFailOpen(t *testing.T) {
	bg := context.Background()
	for i := 0; i < 20; i++ {
		st := openTempStore(t, t.TempDir())
		token, err := st.IssueToken(bg, "node-a")
		if err != nil {
			t.Fatalf("IssueToken: %v", err)
		}
		_ = st.RevokeToken(newCancelOnSecondDone(), "node-a")

		// Let any write that outlived the cancelled call land.
		deadline := time.Now().Add(time.Second)
		var rec *AgentToken
		for {
			if rec, err = st.getAgentToken(bg, "node-a"); err != nil {
				t.Fatalf("getAgentToken: %v", err)
			}
			if rec.RevokedAt != nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		_, ok, _ := st.LookupNodeID(bg, token)
		if rec.RevokedAt != nil && ok {
			t.Fatalf("iteration %d: token revoked on disk but still accepted", i)
		}
	}
}

func TestLookupNodeID_WrongLengthRejected(t *testing.T) {
	st := openTempStore(t, t.TempDir())
	if _, ok, err := st.LookupNodeID(context.Background(), strings.Repeat("a", 1<<20)); ok || err != nil {
		t.Fatalf("oversized token: ok=%v err=%v", ok, err)
	}
}

// A record that can't be decoded at startup must not break authentication
// for every other token (#242). Only the broken record is skipped; a warning
// carries the record key and count, never token values or hashes.
func TestLoadTokenIndex_SkipsUndecodableRecords(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	st, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	good, err := st.IssueToken(ctx, "node-good")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	revoked, err := st.IssueToken(ctx, "node-revoked")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if err := st.RevokeToken(ctx, "node-revoked"); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	putRaw(t, st, "agenttoken:node-broken", `{"node_id":"node-broken","token_hash":"deadbeefcafe`)
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	st = openTempStore(t, dir)

	nodeID, ok, err := st.LookupNodeID(ctx, good)
	if err != nil || !ok || nodeID != "node-good" {
		t.Fatalf("valid token = (%q, %v, %v), want (node-good, true, nil)", nodeID, ok, err)
	}
	if _, ok, err := st.LookupNodeID(ctx, revoked); err != nil || ok {
		t.Fatalf("revoked token = (%v, %v), want (false, nil)", ok, err)
	}
	if _, ok, err := st.LookupNodeID(ctx, strings.Repeat("a", TokenLength)); err != nil || ok {
		t.Fatalf("unknown token = (%v, %v), want (false, nil)", ok, err)
	}

	logs := buf.String()
	if !strings.Contains(logs, "agenttoken:node-broken") || !strings.Contains(logs, "count=1") {
		t.Errorf("warning should name the broken key and count, got: %q", logs)
	}
	if strings.Contains(logs, "deadbeefcafe") || strings.Contains(logs, good) {
		t.Errorf("warning must not leak hashes or tokens: %q", logs)
	}
}

// truncatedRealRecord issues a token for nodeID, then overwrites its stored
// record with the first half of the real JSON (a write cut short mid-record).
// Unlike a hand-made fake, the cut-off record belongs to a token that was
// genuinely issued, so it pins that this token is rejected after restart.
func truncatedRealRecord(t *testing.T, st *Store, nodeID string) string {
	t.Helper()
	token, err := st.IssueToken(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	var raw []byte
	if err := st.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(agentTokenKey(nodeID))
		if err != nil {
			return err
		}
		raw, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		t.Fatalf("read record: %v", err)
	}
	putRaw(t, st, string(agentTokenKey(nodeID)), string(raw[:len(raw)/2]))
	return token
}

// A genuinely truncated record (not a fake hash) must leave its own token
// rejected after restart while other tokens keep working (#276).
func TestLoadTokenIndex_TruncatedRealRecordRejectsItsToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	good, err := st.IssueToken(ctx, "node-good")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	cut := truncatedRealRecord(t, st, "node-cut")
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st = openTempStore(t, dir)
	if _, ok, err := st.LookupNodeID(ctx, cut); err != nil || ok {
		t.Fatalf("token of truncated record = (%v, %v), want (false, nil)", ok, err)
	}
	if id, ok, err := st.LookupNodeID(ctx, good); err != nil || !ok || id != "node-good" {
		t.Fatalf("other token = (%q, %v, %v), want (node-good, true, nil)", id, ok, err)
	}
	// A damaged record cannot be fixed by revoke: it fails to decode too.
	if err := st.RevokeToken(ctx, "node-cut"); err == nil {
		t.Fatal("RevokeToken on a damaged record should fail to decode")
	}
}

// Iteration/IO errors at startup must fail closed: LookupNodeID returns the
// error rather than (false, nil) or a partial index. Injecting an iterator
// error into badger is not practical, so this sets the state loadTokenIndex
// records on such an error (tokenIndexErr set, index emptied) and checks
// that lookups surface it even for a token that was valid.
func TestLookupNodeID_IndexLoadErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	st := openTempStore(t, t.TempDir())
	token, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	loadErr := fmt.Errorf("injected io error")
	st.tokensMu.Lock()
	st.tokenIndex = map[string]string{}
	st.tokenIndexErr = loadErr
	st.tokensMu.Unlock()

	if id, ok, err := st.LookupNodeID(ctx, token); err == nil || ok || id != "" {
		t.Fatalf("LookupNodeID = (%q, %v, %v), want (\"\", false, error)", id, ok, err)
	}
}

// ListTokens skips undecodable records (like the startup index load) so
// `token list` stays usable, warning with the key and count only.
func TestListTokens_SkipsUndecodableRecords(t *testing.T) {
	ctx := context.Background()
	st := openTempStore(t, t.TempDir())
	good, err := st.IssueToken(ctx, "node-good")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	cut := truncatedRealRecord(t, st, "node-cut")

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	tokens, err := st.ListTokens(ctx)
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	if len(tokens) != 1 || tokens[0].NodeID != "node-good" {
		t.Fatalf("tokens = %+v, want only node-good", tokens)
	}
	logs := buf.String()
	if !strings.Contains(logs, "agenttoken:node-cut") || !strings.Contains(logs, "count=1") {
		t.Errorf("warning should name the key and count, got: %q", logs)
	}
	if strings.Contains(logs, good) || strings.Contains(logs, cut) || strings.Contains(logs, hashToken(good)) {
		t.Errorf("warning must not leak tokens or hashes: %q", logs)
	}
}
