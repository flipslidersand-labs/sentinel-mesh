package store_test

import (
	"context"
	"testing"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func TestIssueToken_ThenLookupResolvesNodeID(t *testing.T) {
	st := newTempStore(t)

	token, err := st.IssueToken(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if token == "" {
		t.Fatal("IssueToken returned empty token")
	}

	nodeID, ok, err := st.LookupNodeID(context.Background(), token)
	if err != nil {
		t.Fatalf("LookupNodeID: %v", err)
	}
	if !ok || nodeID != "node-a" {
		t.Errorf("LookupNodeID = (%q, %v), want (node-a, true)", nodeID, ok)
	}
}

func TestIssueToken_DuplicateActiveTokenRejected(t *testing.T) {
	st := newTempStore(t)

	if _, err := st.IssueToken(context.Background(), "node-a"); err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if _, err := st.IssueToken(context.Background(), "node-a"); err != store.ErrTokenExists {
		t.Errorf("second IssueToken error = %v, want ErrTokenExists", err)
	}
}

func TestIssueToken_ReissueAllowedAfterRevoke(t *testing.T) {
	st := newTempStore(t)
	ctx := context.Background()

	first, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if err := st.RevokeToken(ctx, "node-a"); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	second, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken after revoke: %v", err)
	}
	if second == first {
		t.Error("reissued token should differ from the revoked one")
	}

	if _, ok, err := st.LookupNodeID(ctx, first); err != nil {
		t.Fatalf("LookupNodeID(first): %v", err)
	} else if ok {
		t.Error("revoked token should no longer resolve")
	}

	nodeID, ok, err := st.LookupNodeID(ctx, second)
	if err != nil {
		t.Fatalf("LookupNodeID(second): %v", err)
	}
	if !ok || nodeID != "node-a" {
		t.Errorf("LookupNodeID(second) = (%q, %v), want (node-a, true)", nodeID, ok)
	}
}

func TestRevokeToken_UnknownNodeIsNoop(t *testing.T) {
	st := newTempStore(t)
	if err := st.RevokeToken(context.Background(), "node-x"); err != nil {
		t.Errorf("RevokeToken(unknown) = %v, want nil", err)
	}
}

func TestRevokeToken_AlreadyRevokedIsNoop(t *testing.T) {
	st := newTempStore(t)
	ctx := context.Background()
	if _, err := st.IssueToken(ctx, "node-a"); err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if err := st.RevokeToken(ctx, "node-a"); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if err := st.RevokeToken(ctx, "node-a"); err != nil {
		t.Errorf("second RevokeToken = %v, want nil (no-op)", err)
	}
}

func TestLookupNodeID_UnknownTokenNotFound(t *testing.T) {
	st := newTempStore(t)
	if _, err := st.IssueToken(context.Background(), "node-a"); err != nil {
		t.Fatalf("IssueToken: %v", err)
	}

	_, ok, err := st.LookupNodeID(context.Background(), "not-a-real-token")
	if err != nil {
		t.Fatalf("LookupNodeID: %v", err)
	}
	if ok {
		t.Error("LookupNodeID should not resolve an unknown token")
	}
}

func TestListTokens_ReflectsIssueAndRevoke(t *testing.T) {
	st := newTempStore(t)
	ctx := context.Background()
	if _, err := st.IssueToken(ctx, "node-a"); err != nil {
		t.Fatalf("IssueToken(node-a): %v", err)
	}
	if _, err := st.IssueToken(ctx, "node-b"); err != nil {
		t.Fatalf("IssueToken(node-b): %v", err)
	}
	if err := st.RevokeToken(ctx, "node-b"); err != nil {
		t.Fatalf("RevokeToken(node-b): %v", err)
	}

	tokens, err := st.ListTokens(ctx)
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	if len(tokens) != 2 {
		t.Fatalf("want 2 tokens, got %d", len(tokens))
	}
	byNode := make(map[string]store.AgentToken, len(tokens))
	for _, tok := range tokens {
		byNode[tok.NodeID] = tok
	}
	if byNode["node-a"].RevokedAt != nil {
		t.Error("node-a should not be revoked")
	}
	if byNode["node-b"].RevokedAt == nil {
		t.Error("node-b should be revoked")
	}
}

func TestIssueToken_PlaintextNeverStored(t *testing.T) {
	st := newTempStore(t)
	ctx := context.Background()
	token, err := st.IssueToken(ctx, "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}

	tokens, err := st.ListTokens(ctx)
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	for _, rec := range tokens {
		if rec.TokenHash == token {
			t.Fatal("stored TokenHash equals the plaintext token")
		}
	}
}
