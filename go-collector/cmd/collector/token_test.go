package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func runTokenCmd(t *testing.T, dataDir string, stdin string, args ...string) (stdout string, err error) {
	t.Helper()
	cmd := tokenCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if stdin != "" {
		cmd.SetIn(strings.NewReader(stdin))
	}
	cmd.SetArgs(append([]string{"--data-dir", dataDir}, args...))
	err = cmd.Execute()
	return out.String(), err
}

func TestTokenIssue_PrintsTokenOnce(t *testing.T) {
	dir := t.TempDir()
	out, err := runTokenCmd(t, dir, "", "issue", "node-a")
	if err != nil {
		t.Fatalf("token issue: %v", err)
	}
	token := strings.TrimSpace(out)
	if token == "" {
		t.Fatal("expected a non-empty token on stdout")
	}
}

func TestTokenIssue_RejectsEmptyNodeID(t *testing.T) {
	dir := t.TempDir()
	if _, err := runTokenCmd(t, dir, "", "issue", "  "); err == nil {
		t.Fatal("expected error for whitespace-only node_id")
	}
}

func TestTokenIssue_RejectsDuplicateActiveToken(t *testing.T) {
	dir := t.TempDir()
	if _, err := runTokenCmd(t, dir, "", "issue", "node-a"); err != nil {
		t.Fatalf("first issue: %v", err)
	}
	if _, err := runTokenCmd(t, dir, "", "issue", "node-a"); err == nil {
		t.Fatal("expected error reissuing over an active token")
	}
}

func TestTokenRevoke_RequiresConfirmation(t *testing.T) {
	dir := t.TempDir()
	if _, err := runTokenCmd(t, dir, "", "issue", "node-a"); err != nil {
		t.Fatalf("issue: %v", err)
	}

	// Declining the prompt must not revoke.
	out, err := runTokenCmd(t, dir, "n\n", "revoke", "node-a")
	if err != nil {
		t.Fatalf("revoke (declined): %v", err)
	}
	if !strings.Contains(out, "aborted") {
		t.Errorf("expected 'aborted' in output, got %q", out)
	}
	listOut, err := runTokenCmd(t, dir, "", "list")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if strings.Contains(listOut, "revoked") {
		t.Error("token should still be active after declining the revoke prompt")
	}
}

func TestTokenRevoke_YesFlagSkipsPrompt(t *testing.T) {
	dir := t.TempDir()
	if _, err := runTokenCmd(t, dir, "", "issue", "node-a"); err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := runTokenCmd(t, dir, "", "revoke", "node-a", "--yes"); err != nil {
		t.Fatalf("revoke --yes: %v", err)
	}

	listOut, err := runTokenCmd(t, dir, "", "list")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !strings.Contains(listOut, "revoked") {
		t.Errorf("expected revoked status in list output, got %q", listOut)
	}
}

func TestTokenList_DistinguishesActiveAndRevoked(t *testing.T) {
	dir := t.TempDir()
	if _, err := runTokenCmd(t, dir, "", "issue", "node-a"); err != nil {
		t.Fatalf("issue node-a: %v", err)
	}
	if _, err := runTokenCmd(t, dir, "", "issue", "node-b"); err != nil {
		t.Fatalf("issue node-b: %v", err)
	}
	if _, err := runTokenCmd(t, dir, "", "revoke", "node-b", "--yes"); err != nil {
		t.Fatalf("revoke node-b: %v", err)
	}

	out, err := runTokenCmd(t, dir, "", "list")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 { // header + 2 rows
		t.Fatalf("expected 3 lines (header + 2 tokens), got %d: %q", len(lines), out)
	}
	if !strings.Contains(out, "node-a") || !strings.Contains(out, "node-b") {
		t.Errorf("expected both node_ids in output, got %q", out)
	}
}

func TestTokenIssue_OpenStoreErrorIsWrapped(t *testing.T) {
	// A file (not a directory) at dataDir makes badger.Open fail — the
	// wrapping in openTokenStore should surface a hint about the collector
	// possibly still holding the lock, not badger's raw error alone.
	dir := t.TempDir()
	filePath := dir + "/not-a-dir"
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("os.WriteFile: %v", err)
	}

	_, err := runTokenCmd(t, filePath, "", "issue", "node-a")
	if err == nil {
		t.Fatal("expected error opening store at a non-directory path")
	}
	if !strings.Contains(err.Error(), "stop it first") {
		t.Errorf("expected hint about stopping the collector, got: %v", err)
	}
}
