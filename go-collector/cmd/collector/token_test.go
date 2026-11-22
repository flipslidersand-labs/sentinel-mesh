package main

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
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

// Subcommands run without the parent "token" command lack the persistent
// --data-dir flag; the lookup error must surface instead of yielding "".
func TestTokenSubcommands_UnknownFlagLookupErrors(t *testing.T) {
	cases := map[string][]string{
		"issue":  {"node-a"},
		"revoke": {"--yes", "node-a"},
		"list":   {},
	}
	ctors := map[string]func() *cobra.Command{
		"issue": tokenIssueCmd, "revoke": tokenRevokeCmd, "list": tokenListCmd,
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			cmd := ctors[name]()
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs(args)
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "flag accessed but not defined: "+flagDataDir) {
				t.Fatalf("expected flag lookup error for %q, got %v", flagDataDir, err)
			}
		})
	}
}

func TestTokenRevoke_MissingYesFlagErrors(t *testing.T) {
	cmd := tokenRevokeCmd()
	cmd.ResetFlags() // drop --yes to simulate a typo'd/missing name
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"node-a"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "flag accessed but not defined: "+flagYes) {
		t.Fatalf("expected flag lookup error for %q, got %v", flagYes, err)
	}
}

func TestTokenIssue_RejectsInvalidNodeID(t *testing.T) {
	cases := map[string]string{
		"non-ascii": "nodo-\u00e9",
		"too-long":  strings.Repeat("a", 257),
	}
	for name, id := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := runTokenCmd(t, t.TempDir(), "", "issue", id)
			if err == nil {
				t.Fatalf("expected error for node_id %q, got output %q", id, out)
			}
			if strings.TrimSpace(out) != "" && !strings.Contains(out, "node_id") {
				t.Fatalf("unexpected output: %q", out)
			}
		})
	}
}

func TestTokenIssue_AcceptsMaxLengthNodeID(t *testing.T) {
	id := strings.Repeat("a", 256)
	if _, err := runTokenCmd(t, t.TempDir(), "", "issue", id); err != nil {
		t.Fatalf("token issue: %v", err)
	}
}

// putBrokenTokenRecord writes an undecodable token record under key straight
// into Badger (the store is closed between CLI invocations, so this is safe).
func putBrokenTokenRecord(t *testing.T, dataDir, key string) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dataDir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	defer db.Close() //nolint:errcheck
	if err := db.Update(func(tx *badger.Txn) error {
		return tx.Set([]byte(key), []byte(`{"node_id":"node-broken","token_hash":"trunc`))
	}); err != nil {
		t.Fatalf("raw put: %v", err)
	}
}

// A record that cannot be decoded can't be revoked by node_id (#276); --force
// removes it by its record key, and only that record.
func TestTokenRevokeForce_RemovesBrokenRecord(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"node-a", "node-b"} {
		if _, err := runTokenCmd(t, dir, "", "issue", n); err != nil {
			t.Fatalf("issue %s: %v", n, err)
		}
	}
	const key = "agenttoken:node-broken"
	putBrokenTokenRecord(t, dir, key)

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// The list warning names the key to pass to --force.
	if _, err := runTokenCmd(t, dir, "", "list"); err != nil {
		t.Fatalf("list: %v", err)
	}
	if !strings.Contains(buf.String(), key) {
		t.Fatalf("list warning should contain %q, got %q", key, buf.String())
	}

	// Without --force the node_id path still fails on the broken record.
	if _, err := runTokenCmd(t, dir, "", "revoke", "node-broken", "--yes"); err == nil {
		t.Fatal("plain revoke of a broken record should fail")
	}
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "--force", key); err != nil {
		t.Fatalf("revoke --force: %v", err)
	}

	buf.Reset()
	out, err := runTokenCmd(t, dir, "", "list")
	if err != nil {
		t.Fatalf("list after force: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("list should no longer warn, got %q", buf.String())
	}
	if !strings.Contains(out, "node-a") || !strings.Contains(out, "node-b") {
		t.Errorf("healthy records must remain, got %q", out)
	}
}

func TestTokenRevokeForce_UnknownKeyErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := runTokenCmd(t, dir, "", "issue", "node-a"); err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "--force", "agenttoken:nope"); err == nil {
		t.Fatal("expected error for a record key that does not exist")
	}
	// Keys outside the token namespace are refused, never deleted.
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "--force", "event:whatever"); err == nil {
		t.Fatal("expected error for a key outside the agenttoken: namespace")
	}
	if out, _ := runTokenCmd(t, dir, "", "list"); !strings.Contains(out, "node-a") {
		t.Errorf("node-a must remain, got %q", out)
	}
}

// Without --force, a node_id that has no record is still a no-op (unchanged).
func TestTokenRevoke_WithoutForceUnknownNodeIsNoop(t *testing.T) {
	if _, err := runTokenCmd(t, t.TempDir(), "", "revoke", "--yes", "ghost"); err != nil {
		t.Fatalf("revoke unknown node_id: %v", err)
	}
}
