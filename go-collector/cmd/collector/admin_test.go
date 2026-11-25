package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// startAdmin opens the store at dataDir (taking the Badger lock, like serve)
// and serves the admin socket on it until the test ends.
func startAdmin(t *testing.T, dataDir string) *store.Store {
	t.Helper()
	return startAdminLogged(t, dataDir, zap.NewNop())
}

// startAdminLogged is startAdmin with the serve-side logger of the test's choice.
func startAdminLogged(t *testing.T, dataDir string, logger *zap.Logger) *store.Store {
	t.Helper()
	st, err := store.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); serveAdmin(ctx, adminSocketPath(dataDir), st, logger) }()
	t.Cleanup(func() { cancel(); <-done; st.Close() }) //nolint:errcheck
	deadline := time.Now().Add(5 * time.Second)
	for {
		if c, err := net.Dial("unix", adminSocketPath(dataDir)); err == nil {
			c.Close()
			return st
		}
		if time.Now().After(deadline) {
			t.Fatal("admin socket did not come up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServeAdmin_SocketAndDirModes(t *testing.T) {
	dir := shortTempDir(t)
	startAdmin(t, dir)
	di, err := os.Lstat(adminDir(dir))
	if err != nil || di.Mode().Perm() != 0o700 || !di.IsDir() {
		t.Fatalf("admin dir: %v, %v", di, err)
	}
	si, err := os.Lstat(adminSocketPath(dir))
	if err != nil || si.Mode().Perm() != 0o600 || si.Mode().Type() != os.ModeSocket {
		t.Fatalf("admin socket: %v, %v", si, err)
	}
}

func TestServeAdmin_SocketRemovedOnShutdown(t *testing.T) {
	dir := shortTempDir(t)
	st, _ := store.New(dir)
	defer st.Close() //nolint:errcheck
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); serveAdmin(ctx, adminSocketPath(dir), st, zap.NewNop()) }()
	for i := 0; i < 500; i++ {
		if _, err := os.Lstat(adminSocketPath(dir)); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveAdmin did not stop")
	}
	if _, err := os.Lstat(adminSocketPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("socket still present after shutdown: %v", err)
	}
}

func TestCheckAdminDir(t *testing.T) {
	euid := os.Geteuid()
	base := shortTempDir(t)
	mk := func(name string, mode os.FileMode) string {
		p := filepath.Join(base, name)
		if err := os.Mkdir(p, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := mk("good", 0o700)
	if err := checkAdminDir(good, euid); err != nil {
		t.Fatalf("good dir rejected: %v", err)
	}
	if err := checkAdminDir(mk("open", 0o755), euid); err == nil {
		t.Error("0755 dir accepted")
	}
	if err := checkAdminDir(mk("loose", 0o770), euid); err == nil {
		t.Error("0770 dir accepted")
	}
	if err := checkAdminDir(good, euid+1); err == nil {
		t.Error("dir owned by another uid accepted")
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(good, link); err != nil {
		t.Fatal(err)
	}
	if err := checkAdminDir(link, euid); err == nil {
		t.Error("symlink to a 0700 dir accepted")
	}
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := checkAdminDir(file, euid); err == nil {
		t.Error("regular file accepted")
	}
	if err := checkAdminDir(filepath.Join(base, "missing"), euid); !os.IsNotExist(err) {
		t.Errorf("missing dir: err = %v, want not-exist", err)
	}
}

// serve must not open a socket in an unsafe admin dir, and must not create one.
func TestServeAdmin_RefusesUnsafeDir(t *testing.T) {
	for name, setup := range map[string]func(dir string){
		"mode 0755": func(dir string) { _ = os.Mkdir(adminDir(dir), 0o755); _ = os.Chmod(adminDir(dir), 0o755) },
		"symlink": func(dir string) {
			real := filepath.Join(dir, "real")
			_ = os.Mkdir(real, 0o700)
			_ = os.Symlink(real, adminDir(dir))
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := shortTempDir(t)
			setup(dir)
			st, _ := store.New(dir)
			defer st.Close() //nolint:errcheck
			done := make(chan struct{})
			go func() { defer close(done); serveAdmin(context.Background(), adminSocketPath(dir), st, zap.NewNop()) }()
			select {
			case <-done: // returned (warned) without blocking serve
			case <-time.After(5 * time.Second):
				t.Fatal("serveAdmin kept running in an unsafe directory")
			}
			if _, err := os.Lstat(adminSocketPath(dir)); err == nil {
				t.Fatal("socket created in an unsafe directory")
			}
		})
	}
}

func TestServeAdmin_StaleSocketReplacedOtherFileKept(t *testing.T) {
	dir := shortTempDir(t)
	if err := os.Mkdir(adminDir(dir), 0o700); err != nil {
		t.Fatal(err)
	}
	// A stale socket left by a crashed serve.
	ln, err := net.Listen("unix", adminSocketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	if _, err := os.Lstat(adminSocketPath(dir)); err != nil {
		t.Fatal("stale socket setup failed")
	}
	startAdmin(t, dir) // would fail to listen if the stale socket stayed

	// A non-socket file at the path is never deleted.
	dir2 := shortTempDir(t)
	if err := os.Mkdir(adminDir(dir2), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(adminSocketPath(dir2), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, _ := store.New(dir2)
	defer st.Close() //nolint:errcheck
	serveAdmin(context.Background(), adminSocketPath(dir2), st, zap.NewNop())
	if b, err := os.ReadFile(adminSocketPath(dir2)); err != nil || string(b) != "keep" {
		t.Fatalf("non-socket file was touched: %q, %v", b, err)
	}
}

// adminSockPathFits must match where net.Listen really starts to fail.
func TestAdminSockPathFits_MatchesListenBoundary(t *testing.T) {
	base := shortTempDir(t)
	for _, n := range []int{len(base) + 10, 100, 107, 108, 120} {
		if n <= len(base)+1 {
			continue
		}
		p := filepath.Join(base, strings.Repeat("s", n-len(base)-1))
		if len(p) != n {
			t.Fatalf("path length %d, want %d", len(p), n)
		}
		ln, err := net.Listen("unix", p)
		if err == nil {
			ln.Close()
		}
		if (err == nil) != adminSockPathFits(p) {
			t.Errorf("len %d: Listen ok=%v but adminSockPathFits=%v", n, err == nil, adminSockPathFits(p))
		}
	}
}

func TestServeAdmin_TooLongPathWarnsAndReturns(t *testing.T) {
	st, _ := store.New(shortTempDir(t))
	defer st.Close() //nolint:errcheck
	long := filepath.Join(shortTempDir(t), strings.Repeat("d", 120), adminSockName)
	done := make(chan struct{})
	go func() { defer close(done); serveAdmin(context.Background(), long, st, zap.NewNop()) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveAdmin blocked on an unusable path")
	}
}

// Delegated results must match the direct path exactly (output and error).
func TestTokenRevoke_DelegatedMatchesDirect(t *testing.T) {
	prep := func(dir string, issue, revokeFirst bool) {
		if issue {
			if _, err := runTokenCmd(t, dir, "", "issue", "n1"); err != nil {
				t.Fatal(err)
			}
		}
		if revokeFirst {
			if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "n1"); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, c := range map[string]struct{ issue, revoked bool }{
		"never issued":    {false, false},
		"already revoked": {true, true},
	} {
		t.Run(name, func(t *testing.T) {
			direct := shortTempDir(t)
			prep(direct, c.issue, c.revoked)
			dOut, dErr := runTokenCmd(t, direct, "", "revoke", "--yes", "n1")

			live := shortTempDir(t)
			prep(live, c.issue, c.revoked)
			startAdmin(t, live)
			lOut, lErr := runTokenCmd(t, live, "", "revoke", "--yes", "n1")

			if (dErr == nil) != (lErr == nil) || dOut != lOut || lErr != nil {
				t.Fatalf("direct (%q, %v) != delegated (%q, %v)", dOut, dErr, lOut, lErr)
			}
		})
	}
}

func TestTokenRevoke_DelegatedRevokesAndReports(t *testing.T) {
	dir := shortTempDir(t)
	if _, err := runTokenCmd(t, dir, "", "issue", "n1"); err != nil {
		t.Fatal(err)
	}
	st := startAdmin(t, dir)
	out, err := runTokenCmd(t, dir, "", "revoke", "--yes", "n1")
	if err != nil || !strings.Contains(out, "running collector") {
		t.Fatalf("out=%q err=%v", out, err)
	}
	toks, err := st.ListTokens(context.Background())
	if err != nil || len(toks) != 1 || toks[0].RevokedAt == nil {
		t.Fatalf("token not revoked in the serving store: %+v, %v", toks, err)
	}
}

func TestTokenRevoke_NoSocketUsesDirectPath(t *testing.T) {
	dir := shortTempDir(t)
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "ghost"); err != nil {
		t.Fatalf("no admin dir: %v", err)
	}
	// admin dir present (valid) but nothing listening, and a stale socket file.
	if err := os.Mkdir(adminDir(dir), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "ghost"); err != nil {
		t.Fatalf("empty admin dir: %v", err)
	}
	ln, err := net.Listen("unix", adminSocketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "ghost"); err != nil {
		t.Fatalf("stale socket: %v", err)
	}
}

// An unsafe admin dir is an error: no silent fallback to the direct path.
func TestTokenRevoke_UnsafeAdminDirIsError(t *testing.T) {
	dir := shortTempDir(t)
	if err := os.Mkdir(adminDir(dir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(adminDir(dir), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := runTokenCmd(t, dir, "", "revoke", "--yes", "ghost")
	if err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("err = %v, want an unsafe-directory error", err)
	}
}

// --force is never delegated: against a running serve it stays a lock error.
func TestTokenRevokeForce_NotDelegated(t *testing.T) {
	dir := shortTempDir(t)
	startAdmin(t, dir)
	_, err := runTokenCmd(t, dir, "", "revoke", "--yes", "--force", "agenttoken:n1")
	if err == nil || !strings.Contains(err.Error(), "open store") {
		t.Fatalf("err = %v, want the store-lock error", err)
	}
}

var tokenLine = regexp.MustCompile(`^[0-9a-f]{64}\n$`)

// Delegated issue must print the same shape and fail the same way as the
// direct path (the token itself is random, so compare format, not value).
func TestTokenIssue_DelegatedMatchesDirect(t *testing.T) {
	direct := shortTempDir(t)
	dOut, dErr := runTokenCmd(t, direct, "", "issue", "n1")
	_, dDupErr := runTokenCmd(t, direct, "", "issue", "n1")
	_, dBadErr := runTokenCmd(t, direct, "", "issue", "bad\x01id")
	_, dEmptyErr := runTokenCmd(t, direct, "", "issue", "  ")

	live := shortTempDir(t)
	st := startAdmin(t, live)
	lOut, lErr := runTokenCmd(t, live, "", "issue", "n1")
	_, lDupErr := runTokenCmd(t, live, "", "issue", "n1")
	_, lBadErr := runTokenCmd(t, live, "", "issue", "bad\x01id")
	_, lEmptyErr := runTokenCmd(t, live, "", "issue", "  ")

	if dErr != nil || lErr != nil || !tokenLine.MatchString(dOut) || !tokenLine.MatchString(lOut) {
		t.Fatalf("direct (%q, %v) vs delegated (%q, %v)", dOut, dErr, lOut, lErr)
	}
	for name, pair := range map[string][2]error{
		"duplicate": {dDupErr, lDupErr}, "invalid": {dBadErr, lBadErr}, "empty": {dEmptyErr, lEmptyErr},
	} {
		if pair[0] == nil || pair[1] == nil || pair[0].Error() != pair[1].Error() {
			t.Errorf("%s: direct err %v != delegated err %v", name, pair[0], pair[1])
		}
	}
	if !errors.Is(lDupErr, store.ErrTokenExists) {
		t.Errorf("delegated duplicate err = %v, want ErrTokenExists", lDupErr)
	}
	// The token printed by the CLI is live in the serving store's index.
	if id, ok, err := st.LookupNodeID(context.Background(), strings.TrimSpace(lOut)); err != nil || !ok || id != "n1" {
		t.Fatalf("issued token not usable on the serving store: %q %v %v", id, ok, err)
	}
}

// Rotation without stopping the collector: issue, revoke, issue again.
func TestTokenIssue_RotationWhileRunning(t *testing.T) {
	dir := shortTempDir(t)
	st := startAdmin(t, dir)
	first, err := runTokenCmd(t, dir, "", "issue", "n1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "n1"); err != nil {
		t.Fatal(err)
	}
	second, err := runTokenCmd(t, dir, "", "issue", "n1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, ok, _ := st.LookupNodeID(ctx, strings.TrimSpace(first)); ok {
		t.Error("revoked token still authenticates")
	}
	if id, ok, _ := st.LookupNodeID(ctx, strings.TrimSpace(second)); !ok || id != "n1" {
		t.Error("reissued token does not authenticate")
	}
}

// Delegated list must print byte-for-byte what the direct path prints for the
// same data-dir.
func TestTokenList_DelegatedMatchesDirect(t *testing.T) {
	for name, prep := range map[string]func(dir string){
		"empty": func(string) {},
		"mixed": func(dir string) {
			for _, a := range [][]string{{"issue", "b"}, {"issue", "a"}, {"issue", "c"}, {"revoke", "--yes", "b"}} {
				if _, err := runTokenCmd(t, dir, "", a...); err != nil {
					t.Fatal(err)
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := shortTempDir(t)
			prep(dir)
			dOut, dErr := runTokenCmd(t, dir, "", "list")
			startAdmin(t, dir)
			lOut, lErr := runTokenCmd(t, dir, "", "list")
			if dErr != nil || lErr != nil || dOut != lOut {
				t.Fatalf("direct (%q, %v) != delegated (%q, %v)", dOut, dErr, lOut, lErr)
			}
			if !strings.HasPrefix(lOut, "NODE_ID") {
				t.Fatalf("unexpected output %q", lOut)
			}
		})
	}
}

func TestTokenIssueList_NoSocketUsesDirectPath(t *testing.T) {
	dir := shortTempDir(t)
	if err := os.Mkdir(adminDir(dir), 0o700); err != nil { // dir, no socket
		t.Fatal(err)
	}
	if _, err := runTokenCmd(t, dir, "", "issue", "n1"); err != nil {
		t.Fatalf("issue, empty admin dir: %v", err)
	}
	ln, err := net.Listen("unix", adminSocketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	if out, err := runTokenCmd(t, dir, "", "list"); err != nil || !strings.Contains(out, "n1") {
		t.Fatalf("list, stale socket: %q, %v", out, err)
	}
}

func TestTokenIssueList_UnsafeAdminDirIsError(t *testing.T) {
	dir := shortTempDir(t)
	if err := os.Mkdir(adminDir(dir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(adminDir(dir), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"issue", "n1"}, {"list"}} {
		if _, err := runTokenCmd(t, dir, "", args...); err == nil || !strings.Contains(err.Error(), "unsafe") {
			t.Fatalf("%v: err = %v, want an unsafe-directory error", args, err)
		}
	}
	// And nothing was issued behind the refusal.
	if err := os.Chmod(adminDir(dir), 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := runTokenCmd(t, dir, "", "list"); err != nil || strings.Count(out, "\n") != 1 {
		t.Fatalf("a token was issued despite the unsafe dir: %q, %v", out, err)
	}
}

// --force is never delegated: against a running serve it stays a lock error.
// A plain issue/list against a serve without the admin socket gets the lock
// error too (covered by TestOpenTokenStore_LockedErrorMentionsForceOnly).

// rawAdmin sends a request straight to the admin socket.
func rawAdmin(t *testing.T, dataDir, method, path, body string) (int, string, http.Header) {
	t.Helper()
	sock := adminSocketPath(dataDir)
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}
	req, err := http.NewRequest(method, "http://admin"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func TestAdminHandlers_MethodAndBodyLimits(t *testing.T) {
	dir := shortTempDir(t)
	st := startAdmin(t, dir)
	for _, c := range []struct{ method, path string }{
		{"GET", adminIssuePath}, {"PUT", adminIssuePath}, {"DELETE", adminIssuePath},
		{"POST", adminListPath}, {"PUT", adminListPath}, {"GET", adminRevokePath},
	} {
		if code, _, h := rawAdmin(t, dir, c.method, c.path, `{"node_id":"n1"}`); code != http.StatusMethodNotAllowed || h.Get("Allow") == "" {
			t.Errorf("%s %s = %d (Allow %q), want 405", c.method, c.path, code, h.Get("Allow"))
		}
	}
	for _, path := range []string{adminIssuePath, adminRevokePath} {
		big := `{"node_id":"` + strings.Repeat("x", 5000) + `"}`
		if code, _, _ := rawAdmin(t, dir, "POST", path, big); code != http.StatusBadRequest {
			t.Errorf("%s oversized body = %d, want 400", path, code)
		}
		if code, _, _ := rawAdmin(t, dir, "POST", path, `not json`); code != http.StatusBadRequest {
			t.Errorf("%s bad json = %d, want 400", path, code)
		}
	}
	// Server-side node_id validation (not only the CLI's).
	if code, _, _ := rawAdmin(t, dir, "POST", adminIssuePath, `{"node_id":"bad\u0001id"}`); code != http.StatusBadRequest {
		t.Errorf("invalid node_id = %d, want 400", code)
	}
	if toks, _ := st.ListTokens(context.Background()); len(toks) != 0 {
		t.Fatalf("rejected requests created tokens: %+v", toks)
	}
	// Issue responses must not be cacheable.
	if _, _, h := rawAdmin(t, dir, "POST", adminIssuePath, `{"node_id":"n1"}`); h.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q", h.Get("Cache-Control"))
	}
}

// The plaintext token must not reach serve's log on success or on any error
// path; the audit line carries only action and node_id.
func TestAdmin_IssueNeverLogsToken(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	dir := shortTempDir(t)
	startAdminLogged(t, dir, zap.New(core))

	out, err := runTokenCmd(t, dir, "", "issue", "n1")
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(out)
	_, _ = runTokenCmd(t, dir, "", "issue", "n1")                   // duplicate
	rawAdmin(t, dir, "POST", adminIssuePath, `{"junk":"`+token+`"`) // token inside a malformed body
	rawAdmin(t, dir, "GET", adminIssuePath, "")
	if _, err := runTokenCmd(t, dir, "", "list"); err != nil {
		t.Fatal(err)
	}
	if _, err := runTokenCmd(t, dir, "", "revoke", "--yes", "n1"); err != nil {
		t.Fatal(err)
	}

	var audits []map[string]any
	for _, e := range logs.All() {
		blob := e.Message
		for k, v := range e.ContextMap() {
			blob += k + fmt.Sprint(v)
		}
		if strings.Contains(blob, token) {
			t.Fatalf("log entry leaks the token: %q", e.Message)
		}
		if e.Message == "admin action" {
			audits = append(audits, e.ContextMap())
		}
	}
	want := []map[string]any{
		{"action": "issue", "node_id": "n1"},
		{"action": "revoke", "node_id": "n1"},
	}
	if !reflect.DeepEqual(audits, want) {
		t.Fatalf("audit lines = %v, want %v", audits, want)
	}
}
