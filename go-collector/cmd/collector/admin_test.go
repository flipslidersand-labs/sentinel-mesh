package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// startAdmin opens the store at dataDir (taking the Badger lock, like serve)
// and serves the admin socket on it until the test ends.
func startAdmin(t *testing.T, dataDir string) *store.Store {
	t.Helper()
	st, err := store.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); serveAdmin(ctx, adminSocketPath(dataDir), st, zap.NewNop()) }()
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

// issue and list are not delegated either.
func TestTokenIssueList_NotDelegated(t *testing.T) {
	dir := shortTempDir(t)
	startAdmin(t, dir)
	for _, args := range [][]string{{"issue", "n1"}, {"list"}} {
		if _, err := runTokenCmd(t, dir, "", args...); err == nil || !strings.Contains(err.Error(), "open store") {
			t.Fatalf("%v: err = %v, want the store-lock error", args, err)
		}
	}
}
