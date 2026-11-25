package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// Live revoke (#572). `token revoke` normally opens the BadgerDB directory,
// which a running serve already holds exclusively. serve therefore also
// listens on a local Unix socket under <data-dir>/admin/; the CLI delegates
// a plain revoke to it. The socket is reachable only through a directory that
// is a real directory, owned by the effective user and mode 0700.
const (
	adminDirName  = "admin"
	adminSockName = "admin.sock"

	adminRevokePath = "/tokens/revoke"

	adminResultRevoked        = "revoked"
	adminResultAlreadyRevoked = "already_revoked"
	adminResultNotFound       = "not_found"
)

// adminStore is the part of *store.Store the admin endpoint uses. It must be
// the same Store the gRPC receiver authenticates against, so a revoke drops
// the token from the in-memory index the liveness re-check reads.
type adminStore interface {
	ListTokens(ctx context.Context) ([]store.AgentToken, error)
	RevokeToken(ctx context.Context, nodeID string) error
}

type adminRevokeRequest struct {
	NodeID string `json:"node_id"`
}

type adminRevokeResponse struct {
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func adminDir(dataDir string) string { return filepath.Join(dataDir, adminDirName) }

func adminSocketPath(dataDir string) string {
	return filepath.Join(adminDir(dataDir), adminSockName)
}

// adminSockPathFits reports whether path fits in sockaddr_un.sun_path (the
// kernel limit, which net.Listen/Dial enforce). serve and the CLI share it.
func adminSockPathFits(path string) bool {
	var sa syscall.RawSockaddrUnix
	return len(path) < len(sa.Path)
}

// checkAdminDir verifies dir is a real directory (not a symlink), owned by
// euid, with permissions exactly 0700. A missing dir wraps fs.ErrNotExist.
func checkAdminDir(dir string, euid int) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() { // Lstat: a symlink reports ModeSymlink, never IsDir
		return fmt.Errorf("%s is not a real directory (symlinks are refused)", dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s: cannot determine owner on this platform", dir)
	}
	if int(st.Uid) != euid {
		return fmt.Errorf("%s is owned by uid %d, not by the current user (uid %d)", dir, st.Uid, euid)
	}
	if fi.Mode().Perm() != 0o700 || fi.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return fmt.Errorf("%s has mode %v, want drwx------ (0700)", dir, fi.Mode())
	}
	return nil
}

// prepareAdminDir creates dir with mode 0700 if it is missing, then always
// verifies it (an existing dir, a lost mkdir race and the umask all end up here).
func prepareAdminDir(dir string) error {
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
	}
	return checkAdminDir(dir, os.Geteuid())
}

// removeStaleSocket deletes a leftover socket from a previous run. Nothing
// else is removed: serve holds the Badger lock, so no other serve is using
// this data-dir, but a non-socket file is never ours to delete.
func removeStaleSocket(path string) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Mode().Type() != fs.ModeSocket {
		return fmt.Errorf("%s exists and is not a socket; not removing it", path)
	}
	return os.Remove(path)
}

// serveAdmin serves the live-revoke endpoint on a Unix socket at sockPath
// until ctx is done. It never fails the process: if the socket cannot be
// opened (path too long, unsafe admin dir, ...) it logs a warning and returns
// nil, and serve continues without live revoke.
func serveAdmin(ctx context.Context, sockPath string, st adminStore, logger *zap.Logger) {
	warn := func(err error) {
		logger.Warn("admin socket not opened; `token revoke` needs serve stopped",
			zap.String("path", sockPath), zap.Error(err))
	}
	if !adminSockPathFits(sockPath) {
		warn(errors.New("socket path is too long for a Unix socket"))
		return
	}
	if err := prepareAdminDir(filepath.Dir(sockPath)); err != nil {
		warn(err)
		return
	}
	if err := removeStaleSocket(sockPath); err != nil {
		warn(err)
		return
	}
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		warn(err)
		return
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		_ = ln.Close()
		warn(err)
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc(adminRevokePath, adminRevokeHandler(st))
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	logger.Info("admin socket listening", zap.String("path", sockPath))

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("admin socket stopped; `token revoke` needs serve stopped", zap.Error(err))
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
		<-errCh
	}
}

// restWithAdmin wraps the REST runner so the admin socket runs next to it and
// is fully stopped before the wrapper returns. runNormal closes the store only
// after both servers return, so no admin handler can touch a closed store.
func restWithAdmin(rest func(context.Context) error, sockPath string, st adminStore, logger *zap.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		actx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			serveAdmin(actx, sockPath, st, logger)
		}()
		err := rest(ctx)
		stop()
		<-done
		return err
	}
}

func writeAdminJSON(w http.ResponseWriter, code int, resp adminRevokeResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

func adminRevokeHandler(st adminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeAdminJSON(w, http.StatusMethodNotAllowed, adminRevokeResponse{Error: "method not allowed"})
			return
		}
		var req adminRevokeRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		if err := dec.Decode(&req); err != nil {
			writeAdminJSON(w, http.StatusBadRequest, adminRevokeResponse{Error: "invalid request body"})
			return
		}
		nodeID := strings.TrimSpace(req.NodeID)
		if nodeID == "" {
			writeAdminJSON(w, http.StatusBadRequest, adminRevokeResponse{Error: "node_id must not be empty"})
			return
		}
		// Same semantics as the direct path: RevokeToken is a no-op for an
		// unknown or already revoked node. The kind is read first only so the
		// CLI can report what actually happened.
		result := adminResultNotFound
		tokens, err := st.ListTokens(r.Context())
		if err != nil {
			writeAdminJSON(w, http.StatusInternalServerError, adminRevokeResponse{Error: err.Error()})
			return
		}
		for _, tok := range tokens {
			if tok.NodeID == nodeID {
				result = adminResultRevoked
				if tok.RevokedAt != nil {
					result = adminResultAlreadyRevoked
				}
			}
		}
		if err := st.RevokeToken(r.Context(), nodeID); err != nil {
			writeAdminJSON(w, http.StatusInternalServerError, adminRevokeResponse{Error: err.Error()})
			return
		}
		writeAdminJSON(w, http.StatusOK, adminRevokeResponse{Result: result})
	}
}

// revokeViaAdmin asks a running serve to revoke nodeID. delegated is false
// (with a nil error) only when no serve can be listening: the admin dir or
// socket does not exist, or the connection is refused (stale socket). An
// unsafe admin dir or any other failure is an error: the CLI never falls
// back silently after a failed check.
func revokeViaAdmin(ctx context.Context, dataDir, nodeID string) (result string, delegated bool, err error) {
	sock := adminSocketPath(dataDir)
	if !adminSockPathFits(sock) {
		return "", false, nil // serve could not have opened it either
	}
	if err := checkAdminDir(adminDir(dataDir), os.Geteuid()); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("admin socket directory is unsafe, refusing to use it: %w", err)
	}
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
			DisableKeepAlives: true,
		},
	}
	body, err := json.Marshal(adminRevokeRequest{NodeID: nodeID})
	if err != nil {
		return "", false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://admin"+adminRevokePath, strings.NewReader(string(body)))
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("admin socket %s: %w", sock, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	var out adminRevokeResponse
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 4096)).Decode(&out); err != nil {
		return "", true, fmt.Errorf("admin socket: unreadable response (HTTP %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", true, fmt.Errorf("revoke via running collector failed: %s", out.Error)
	}
	return out.Result, true, nil
}
