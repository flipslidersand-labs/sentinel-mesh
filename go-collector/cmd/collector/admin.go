package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/receiver"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// Live token administration (#572, #574). `token issue|list|revoke` normally
// open the BadgerDB directory, which a running serve already holds
// exclusively. serve therefore also listens on a local Unix socket under
// <data-dir>/admin/; the CLI delegates issue, list and a plain revoke to it.
// The socket is reachable only through a directory that is a real directory,
// owned by the effective user and mode 0700.
//
// The issue response carries the plaintext token. It must never reach a log:
// no handler or error path in this file logs a request or response body, only
// the audit line (action and node_id).
const (
	adminDirName  = "admin"
	adminSockName = "admin.sock"

	adminRevokePath = "/tokens/revoke"
	adminIssuePath  = "/tokens/issue"
	adminListPath   = "/tokens"

	// Request bodies are capped at adminReqLimit. The list response grows with
	// the number of tokens; the other responses are tiny.
	adminReqLimit       = 4096
	adminSmallRespLimit = 4096
	adminListRespLimit  = 64 << 20

	adminActionIssue  = "issue"
	adminActionRevoke = "revoke"

	adminResultRevoked        = "revoked"
	adminResultAlreadyRevoked = "already_revoked"
	adminResultNotFound       = "not_found"
)

// adminStore is the part of *store.Store the admin endpoint uses. It must be
// the same Store the gRPC receiver authenticates against, so a revoke drops
// the token from the in-memory index the liveness re-check reads.
type adminStore interface {
	IssueToken(ctx context.Context, nodeID string) (string, error)
	ListTokens(ctx context.Context) ([]store.AgentToken, error)
	RevokeToken(ctx context.Context, nodeID string) error
}

type adminNodeRequest struct {
	NodeID string `json:"node_id"`
}

type adminResponse struct {
	Result string             `json:"result,omitempty"`
	Token  string             `json:"token,omitempty"` // issue only: plaintext, never logged
	Tokens []store.AgentToken `json:"tokens,omitempty"`
	Error  string             `json:"error,omitempty"`
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

// serveAdmin serves the live token admin endpoints on a Unix socket at sockPath
// until ctx is done. It never fails the process: if the socket cannot be
// opened (path too long, unsafe admin dir, ...) it logs a warning and returns
// nil, and serve continues without live token admin.
func serveAdmin(ctx context.Context, sockPath string, st adminStore, logger *zap.Logger) {
	warn := func(err error) {
		logger.Warn("admin socket not opened; `token issue|list|revoke` need serve stopped",
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
	mux.HandleFunc(adminRevokePath, adminRevokeHandler(st, logger))
	mux.HandleFunc(adminIssuePath, adminIssueHandler(st, logger))
	mux.HandleFunc(adminListPath, adminListHandler(st))
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
			logger.Warn("admin socket stopped; `token issue|list|revoke` need serve stopped", zap.Error(err))
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

func writeAdminJSON(w http.ResponseWriter, code int, resp adminResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// requireMethod writes a 405 and returns false unless r uses method.
func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeAdminJSON(w, http.StatusMethodNotAllowed, adminResponse{Error: "method not allowed"})
	return false
}

// decodeNodeID reads the 4096-byte-capped request body and returns the
// trimmed node_id, or writes a 400 and returns false.
func decodeNodeID(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req adminNodeRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, adminReqLimit))
	if err := dec.Decode(&req); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, adminResponse{Error: "invalid request body"})
		return "", false
	}
	nodeID := strings.TrimSpace(req.NodeID)
	if nodeID == "" {
		writeAdminJSON(w, http.StatusBadRequest, adminResponse{Error: "node_id must not be empty"})
		return "", false
	}
	return nodeID, true
}

// audit records that an admin action ran. Only the action and node_id are
// logged: never the token, a hash, or a request/response body.
func audit(logger *zap.Logger, action, nodeID string) {
	logger.Info("admin action", zap.String("action", action), zap.String("node_id", nodeID))
}

func adminRevokeHandler(st adminStore, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		nodeID, ok := decodeNodeID(w, r)
		if !ok {
			return
		}
		// Same semantics as the direct path: RevokeToken is a no-op for an
		// unknown or already revoked node. The kind is read first only so the
		// CLI can report what actually happened.
		result := adminResultNotFound
		tokens, err := st.ListTokens(r.Context())
		if err != nil {
			writeAdminJSON(w, http.StatusInternalServerError, adminResponse{Error: err.Error()})
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
			writeAdminJSON(w, http.StatusInternalServerError, adminResponse{Error: err.Error()})
			return
		}
		audit(logger, adminActionRevoke, nodeID)
		writeAdminJSON(w, http.StatusOK, adminResponse{Result: result})
	}
}

// adminIssueHandler issues through the same Store the gRPC receiver
// authenticates against, so IssueToken's index update makes the new token
// usable at once. node_id goes through the same validation as the direct path.
func adminIssueHandler(st adminStore, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		nodeID, ok := decodeNodeID(w, r)
		if !ok {
			return
		}
		if err := receiver.ValidateNodeID(nodeID); err != nil {
			writeAdminJSON(w, http.StatusBadRequest, adminResponse{Error: err.Error()})
			return
		}
		token, err := st.IssueToken(r.Context(), nodeID)
		if err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, store.ErrTokenExists) {
				code = http.StatusConflict
			}
			writeAdminJSON(w, code, adminResponse{Error: err.Error()})
			return
		}
		audit(logger, adminActionIssue, nodeID)
		writeAdminJSON(w, http.StatusOK, adminResponse{Token: token})
	}
}

func adminListHandler(st adminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		tokens, err := st.ListTokens(r.Context())
		if err != nil {
			writeAdminJSON(w, http.StatusInternalServerError, adminResponse{Error: err.Error()})
			return
		}
		writeAdminJSON(w, http.StatusOK, adminResponse{Tokens: tokens})
	}
}

// callAdmin sends one request to a running serve. delegated is false (with a
// nil error) only when no serve can be listening: the admin dir or socket does
// not exist, or the connection is refused (stale socket). An unsafe admin dir
// or any other failure is an error: the CLI never falls back silently after a
// failed check. On delegated=true the decoded response and HTTP status are
// returned. The body is never logged or echoed into an error.
func callAdmin(ctx context.Context, dataDir, method, path string, in *adminNodeRequest, respLimit int64) (out adminResponse, status int, delegated bool, err error) {
	sock := adminSocketPath(dataDir)
	if !adminSockPathFits(sock) {
		return out, 0, false, nil // serve could not have opened it either
	}
	if err := checkAdminDir(adminDir(dataDir), os.Geteuid()); err != nil {
		// A missing admin dir, or a data-dir whose parent is not a directory,
		// means no serve can be listening here.
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return out, 0, false, nil
		}
		return out, 0, false, fmt.Errorf("admin socket directory is unsafe, refusing to use it: %w", err)
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
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return out, 0, false, err
		}
		body = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://admin"+path, body)
	if err != nil {
		return out, 0, false, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
			return out, 0, false, nil
		}
		return out, 0, false, fmt.Errorf("admin socket %s: %w", sock, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if err := json.NewDecoder(io.LimitReader(resp.Body, respLimit)).Decode(&out); err != nil {
		return out, resp.StatusCode, true, fmt.Errorf("admin socket: unreadable response (HTTP %d): %w", resp.StatusCode, err)
	}
	return out, resp.StatusCode, true, nil
}

// revokeViaAdmin asks a running serve to revoke nodeID (see callAdmin for the
// delegated contract).
func revokeViaAdmin(ctx context.Context, dataDir, nodeID string) (result string, delegated bool, err error) {
	out, status, delegated, err := callAdmin(ctx, dataDir, http.MethodPost, adminRevokePath, &adminNodeRequest{NodeID: nodeID}, adminSmallRespLimit)
	if !delegated || err != nil {
		return "", delegated, err
	}
	if status != http.StatusOK {
		return "", true, fmt.Errorf("revoke via running collector failed: %s", out.Error)
	}
	return out.Result, true, nil
}

// issueViaAdmin asks a running serve to issue a token for nodeID. Errors are
// surfaced with the same text as the direct path (ErrTokenExists included),
// so the CLI's stderr and exit code do not depend on the route taken.
func issueViaAdmin(ctx context.Context, dataDir, nodeID string) (token string, delegated bool, err error) {
	out, status, delegated, err := callAdmin(ctx, dataDir, http.MethodPost, adminIssuePath, &adminNodeRequest{NodeID: nodeID}, adminSmallRespLimit)
	if !delegated || err != nil {
		return "", delegated, err
	}
	switch {
	case status == http.StatusOK && out.Token != "":
		return out.Token, true, nil
	case status == http.StatusConflict:
		return "", true, store.ErrTokenExists
	case status == http.StatusOK:
		return "", true, errors.New("admin socket: response carried no token")
	default:
		return "", true, errors.New(out.Error)
	}
}

// listViaAdmin asks a running serve for all token records.
func listViaAdmin(ctx context.Context, dataDir string) (tokens []store.AgentToken, delegated bool, err error) {
	out, status, delegated, err := callAdmin(ctx, dataDir, http.MethodGet, adminListPath, nil, adminListRespLimit)
	if !delegated || err != nil {
		return nil, delegated, err
	}
	if status != http.StatusOK {
		return nil, true, errors.New(out.Error)
	}
	return out.Tokens, true, nil
}
