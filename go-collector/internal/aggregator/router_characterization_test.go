package aggregator

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func writeStaticDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"index.html":    "INDEX",
		"favicon.svg":   "FAVICON",
		"assets/app.js": "APPJS",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func serve(r *gin.Engine, method, path string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// Characterization tests pinning the scaffolding behavior of Router so the
// shared base-router refactor (#264) cannot change it.
func TestRouterScaffold_Characterization(t *testing.T) {
	dir := writeStaticDir(t)
	agg := New(nil, 0, "", nil)
	r := Router(agg, dir, nil, "s3cr3t")

	cases := []struct {
		name, path string
		code       int
		body       string
	}{
		{"root", "/", 200, "INDEX"},
		{"favicon", "/favicon.svg", 200, "FAVICON"},
		{"asset", "/assets/app.js", 200, "APPJS"},
		{"spa fallback", "/some/ui/route", 200, "INDEX"},
		{"api not found is JSON 404", "/api/nope", 404, `{"error":"not found"}`},
		{"healthz open with mode", "/healthz", 200, `{"mode":"aggregator","status":"ok"}`},
		// Aggregator registers no /metrics: it falls through to the SPA fallback.
		{"metrics unregistered", "/metrics", 200, "INDEX"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := serve(r, http.MethodGet, c.path, nil)
			if w.Code != c.code || !strings.Contains(w.Body.String(), c.body) {
				t.Errorf("%s = %d %q, want %d containing %q", c.path, w.Code, w.Body.String(), c.code, c.body)
			}
		})
	}

	if w := serve(r, http.MethodGet, "/api/nodes", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("/api/nodes without token = %d, want 401", w.Code)
	}
	if w := serve(r, http.MethodGet, "/api/nodes", map[string]string{"Authorization": "Bearer s3cr3t"}); w.Code != http.StatusOK {
		t.Errorf("/api/nodes with token = %d, want 200", w.Code)
	}
	open := Router(agg, dir, nil, "")
	if w := serve(open, http.MethodGet, "/api/nodes", nil); w.Code != http.StatusOK {
		t.Errorf("/api/nodes with empty token = %d, want 200", w.Code)
	}
}

func TestRouterScaffold_CORS_Characterization(t *testing.T) {
	dir := writeStaticDir(t)
	agg := New(nil, 0, "", nil)
	const origin = "https://ui.example.com"

	r := Router(agg, dir, []string{origin}, "")
	w := serve(r, http.MethodGet, "/healthz", map[string]string{"Origin": origin})
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("allowed origin ACAO = %q, want %q", got, origin)
	}
	w = serve(r, http.MethodGet, "/healthz", map[string]string{"Origin": "https://evil.example.com"})
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("disallowed origin ACAO = %q, want empty", got)
	}

	deny := Router(agg, dir, nil, "")
	w = serve(deny, http.MethodGet, "/healthz", map[string]string{"Origin": origin})
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("no-config ACAO = %q, want empty", got)
	}
	if w.Code != http.StatusOK {
		t.Errorf("no-config cross-origin code = %d, want 200", w.Code)
	}
}
