package httpserver

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSecurityHeaders(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := NewBaseRouter(dir, nil, gin.H{"status": "ok"})
	r.GET("/api/v1/x", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{}) })

	want := map[string]string{
		"Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "no-referrer",
	}
	for _, path := range []string{"/", "/healthz", "/api/v1/x", "/api/missing", "/spa/route"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		for k, v := range want {
			if got := w.Header().Get(k); got != v {
				t.Errorf("%s: %s = %q, want %q", path, k, got, v)
			}
		}
	}
}

func TestCORSPreflightAllowsAuthorization(t *testing.T) {
	const origin = "https://ui.example.test"
	r := NewBaseRouter(t.TempDir(), []string{origin}, gin.H{"status": "ok"})

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/nodes", nil)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	req.Header.Set("Access-Control-Request-Headers", "authorization")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("Allow-Origin = %q, want %q", got, origin)
	}
	allowed := strings.ToLower(w.Header().Get("Access-Control-Allow-Headers"))
	if !strings.Contains(allowed, "authorization") {
		t.Errorf("Allow-Headers = %q, want it to include Authorization", allowed)
	}
}
