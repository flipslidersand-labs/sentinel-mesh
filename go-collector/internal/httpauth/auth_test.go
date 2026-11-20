package httpauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/authlimit"
	"github.com/gin-gonic/gin"
)

func newRouter(token string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/x", BearerAuth(token), func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	return r
}

func do(r *gin.Engine, authHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/x", nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestBearerAuth_DisabledWhenEmpty(t *testing.T) {
	r := newRouter("")
	if got := do(r, "").Code; got != http.StatusOK {
		t.Fatalf("empty token should disable auth, got %d", got)
	}
}

func TestBearerAuth_ValidToken(t *testing.T) {
	r := newRouter("s3cret")
	if got := do(r, "Bearer s3cret").Code; got != http.StatusOK {
		t.Fatalf("valid token should pass, got %d", got)
	}
}

func TestBearerAuth_RejectsMissingAndWrong(t *testing.T) {
	r := newRouter("s3cret")
	for _, h := range []string{"", "Bearer wrong", "s3cret", "Basic s3cret"} {
		if got := do(r, h).Code; got != http.StatusUnauthorized {
			t.Fatalf("header %q should be rejected, got %d", h, got)
		}
	}
}

func TestBearerAuth_RateLimitsRepeatedFailures(t *testing.T) {
	r := newRouter("s3cret")
	for i := 0; i < int(authlimit.Burst); i++ {
		if got := do(r, "Bearer wrong").Code; got != http.StatusUnauthorized {
			t.Fatalf("attempt %d: got %d, want 401", i, got)
		}
	}
	w := do(r, "Bearer wrong")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("got %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}
	if got := do(r, "Bearer s3cret").Code; got != http.StatusOK {
		t.Fatalf("valid token must pass even when limited, got %d", got)
	}
}

func TestBearerAuth_ForwardedForDoesNotEvadeLimit(t *testing.T) {
	r := newRouter("s3cret")
	var last int
	for i := 0; i < int(authlimit.Burst)+1; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/x", nil)
		req.Header.Set("Authorization", "Bearer wrong")
		req.Header.Set("X-Forwarded-For", "203.0.113."+string(rune('1'+i)))
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		last = w.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("spoofed X-Forwarded-For evaded limit: %d", last)
	}
}

func TestFailLimiter_RecoversAndKeysIPv6(t *testing.T) {
	l := authlimit.New()
	now := time.Unix(1000, 0)
	l.Now = func() time.Time { return now }
	for i := 0; i < int(authlimit.Burst); i++ {
		l.Reserve("2001:db8:1:2::1")
	}
	if l.Reserve("2001:db8:1:2:ffff::9") {
		t.Fatal("same /64 should share budget")
	}
	if !l.Reserve("2001:db8:1:3::1") {
		t.Fatal("other /64 should be independent")
	}
	now = now.Add(13 * time.Second)
	if !l.Reserve("2001:db8:1:2::1") {
		t.Fatal("budget should recover over time")
	}
}
