package httpauth

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
