// Package httpauth provides shared HTTP authentication middleware for the
// collector's REST API (exporter and aggregator modes).
package httpauth

import (
	"crypto/subtle"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// EnvAPIToken is the environment variable holding the bearer token required to
// access /api/* endpoints. Following the notify package convention, secrets are
// read from the environment only — never from flags or YAML.
const EnvAPIToken = "SENTINEL_API_TOKEN" //nolint:gosec // env var name, not a credential

// TokenFromEnv returns the configured API token (trimmed), or "" if unset.
func TokenFromEnv() string {
	return strings.TrimSpace(os.Getenv(EnvAPIToken))
}

// BearerAuth returns middleware that requires an `Authorization: Bearer <token>`
// header matching the given token, compared in constant time. If token is empty
// the middleware is a no-op (auth disabled) — callers are expected to warn.
func BearerAuth(token string) gin.HandlerFunc {
	if token == "" {
		return func(c *gin.Context) { c.Next() }
	}
	want := []byte(token)
	return func(c *gin.Context) {
		got, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}
