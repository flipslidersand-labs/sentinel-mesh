// Package httpauth provides shared HTTP authentication middleware for the
// collector's REST API (exporter and aggregator modes).
package httpauth

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// EnvAPIToken is the environment variable holding the bearer token required to
// access /api/* endpoints. Following the notify package convention, secrets are
// read from the environment only — never from flags or YAML.
const EnvAPIToken = "SENTINEL_API_TOKEN" //nolint:gosec // env var name, not a credential

// MinTokenBytes is the minimum accepted length of SENTINEL_API_TOKEN. Shorter
// tokens make online brute force practical, so startup is refused (#360).
const MinTokenBytes = 32

// TokenFromEnv returns the configured API token (trimmed), or "" if unset.
func TokenFromEnv() string {
	return strings.TrimSpace(os.Getenv(EnvAPIToken))
}

// ParseMode maps the --http-auth value to whether a bearer token is required.
// Only the exact values "required" and "disabled" are accepted so a typo can
// never silently turn authentication off.
func ParseMode(v string) (bool, error) {
	switch v {
	case "required":
		return true, nil
	case "disabled":
		return false, nil
	}
	return false, fmt.Errorf("invalid --http-auth %q: must be \"required\" or \"disabled\"", v)
}

// ResolveToken applies the --http-auth policy shared by the normal and
// aggregate REST APIs. It returns the token to enforce and whether the API
// will be served unauthenticated (only possible with an explicit
// --http-auth=disabled and no token set). With "required" and no token it
// fails, so the REST API is never exposed unauthenticated by omission.
func ResolveToken(mode string) (token string, unauthenticated bool, err error) {
	required, err := ParseMode(mode)
	if err != nil {
		return "", false, err
	}
	token = TokenFromEnv()
	if token != "" {
		if len(token) < MinTokenBytes {
			// Never include the token value (or any prefix) in the error.
			return "", false, fmt.Errorf("%s is too short: got %d bytes, need at least %d (e.g. generate one with `openssl rand -hex 32`)", EnvAPIToken, len(token), MinTokenBytes)
		}
		return token, false, nil
	}
	if required {
		return "", false, fmt.Errorf("REST API auth is required but %s is not set: set %s to a bearer token, or pass --http-auth=disabled to serve the REST API unauthenticated (dev/trusted networks only)", EnvAPIToken, EnvAPIToken)
	}
	return "", true, nil
}

// BearerAuth returns middleware that requires an `Authorization: Bearer <token>`
// header matching the given token, compared in constant time. If token is empty
// the middleware is a no-op (auth disabled) — callers are expected to warn.
func BearerAuth(token string) gin.HandlerFunc {
	if token == "" {
		return func(c *gin.Context) { c.Next() }
	}
	return bearerAuth(token, newFailLimiter())
}

func bearerAuth(token string, lim *failLimiter) gin.HandlerFunc {
	want := []byte(token)
	return func(c *gin.Context) {
		got, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		if ok && subtle.ConstantTimeCompare([]byte(got), want) == 1 {
			// A valid token always passes, whatever the source's failure budget.
			c.Next()
			return
		}
		// RemoteIP (not ClientIP): X-Forwarded-For is client-controlled and
		// would let an attacker dodge the limit.
		if !lim.reserve(c.RemoteIP()) {
			c.Header("Retry-After", "60")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "too many failed attempts"})
			return
		}
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
	}
}
