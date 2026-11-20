// Package httpserver holds scaffolding shared by the collector and aggregator
// HTTP routers so the two cannot drift apart.
package httpserver

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// securityHeaders sets defensive response headers on every response (UI and
// API). The bundled Vite build has no inline script or style, so a strict
// same-origin CSP is sufficient.
func securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.Writer.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		c.Next()
	}
}

// NewBaseRouter builds a gin engine with the common scaffolding: Recovery,
// security headers, default-deny CORS (only corsOrigins are allowed; empty means no CORS
// middleware), the static SPA (/assets, /, /favicon.svg), a NoRoute handler
// (JSON 404 under /api/, SPA index.html otherwise) and /healthz returning
// healthz as JSON. Callers register their own routes (and any auth) on the
// returned engine.
func NewBaseRouter(staticDir string, corsOrigins []string, healthz gin.H) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(securityHeaders())

	// CORS: default deny. Only allow explicitly configured origins. Same-origin
	// requests (the bundled UI) carry no Origin header and are unaffected.
	if len(corsOrigins) > 0 {
		corsConfig := cors.DefaultConfig()
		corsConfig.AllowOrigins = corsOrigins
		// API routes require "Authorization: Bearer", which DefaultConfig does not allow.
		corsConfig.AddAllowHeaders("Authorization")
		r.Use(cors.New(corsConfig))
	}

	r.Static("/assets", staticDir+"/assets")
	r.StaticFile("/", staticDir+"/index.html")
	r.StaticFile("/favicon.svg", staticDir+"/favicon.svg")
	r.NoRoute(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/api/") {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		c.File(staticDir + "/index.html")
	})

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, healthz)
	})
	return r
}

// MaxLimit caps the "limit" query parameter accepted by list endpoints,
// preventing a client-supplied value from forcing an unbounded scan or an
// oversized JSON response.
const MaxLimit = 1000

// ParseLimit parses the "limit" query parameter for key. When absent, def is
// returned. When present but not a positive integer, it writes a 400
// response and returns ok=false so the caller can abort the request. Values
// above MaxLimit are silently clamped rather than rejected.
func ParseLimit(c *gin.Context, key string, def int) (limit int, ok bool) {
	v := c.Query(key)
	if v == "" {
		return def, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + key + ": must be a positive integer"})
		return 0, false
	}
	if n > MaxLimit {
		n = MaxLimit
	}
	return n, true
}
