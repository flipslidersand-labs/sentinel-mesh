// Package httpserver holds scaffolding shared by the collector and aggregator
// HTTP routers so the two cannot drift apart.
package httpserver

import (
	"net/http"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// NewBaseRouter builds a gin engine with the common scaffolding: Recovery,
// default-deny CORS (only corsOrigins are allowed; empty means no CORS
// middleware), the static SPA (/assets, /, /favicon.svg), a NoRoute handler
// (JSON 404 under /api/, SPA index.html otherwise) and /healthz returning
// healthz as JSON. Callers register their own routes (and any auth) on the
// returned engine.
func NewBaseRouter(staticDir string, corsOrigins []string, healthz gin.H) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// CORS: default deny. Only allow explicitly configured origins. Same-origin
	// requests (the bundled UI) carry no Origin header and are unaffected.
	if len(corsOrigins) > 0 {
		corsConfig := cors.DefaultConfig()
		corsConfig.AllowOrigins = corsOrigins
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
