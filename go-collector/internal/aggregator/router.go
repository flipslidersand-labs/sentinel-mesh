package aggregator

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
	"github.com/flipslidersand/sentinel-mesh/internal/httpserver"
)

// Router builds the read-only HTTP router for aggregate mode. It serves the same
// REST shape as a normal collector (nodes / events / alerts / regions) plus the
// static UI, but backed by the aggregator's merged cache instead of a store.
// corsOrigins restricts cross-origin access (empty = deny cross-origin);
// apiToken, when non-empty, gates /api/* behind bearer auth.
func Router(a *Aggregator, staticDir string, corsOrigins []string, apiToken string) *gin.Engine {
	r := httpserver.NewBaseRouter(staticDir, corsOrigins, gin.H{"status": "ok", "mode": "aggregator"})

	api := r.Group("/api", httpauth.BearerAuth(apiToken))
	{
		api.GET("/nodes", func(c *gin.Context) {
			nodes := a.Nodes()
			if region := c.Query("region"); region != "" {
				filtered := nodes[:0:0]
				for _, n := range nodes {
					if n.Region == region {
						filtered = append(filtered, n)
					}
				}
				nodes = filtered
			}
			c.JSON(http.StatusOK, nodes)
		})

		api.GET("/events", func(c *gin.Context) {
			c.JSON(http.StatusOK, a.Events())
		})

		api.GET("/alerts", func(c *gin.Context) {
			c.JSON(http.StatusOK, a.Alerts())
		})

		api.GET("/regions", func(c *gin.Context) {
			c.JSON(http.StatusOK, a.Regions())
		})
	}

	return r
}
