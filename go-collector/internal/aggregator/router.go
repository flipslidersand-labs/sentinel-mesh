package aggregator

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/flipslidersand/sentinel-mesh/internal/api"
	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
	"github.com/flipslidersand/sentinel-mesh/internal/httpserver"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

// defaultLimit matches the collector API default for /api/events and /api/alerts.
const defaultLimit = 100

// Router builds the read-only HTTP router for aggregate mode. It serves the same
// REST shape as a normal collector (nodes / events / alerts / regions) plus the
// static UI, but backed by the aggregator's merged cache instead of a store.
// corsOrigins restricts cross-origin access (empty = deny cross-origin);
// apiToken, when non-empty, gates /api/* behind bearer auth.
func Router(a *Aggregator, staticDir string, corsOrigins []string, apiToken string) *gin.Engine {
	r := httpserver.NewBaseRouter(staticDir, corsOrigins, gin.H{"status": "ok", "mode": "aggregator"})

	rest := r.Group("", httpauth.BearerAuth(apiToken))
	{
		rest.GET(api.PathNodes, func(c *gin.Context) {
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

		rest.GET(api.PathEvents, func(c *gin.Context) {
			limit, ok := httpserver.ParseLimit(c, "limit", defaultLimit)
			if !ok {
				return
			}
			node := c.Query("node")
			out := make([]model.Event, 0)
			region := c.Query("region")
			events := a.EventsIn(region)
			if node != "" || limit > eventLimit {
				// The cache holds only the latest eventLimit events per
				// region: ask the upstreams for the node / larger window.
				events = a.FetchEvents(c.Request.Context(), region, node, limit)
			}
			for _, e := range events {
				if node != "" && e.NodeID != node {
					continue
				}
				if len(out) >= limit {
					break
				}
				out = append(out, e)
			}
			c.JSON(http.StatusOK, out)
		})

		rest.GET(api.PathAlerts, func(c *gin.Context) {
			limit, ok := httpserver.ParseLimit(c, "limit", defaultLimit)
			if !ok {
				return
			}
			node := c.Query("node")
			out := make([]model.Alert, 0)
			region := c.Query("region")
			alerts := a.AlertsIn(region)
			if node != "" || limit > eventLimit {
				// Same as /api/events: the cache holds only the latest
				// eventLimit alerts per region (#553).
				alerts = a.FetchAlerts(c.Request.Context(), region, node, limit)
			}
			for _, al := range alerts {
				if node != "" && al.NodeID != node {
					continue
				}
				if len(out) >= limit {
					break
				}
				out = append(out, al)
			}
			c.JSON(http.StatusOK, out)
		})

		rest.GET("/api/stats", func(c *gin.Context) {
			c.JSON(http.StatusOK, a.Stats())
		})

		rest.GET(api.PathRegions, func(c *gin.Context) {
			c.JSON(http.StatusOK, a.Regions())
		})
	}

	return r
}
