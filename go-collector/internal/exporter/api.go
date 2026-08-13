package exporter

import (
	"net/http"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// Router builds the gin HTTP router with REST endpoints and static UI.
func Router(st *store.Store, reg *registry.Registry) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors.Default())

	// Serve the React SPA from go-collector/static/
	r.Static("/assets", "./static/assets")
	r.StaticFile("/", "./static/index.html")
	r.StaticFile("/favicon.svg", "./static/favicon.svg")
	r.NoRoute(func(c *gin.Context) {
		// SPA fallback
		c.File("./static/index.html")
	})

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Prometheus metrics endpoint
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	api := r.Group("/api")
	{
		// GET /api/events?node=xxx&limit=N
		api.GET("/events", func(c *gin.Context) {
			limit := 100
			if l := c.Query("limit"); l != "" {
				if n, err := strconv.Atoi(l); err == nil && n > 0 {
					limit = n
				}
			}
			node := c.Query("node")
			events, err := st.ListEvents(node, limit)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, events)
		})

		// GET /api/nodes
		api.GET("/nodes", func(c *gin.Context) {
			c.JSON(http.StatusOK, reg.List())
		})

		// GET /api/stats
		api.GET("/stats", func(c *gin.Context) {
			counts, err := st.Stats()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, counts)
		})

		// GET /api/alerts?node=xxx&limit=N
		api.GET("/alerts", func(c *gin.Context) {
			limit := 100
			if l := c.Query("limit"); l != "" {
				if n, err := strconv.Atoi(l); err == nil && n > 0 {
					limit = n
				}
			}
			node := c.Query("node")
			alerts, err := st.ListAlerts(node, limit)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, alerts)
		})
	}

	return r
}
