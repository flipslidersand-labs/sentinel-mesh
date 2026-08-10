package exporter

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// Router builds the gin HTTP router with REST endpoints.
func Router(st *store.Store, reg *registry.Registry) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

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
