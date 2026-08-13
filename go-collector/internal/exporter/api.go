package exporter

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func queryInt(c *gin.Context, key string, def int) int {
	if v := c.Query(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// Router builds the gin HTTP router with REST endpoints and static UI.
// staticDir must be an absolute path or relative to the process CWD.
func Router(st *store.Store, reg *registry.Registry, det *anomaly.Detector, staticDir string, corsOrigins []string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	corsConfig := cors.DefaultConfig()
	if len(corsOrigins) > 0 {
		corsConfig.AllowOrigins = corsOrigins
	} else {
		corsConfig.AllowAllOrigins = true
	}
	r.Use(cors.New(corsConfig))

	// Serve the React SPA
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
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	api := r.Group("/api")
	{
		api.GET("/events", func(c *gin.Context) {
			events, err := st.ListEvents(c.Query("node"), queryInt(c, "limit", 100))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, events)
		})

		api.GET("/nodes", func(c *gin.Context) {
			c.JSON(http.StatusOK, reg.List())
		})

		api.GET("/stats", func(c *gin.Context) {
			counts, err := st.Stats()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, counts)
		})

		api.GET("/stats/windows", func(c *gin.Context) {
			c.JSON(http.StatusOK, det.Stats())
		})

		api.GET("/alerts", func(c *gin.Context) {
			alerts, err := st.ListAlerts(c.Query("node"), queryInt(c, "limit", 100))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, alerts)
		})
	}

	return r
}
