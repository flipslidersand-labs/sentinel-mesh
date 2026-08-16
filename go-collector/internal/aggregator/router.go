package aggregator

import (
	"net/http"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// Router builds the read-only HTTP router for aggregate mode. It serves the same
// REST shape as a normal collector (nodes / events / alerts / regions) plus the
// static UI, but backed by the aggregator's merged cache instead of a store.
func Router(a *Aggregator, staticDir string, corsOrigins []string) *gin.Engine {
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
		c.JSON(http.StatusOK, gin.H{"status": "ok", "mode": "aggregator"})
	})

	api := r.Group("/api")
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
