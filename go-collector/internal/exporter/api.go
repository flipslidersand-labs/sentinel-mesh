package exporter

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
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

// regionNodeSet returns the set of node IDs registered in the given region.
func regionNodeSet(reg *registry.Registry, region string) map[string]struct{} {
	nodes := reg.ListByRegion(region)
	set := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		set[n.NodeID] = struct{}{}
	}
	return set
}

// regionSummary is the per-region roll-up returned by GET /api/regions.
type regionSummary struct {
	Region      string `json:"region"`
	NodeCount   int    `json:"node_count"`
	ActiveCount int    `json:"active_count"`
}

// Router builds the gin HTTP router with REST endpoints and static UI.
// staticDir must be an absolute path or relative to the process CWD.
// corsOrigins restricts cross-origin access; when empty, no cross-origin
// requests are allowed (the UI is served same-origin, so this is the safe
// default). apiToken, when non-empty, gates /api/* behind bearer auth.
func Router(st *store.Store, reg *registry.Registry, detector *anomaly.Detector, staticDir string, corsOrigins []string, apiToken string) *gin.Engine {
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

	api := r.Group("/api", httpauth.BearerAuth(apiToken))
	{
		api.GET("/events", func(c *gin.Context) {
			events, err := st.ListEvents(c.Query("node"), queryInt(c, "limit", 100))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			// Optional region filter: keep only events from nodes in the region.
			// Applied post-fetch (region→node mapping lives in the registry).
			if region := c.Query("region"); region != "" {
				set := regionNodeSet(reg, region)
				filtered := make([]store.Event, 0, len(events))
				for _, e := range events {
					if _, ok := set[e.NodeID]; ok {
						filtered = append(filtered, e)
					}
				}
				events = filtered
			}
			c.JSON(http.StatusOK, events)
		})

		api.GET("/nodes", func(c *gin.Context) {
			if region := c.Query("region"); region != "" {
				c.JSON(http.StatusOK, reg.ListByRegion(region))
				return
			}
			c.JSON(http.StatusOK, reg.List())
		})

		api.GET("/regions", func(c *gin.Context) {
			regions := reg.Regions()
			out := make([]regionSummary, 0, len(regions))
			for _, region := range regions {
				nodes := reg.ListByRegion(region)
				active := 0
				for _, n := range nodes {
					if n.Status == "active" {
						active++
					}
				}
				out = append(out, regionSummary{
					Region:      region,
					NodeCount:   len(nodes),
					ActiveCount: active,
				})
			}
			c.JSON(http.StatusOK, out)
		})

		api.GET("/stats", func(c *gin.Context) {
			counts, err := st.Stats()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, counts)
		})

		api.GET("/alerts", func(c *gin.Context) {
			alerts, err := st.ListAlerts(c.Query("node"), queryInt(c, "limit", 100))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			// Optional region filter (applied post-fetch, same as /api/events).
			if region := c.Query("region"); region != "" {
				set := regionNodeSet(reg, region)
				filtered := make([]store.Alert, 0, len(alerts))
				for _, a := range alerts {
					if _, ok := set[a.NodeID]; ok {
						filtered = append(filtered, a)
					}
				}
				alerts = filtered
			}
			c.JSON(http.StatusOK, alerts)
		})

		api.GET("/stats/windows", func(c *gin.Context) {
			if detector == nil {
				c.JSON(http.StatusOK, gin.H{})
				return
			}
			c.JSON(http.StatusOK, detector.WindowStats())
		})
	}

	return r
}
