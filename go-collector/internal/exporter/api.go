package exporter

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
	"github.com/flipslidersand/sentinel-mesh/internal/httpserver"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// internalError logs err server-side and responds 500 with a fixed message so
// store/Badger internals (paths, DB state) are not exposed to clients.
func internalError(c *gin.Context, err error) {
	slog.Error("exporter: internal error", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
}

// maxLimit caps the "limit" query parameter accepted by list endpoints,
// preventing a client-supplied value from forcing an unbounded store scan
// or an oversized JSON response.
const maxLimit = 1000

// parseLimit parses the "limit" query parameter for key. When absent, def is
// returned. When present but not a positive integer, it writes a 400
// response and returns ok=false so the caller can abort the request. Values
// above maxLimit are silently clamped rather than rejected.
func parseLimit(c *gin.Context, key string, def int) (limit int, ok bool) {
	v := c.Query(key)
	if v == "" {
		return def, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + key + ": must be a positive integer"})
		return 0, false
	}
	if n > maxLimit {
		n = maxLimit
	}
	return n, true
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
// default). apiToken, when non-empty, gates /api/* and /metrics behind
// bearer auth (/healthz stays open for unauthenticated liveness probes).
func Router(st *store.Store, reg *registry.Registry, detector *anomaly.Detector, staticDir string, corsOrigins []string, apiToken string) *gin.Engine {
	r := httpserver.NewBaseRouter(staticDir, corsOrigins, gin.H{"status": "ok"})

	// Gated behind the same bearer auth as /api/* (#177): the exposed
	// Prometheus counters carry node/rule/severity labels (internal/otel),
	// the same category of data /api/stats protects, so leaving /metrics
	// outside the auth group let it bypass that protection entirely.
	r.GET("/metrics", httpauth.BearerAuth(apiToken), gin.WrapH(promhttp.Handler()))

	api := r.Group("/api", httpauth.BearerAuth(apiToken))
	{
		api.GET("/events", func(c *gin.Context) {
			limit, ok := parseLimit(c, "limit", 100)
			if !ok {
				return
			}
			events, err := st.ListEvents(c.Request.Context(), c.Query("node"), limit)
			if err != nil {
				internalError(c, err)
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
				internalError(c, err)
				return
			}
			c.JSON(http.StatusOK, counts)
		})

		api.GET("/alerts", func(c *gin.Context) {
			limit, ok := parseLimit(c, "limit", 100)
			if !ok {
				return
			}
			alerts, err := st.ListAlerts(c.Request.Context(), c.Query("node"), limit)
			if err != nil {
				internalError(c, err)
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
