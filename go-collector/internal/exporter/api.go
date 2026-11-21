package exporter

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
	"github.com/flipslidersand/sentinel-mesh/internal/httpserver"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// internalError logs err server-side and responds 500 with a fixed message so
// store/Badger internals (paths, DB state) are not exposed to clients.
func internalError(c *gin.Context, err error) {
	slog.Error("exporter: internal error", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
}

// parseLimit delegates to the shared httpserver.ParseLimit so the collector
// and aggregator APIs treat the "limit" query identically.
func parseLimit(c *gin.Context, key string, def int) (limit int, ok bool) {
	return httpserver.ParseLimit(c, key, def)
}

// regionNodeSet returns the set of node IDs registered in the given region.
func regionNodeSet(reg NodeLister, region string) map[string]struct{} {
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

// EventLister lists stored events. *store.Store satisfies it.
type EventLister interface {
	ListEvents(ctx context.Context, node string, limit int) ([]model.Event, error)
}

// AlertLister lists stored alerts. *store.Store satisfies it.
type AlertLister interface {
	ListAlerts(ctx context.Context, node string, limit int) ([]model.Alert, error)
}

// StatsProvider returns event counts. *store.Store satisfies it.
type StatsProvider interface {
	Stats() (map[string]int, error)
}

// NodeLister reads the node registry. *registry.Registry satisfies it.
type NodeLister interface {
	List() []registry.AgentNode
	ListByRegion(region string) []registry.AgentNode
	Regions() []string
}

var (
	_ EventLister   = (*store.Store)(nil)
	_ AlertLister   = (*store.Store)(nil)
	_ StatsProvider = (*store.Store)(nil)
	_ NodeLister    = (*registry.Registry)(nil)
)

// RouterConfig carries the dependencies and options for Router.
//
// StaticDir must be an absolute path or relative to the process CWD.
// CORSOrigins restricts cross-origin access; when empty, no cross-origin
// requests are allowed (the UI is served same-origin, so this is the safe
// default). APIToken, when non-empty, gates /api/* and /metrics behind
// bearer auth (/healthz stays open for unauthenticated liveness probes).
// Detector may be nil.
type RouterConfig struct {
	Events      EventLister
	Alerts      AlertLister
	Stats       StatsProvider
	Nodes       NodeLister
	Detector    *anomaly.Detector
	StaticDir   string
	CORSOrigins []string
	APIToken    string
}

// Router builds the gin HTTP router with REST endpoints and static UI.
func Router(cfg RouterConfig) *gin.Engine {
	reg, detector := cfg.Nodes, cfg.Detector
	r := httpserver.NewBaseRouter(cfg.StaticDir, cfg.CORSOrigins, gin.H{"status": "ok"})

	// Gated behind the same bearer auth as /api/* (#177): the exposed
	// Prometheus counters carry node/rule/severity labels (internal/otel),
	// the same category of data /api/stats protects, so leaving /metrics
	// outside the auth group let it bypass that protection entirely.
	// One middleware instance so /metrics and /api/* share a single
	// failed-auth budget per source (#360).
	auth := httpauth.BearerAuth(cfg.APIToken)
	r.GET("/metrics", auth, gin.WrapH(promhttp.Handler()))

	api := r.Group("/api", auth)
	{
		api.GET("/events", func(c *gin.Context) {
			limit, ok := parseLimit(c, "limit", 100)
			if !ok {
				return
			}
			events, err := cfg.Events.ListEvents(c.Request.Context(), c.Query("node"), limit)
			if err != nil {
				internalError(c, err)
				return
			}
			// Optional region filter: keep only events from nodes in the region.
			// Applied post-fetch (region→node mapping lives in the registry).
			if region := c.Query("region"); region != "" {
				set := regionNodeSet(reg, region)
				filtered := make([]model.Event, 0, len(events))
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
			counts, err := cfg.Stats.Stats()
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
			alerts, err := cfg.Alerts.ListAlerts(c.Request.Context(), c.Query("node"), limit)
			if err != nil {
				internalError(c, err)
				return
			}
			// Optional region filter (applied post-fetch, same as /api/events).
			if region := c.Query("region"); region != "" {
				set := regionNodeSet(reg, region)
				filtered := make([]model.Alert, 0, len(alerts))
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
