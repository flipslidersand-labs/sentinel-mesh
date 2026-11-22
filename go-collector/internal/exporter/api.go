package exporter

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/api"
	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
	"github.com/flipslidersand/sentinel-mesh/internal/httpserver"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// internalError logs err server-side and responds 500 with a fixed message so
// store/Badger internals (paths, DB state) are not exposed to clients. A
// transient store error (busy or closed) is answered 503 instead so the client
// retries (#533).
func internalError(c *gin.Context, err error) {
	if store.IsTransient(err) {
		slog.Warn("exporter: store unavailable", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service unavailable"})
		return
	}
	slog.Error("exporter: internal error", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
}

// parseLimit delegates to the shared httpserver.ParseLimit so the collector
// and aggregator APIs treat the "limit" query identically.
func parseLimit(c *gin.Context, key string, def int) (limit int, ok bool) {
	return httpserver.ParseLimit(c, key, def)
}

// recordFilter builds a record predicate from the "node" and "region" query
// parameters; nil means no filtering. A record carrying its own region is
// matched on that, so records of nodes that left the registry (eviction,
// restart) stay reachable. Records stored before the region field existed fall
// back to the registry's current node set (#512, #531). The store scan cap
// (maxNodeFilterScan) applies: a region whose records are older than that many
// entries may yield fewer than limit results (#324).
func recordFilter[T any](c *gin.Context, reg NodeLister, nodeOf, regionOf func(T) string) func(T) bool {
	node, region := c.Query("node"), c.Query("region")
	if node == "" && region == "" {
		return nil
	}
	var set map[string]struct{}
	if region != "" {
		nodes := reg.ListByRegion(region)
		set = make(map[string]struct{}, len(nodes))
		for _, n := range nodes {
			set[n.NodeID] = struct{}{}
		}
	}
	return func(r T) bool {
		if node != "" && nodeOf(r) != node {
			return false
		}
		if region == "" {
			return true
		}
		if rr := regionOf(r); rr != "" {
			return rr == region
		}
		_, ok := set[nodeOf(r)]
		return ok
	}
}

// eventFilter is recordFilter for events.
func eventFilter(c *gin.Context, reg NodeLister) func(model.Event) bool {
	return recordFilter(c, reg, func(e model.Event) string { return e.NodeID }, func(e model.Event) string { return e.Region })
}

// alertFilter is recordFilter for alerts.
func alertFilter(c *gin.Context, reg NodeLister) func(model.Alert) bool {
	return recordFilter(c, reg, func(a model.Alert) string { return a.NodeID }, func(a model.Alert) string { return a.Region })
}

// EventLister lists stored events. *store.Store satisfies it.
type EventLister interface {
	// ListEventsMatching returns up to limit newest-first events that pass keep
	// (nil keeps all). The store keeps scanning until limit matches are found
	// or its scan cap is hit (#324).
	ListEventsMatching(ctx context.Context, keep func(model.Event) bool, limit int) ([]model.Event, error)
}

// AlertLister lists stored alerts. *store.Store satisfies it.
type AlertLister interface {
	// ListAlertsMatching is the alert counterpart of ListEventsMatching.
	ListAlertsMatching(ctx context.Context, keep func(model.Alert) bool, limit int) ([]model.Alert, error)
}

// StatsProvider returns event counts. *store.Store satisfies it.
type StatsProvider interface {
	Stats() map[string]int
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

	rest := r.Group("", auth)
	{
		rest.GET(api.PathEvents, func(c *gin.Context) {
			limit, ok := parseLimit(c, "limit", 100)
			if !ok {
				return
			}
			events, err := cfg.Events.ListEventsMatching(c.Request.Context(), eventFilter(c, reg), limit)
			if err != nil {
				internalError(c, err)
				return
			}
			c.JSON(http.StatusOK, events)
		})

		rest.GET(api.PathNodes, func(c *gin.Context) {
			if region := c.Query("region"); region != "" {
				c.JSON(http.StatusOK, reg.ListByRegion(region))
				return
			}
			c.JSON(http.StatusOK, reg.List())
		})

		rest.GET(api.PathRegions, func(c *gin.Context) {
			regions := reg.Regions()
			out := make([]api.RegionSummary, 0, len(regions))
			for _, region := range regions {
				nodes := reg.ListByRegion(region)
				out = append(out, api.RegionSummary{
					Region:      region,
					NodeCount:   len(nodes),
					ActiveCount: registry.CountActive(nodes),
				})
			}
			c.JSON(http.StatusOK, out)
		})

		rest.GET("/api/stats", func(c *gin.Context) {
			c.JSON(http.StatusOK, cfg.Stats.Stats())
		})

		rest.GET(api.PathAlerts, func(c *gin.Context) {
			limit, ok := parseLimit(c, "limit", 100)
			if !ok {
				return
			}
			alerts, err := cfg.Alerts.ListAlertsMatching(c.Request.Context(), alertFilter(c, reg), limit)
			if err != nil {
				internalError(c, err)
				return
			}
			c.JSON(http.StatusOK, alerts)
		})

		rest.GET("/api/stats/windows", func(c *gin.Context) {
			if detector == nil {
				c.JSON(http.StatusOK, gin.H{})
				return
			}
			c.JSON(http.StatusOK, detector.WindowStats())
		})
	}

	return r
}
