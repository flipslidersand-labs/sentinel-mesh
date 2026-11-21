// Package api holds the REST contract shared by the per-region collector
// (exporter) and its consumer/re-publisher (aggregator): route paths and the
// region roll-up type. Keeping them in one place makes a change on one side
// a compile error on the other instead of a runtime 404.
package api

// Route paths relative to the server root.
const (
	PathNodes   = "/api/nodes"
	PathEvents  = "/api/events"
	PathAlerts  = "/api/alerts"
	PathRegions = "/api/regions"
)

// RegionSummary is the per-region roll-up returned by GET /api/regions.
type RegionSummary struct {
	Region      string `json:"region"`
	NodeCount   int    `json:"node_count"`
	ActiveCount int    `json:"active_count"`
}
