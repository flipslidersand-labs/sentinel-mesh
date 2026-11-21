package otel

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collectors are registered once on the Prometheus default registry at package
// init, so NewMetricsProvider can be called repeatedly without duplicate
// registration errors.
var (
	eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_events_total",
		Help: "Total number of events received",
	}, []string{"type", "node"})
	alertsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_alerts_total",
		Help: "Total number of alerts triggered",
	}, []string{"rule", "severity", "node"})
	// storeWriteFailuresTotal counts SaveEvent/SaveAlert failures (#103) so
	// persistent storage outages (disk full, corruption, ...) are visible in
	// monitoring instead of being silently swallowed.
	storeWriteFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_store_write_failures_total",
		Help: "Total number of failed event/alert store writes",
	}, []string{"kind"})
	// authFailuresTotal counts rejected authentication attempts (#223) by a
	// fixed, low-cardinality reason. Never labelled by peer IP or token.
	authFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_auth_failures_total",
		Help: "Total number of rejected gRPC authentication attempts",
	}, []string{"reason"})
)

// MetricsProvider records collector metrics on the Prometheus default registry.
type MetricsProvider struct{}

// Fixed reason label values for RecordAuthFailure.
const (
	AuthFailMissingCredentials = "missing_credentials" // no metadata / authorization header
	AuthFailInvalidToken       = "invalid_token"       // Unauthenticated: bad scheme, malformed or unknown/revoked token
	AuthFailRateLimited        = "rate_limited"        // ResourceExhausted: per-source failure budget spent
)

// NewMetricsProvider returns a provider backed by the package-level Prometheus
// counters. The ctx parameter and error result are kept for API compatibility.
func NewMetricsProvider(ctx context.Context) (*MetricsProvider, error) {
	return &MetricsProvider{}, nil
}

// RecordEvent increments the event counter for a given event type and node.
func (mp *MetricsProvider) RecordEvent(eventType, nodeID string) {
	eventsTotal.WithLabelValues(eventType, nodeID).Inc()
}

// RecordAlert increments the alert counter for a given rule and node.
func (mp *MetricsProvider) RecordAlert(ruleID, severity, nodeID string) {
	alertsTotal.WithLabelValues(ruleID, severity, nodeID).Inc()
}

// RecordStoreWriteFailure increments the store write failure counter for a
// given write kind ("event" or "alert").
func (mp *MetricsProvider) RecordStoreWriteFailure(kind string) {
	storeWriteFailuresTotal.WithLabelValues(kind).Inc()
}

// RecordAuthFailure increments the auth failure counter for a fixed reason
// (one of the AuthFail* constants).
func (mp *MetricsProvider) RecordAuthFailure(reason string) {
	authFailuresTotal.WithLabelValues(reason).Inc()
}
