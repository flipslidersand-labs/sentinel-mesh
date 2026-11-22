package otel

import (
	"context"
	"sync"

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
	// eventsThrottledTotal counts events dropped by the per-node event rate
	// limit (#420). Deliberately unlabelled: node_id would be high cardinality.
	eventsThrottledTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sentinel_events_throttled_total",
		Help: "Total number of events dropped by the per-node rate limit",
	})
)

// MaxNodeLabels is the number of distinct node_id values kept as their own
// "node" label value (#326). Without authentication node_id is client
// controlled, so later unknown IDs are folded into OtherNodeLabel to keep the
// series count bounded.
const MaxNodeLabels = 256

// OtherNodeLabel is the node label value used once MaxNodeLabels is reached.
const OtherNodeLabel = "other"

// nodeLabeler is a capped allowlist of node label values. The first max
// distinct IDs map to themselves; later unseen IDs map to OtherNodeLabel.
// A given ID always maps to the same label.
type nodeLabeler struct {
	mu   sync.Mutex
	max  int
	seen map[string]struct{}
}

func newNodeLabeler(max int) *nodeLabeler {
	return &nodeLabeler{max: max, seen: make(map[string]struct{})}
}

func (l *nodeLabeler) label(nodeID string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[nodeID]; ok {
		return nodeID
	}
	if len(l.seen) >= l.max {
		return OtherNodeLabel
	}
	l.seen[nodeID] = struct{}{}
	return nodeID
}

// MetricsProvider records collector metrics on the Prometheus default registry.
type MetricsProvider struct {
	nodes *nodeLabeler
}

// Fixed reason label values for RecordAuthFailure.
const (
	AuthFailMissingCredentials = "missing_credentials" // no metadata / authorization header
	AuthFailInvalidToken       = "invalid_token"       // Unauthenticated: bad scheme, malformed or unknown/revoked token
	AuthFailRateLimited        = "rate_limited"        // ResourceExhausted: per-source failure budget spent
)

// NewMetricsProvider returns a provider backed by the package-level Prometheus
// counters. The ctx parameter and error result are kept for API compatibility.
func NewMetricsProvider(ctx context.Context) (*MetricsProvider, error) {
	return &MetricsProvider{nodes: newNodeLabeler(MaxNodeLabels)}, nil
}

// RecordEvent increments the event counter for a given event type and node.
func (mp *MetricsProvider) RecordEvent(eventType, nodeID string) {
	eventsTotal.WithLabelValues(eventType, mp.nodes.label(nodeID)).Inc()
}

// RecordAlert increments the alert counter for a given rule and node.
func (mp *MetricsProvider) RecordAlert(ruleID, severity, nodeID string) {
	alertsTotal.WithLabelValues(ruleID, severity, mp.nodes.label(nodeID)).Inc()
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

// RecordEventThrottled increments the counter of events dropped by the rate limit.
func (mp *MetricsProvider) RecordEventThrottled() {
	eventsThrottledTotal.Inc()
}
