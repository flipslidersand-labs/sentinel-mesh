package otel

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
)

// MetricsProvider holds OTel metrics state.
type MetricsProvider struct {
	meter metric.Meter

	// Counters for events and alerts
	eventCounter metric.Int64Counter
	alertCounter metric.Int64Counter
	// storeWriteFailureCounter counts SaveEvent/SaveAlert failures (#103) so
	// persistent storage outages (disk full, corruption, ...) are visible in
	// monitoring instead of being silently swallowed.
	storeWriteFailureCounter metric.Int64Counter
}

// NewMetricsProvider initializes Prometheus-backed OTel metrics.
func NewMetricsProvider(ctx context.Context) (*MetricsProvider, error) {
	exporter, err := promexp.New(
		promexp.WithNamespace("sentinel"),
	)
	if err != nil {
		return nil, err
	}

	provider := metricsdk.NewMeterProvider(
		metricsdk.WithReader(exporter),
	)

	meter := provider.Meter("sentinel-collector")

	eventCounter, err := meter.Int64Counter(
		"events_total",
		metric.WithDescription("Total number of events received"),
	)
	if err != nil {
		return nil, err
	}

	alertCounter, err := meter.Int64Counter(
		"alerts_total",
		metric.WithDescription("Total number of alerts triggered"),
	)
	if err != nil {
		return nil, err
	}

	storeWriteFailureCounter, err := meter.Int64Counter(
		"store_write_failures_total",
		metric.WithDescription("Total number of failed event/alert store writes"),
	)
	if err != nil {
		return nil, err
	}

	return &MetricsProvider{
		meter:                    meter,
		eventCounter:             eventCounter,
		alertCounter:             alertCounter,
		storeWriteFailureCounter: storeWriteFailureCounter,
	}, nil
}

// RecordEvent increments the event counter for a given event type and node.
func (mp *MetricsProvider) RecordEvent(eventType, nodeID string) {
	mp.eventCounter.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("type", eventType),
			attribute.String("node", nodeID),
		),
	)
}

// RecordAlert increments the alert counter for a given rule and node.
func (mp *MetricsProvider) RecordAlert(ruleID, severity, nodeID string) {
	mp.alertCounter.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("rule", ruleID),
			attribute.String("severity", severity),
			attribute.String("node", nodeID),
		),
	)
}

// RecordStoreWriteFailure increments the store write failure counter for a
// given write kind ("event" or "alert").
func (mp *MetricsProvider) RecordStoreWriteFailure(kind string) {
	mp.storeWriteFailureCounter.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("kind", kind),
		),
	)
}

// PrometheusHandler returns the Prometheus HTTP handler for /metrics.
func PrometheusHandler() http.Handler {
	return promhttp.Handler()
}
