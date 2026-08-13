package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/trace"
	tracesdk "go.opentelemetry.io/otel/trace"
)

// TracesProvider holds OTel traces state.
type TracesProvider struct {
	tracer tracesdk.Tracer
	closer func(context.Context) error
}

// NewTracesProvider initializes distributed tracing with OTLP HTTP exporter.
// If otelEndpoint is empty, tracing is disabled (no-op).
func NewTracesProvider(ctx context.Context, otelEndpoint string) (*TracesProvider, error) {
	if otelEndpoint == "" {
		return &TracesProvider{
			tracer: otel.Tracer("sentinel-collector"),
			closer: func(context.Context) error { return nil },
		}, nil
	}

	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(otelEndpoint))
	if err != nil {
		return nil, err
	}

	provider := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(provider)

	return &TracesProvider{
		tracer: provider.Tracer("sentinel-collector"),
		closer: provider.Shutdown,
	}, nil
}

// Tracer returns the OTel tracer.
func (tp *TracesProvider) Tracer() tracesdk.Tracer {
	return tp.tracer
}

// Close shuts down the trace provider.
func (tp *TracesProvider) Close(ctx context.Context) error {
	return tp.closer(ctx)
}
