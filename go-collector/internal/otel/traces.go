package otel

import (
	"context"
	"net/url"
	"strings"

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

	exporter, err := otlptracehttp.New(ctx, endpointOption(otelEndpoint))
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

// endpointOption builds the exporter option from the endpoint string.
// A value with a scheme (e.g. http://localhost:4318) is treated as a URL and
// its scheme is honored. A bare host:port keeps the previous behavior
// (WithEndpoint, which defaults to https).
func endpointOption(endpoint string) otlptracehttp.Option {
	if strings.Contains(endpoint, "://") {
		// WithEndpointURL uses the URL path verbatim, so an empty path would
		// post to "/" instead of the OTLP default; fill in /v1/traces.
		if u, err := url.Parse(endpoint); err == nil && (u.Path == "" || u.Path == "/") {
			u.Path = "/v1/traces"
			endpoint = u.String()
		}
		return otlptracehttp.WithEndpointURL(endpoint)
	}
	return otlptracehttp.WithEndpoint(endpoint)
}

// Tracer returns the OTel tracer.
func (tp *TracesProvider) Tracer() tracesdk.Tracer {
	return tp.tracer
}

// Close shuts down the trace provider.
func (tp *TracesProvider) Close(ctx context.Context) error {
	return tp.closer(ctx)
}
