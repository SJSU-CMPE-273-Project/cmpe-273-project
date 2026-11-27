// Package telemetry turns on OpenTelemetry tracing for the Go services.
//
// Tracing is off unless OTEL_EXPORTER_OTLP_ENDPOINT is set (for example
// http://jaeger:4318, or the Instana agent's OTLP/HTTP port), so tests and a
// plain `docker compose up` behave exactly as before. OTEL_SDK_DISABLED=true
// also turns it off. OTEL_SERVICE_NAME overrides the service name.
//
// Every RelayGuard event carries the producer's W3C traceparent in its
// payload, so spans continue that trace from the event itself. Nothing is
// added to the event or to Kafka headers, which keeps the bridge's content
// fingerprint and the contract unchanged.
package telemetry

import (
	"context"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Name of the instrumentation scope shown in trace backends.
const scope = "github.com/SJSU-CMPE-273-Project/cmpe-273-project"

// The W3C propagator is used explicitly, not through the global one, so a
// traceparent read from an event means the same thing whether or not
// tracing is on.
var traceContext = propagation.TraceContext{}

// Enabled reports whether tracing is configured by the environment.
func Enabled() bool {
	return strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != "" &&
		!strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true")
}

// Setup installs an OTLP/HTTP exporter when Enabled, and returns a function
// that flushes and stops it. When tracing is off it changes nothing and the
// returned function does nothing.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	if !Enabled() {
		return func(context.Context) error { return nil }, nil
	}
	if name := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); name != "" {
		service = name
	}
	// Reads OTEL_EXPORTER_OTLP_ENDPOINT itself; an http:// endpoint is sent
	// without TLS and /v1/traces is appended.
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", service)))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(traceContext)
	return provider.Shutdown, nil
}

// override replaces the global provider in tests. OpenTelemetry's global
// provider cannot be reset once set, so tests use UseProvider instead.
var override trace.TracerProvider

// Tracer returns the tracer for RelayGuard spans. It is a no-op tracer
// until Setup has installed a provider.
func Tracer() trace.Tracer {
	if override != nil {
		return override.Tracer(scope)
	}
	return otel.Tracer(scope)
}

// UseProvider makes Tracer use provider until the returned function is
// called. It is for tests, which must not change the global provider.
func UseProvider(provider trace.TracerProvider) (restore func()) {
	previous := override
	override = provider
	return func() { override = previous }
}

// FromTraceparent returns ctx with the remote span that traceparent names
// as its parent. An empty or malformed traceparent leaves ctx unchanged, so
// the next span starts a new trace.
func FromTraceparent(ctx context.Context, traceparent string) context.Context {
	if strings.TrimSpace(traceparent) == "" {
		return ctx
	}
	return traceContext.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// Traceparent returns the W3C traceparent of the span in ctx, or "" when
// ctx holds no valid span.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	traceContext.Inject(ctx, carrier)
	return carrier["traceparent"]
}

// EventAttributes are the attributes that let anyone find every span of one
// message by its event_id.
func EventAttributes(eventID, accountID string, sequence uint64) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("relayguard.event_id", eventID),
		attribute.String("relayguard.account_id", accountID),
		attribute.Int64("relayguard.sequence", int64(sequence)),
	}
}
