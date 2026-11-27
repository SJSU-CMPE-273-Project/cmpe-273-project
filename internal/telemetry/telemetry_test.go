package telemetry

import (
	"context"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

func TestOffByDefault(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	if Enabled() {
		t.Fatal("tracing must be off without an endpoint")
	}
	shutdown, err := Setup(context.Background(), "test")
	if err != nil || shutdown(context.Background()) != nil {
		t.Fatalf("disabled setup must be a no-op, got %v", err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	if Enabled() {
		t.Fatal("OTEL_SDK_DISABLED=true must turn tracing off")
	}
}

func TestTraceparentPassesThroughWhenOff(t *testing.T) {
	// With no SDK installed, a span started under the event's traceparent
	// carries that same traceparent onward, as the worker did before tracing.
	ctx := FromTraceparent(context.Background(), parent)
	ctx, span := Tracer().Start(ctx, "noop")
	defer span.End()
	if got := Traceparent(ctx); got != parent {
		t.Fatalf("got %q, want the event's traceparent %q", got, parent)
	}
}

func TestSpanContinuesTheEventTrace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	t.Cleanup(UseProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))))

	ctx, span := Tracer().Start(FromTraceparent(context.Background(), parent), "attempt")
	child := Traceparent(ctx)
	span.End()

	if !strings.HasPrefix(child, "00-0af7651916cd43dd8448eb211c80319c-") {
		t.Fatalf("child %q must keep the event's trace id", child)
	}
	if child == parent {
		t.Fatal("the outgoing traceparent must name the new span, not the parent")
	}
	ended := recorder.Ended()
	if len(ended) != 1 || ended[0].Parent().SpanID().String() != "b7ad6b7169203331" {
		t.Fatalf("span must be a child of the event's span, got %+v", ended)
	}
}

func TestEmptyTraceparentStartsANewTrace(t *testing.T) {
	ctx := FromTraceparent(context.Background(), "")
	if Traceparent(ctx) != "" {
		t.Fatal("no traceparent must mean no parent")
	}
}
