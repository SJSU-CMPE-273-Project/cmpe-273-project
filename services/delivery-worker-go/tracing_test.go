package delivery

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/SJSU-CMPE-273-Project/cmpe-273-project/internal/telemetry"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	recorder := tracetest.NewSpanRecorder()
	t.Cleanup(telemetry.UseProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))))
	return recorder
}

func attrs(kvs []attribute.KeyValue) map[string]attribute.Value {
	m := map[string]attribute.Value{}
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value
	}
	return m
}

// With tracing on, each attempt is a span in the event's trace, and the
// adapter gets that span's traceparent so its own spans nest under it.
func TestAttemptSpanContinuesEventTrace(t *testing.T) {
	recorder := recordSpans(t)
	e := event(7)
	var sent string
	a := Adapters{Client: &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		sent = r.Header.Get("traceparent")
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"event_id":%q,"status":"accepted","provider_message_id":"p-1"}`, e.EventId)))}, nil
	})}}
	r, err := a.Send(context.Background(), Attempt{e, Target{Provider: "sms_primary"}})
	require.NoError(t, err)
	require.True(t, r.Accepted)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	require.Equal(t, "delivery.attempt sms_primary", span.Name())
	require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", span.SpanContext().TraceID().String())
	require.Equal(t, "00f067aa0ba902b7", span.Parent().SpanID().String())
	require.Equal(t, fmt.Sprintf("00-%s-%s-01", span.SpanContext().TraceID(), span.SpanContext().SpanID()), sent)
	got := attrs(span.Attributes())
	require.Equal(t, e.EventId, got["relayguard.event_id"].AsString())
	require.Equal(t, "sms_primary", got["relayguard.provider"].AsString())
	require.True(t, got["relayguard.accepted"].AsBool())
	require.Equal(t, codes.Unset, span.Status().Code)
}

// A failed attempt is marked as an error with its result code, so a retry
// or failover is easy to spot in the trace.
func TestFailedAttemptSpanIsAnError(t *testing.T) {
	recorder := recordSpans(t)
	a := Adapters{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}}
	r, err := a.Send(context.Background(), Attempt{event(8), Target{Provider: "sms_primary"}})
	require.NoError(t, err)
	require.True(t, r.Retryable)
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, codes.Error, spans[0].Status().Code)
	require.Equal(t, "unavailable", spans[0].Status().Description)
}

// With tracing off the adapter gets the event's traceparent unchanged, as
// before tracing existed.
func TestTraceparentUnchangedWhenTracingOff(t *testing.T) {
	e := event(9)
	var sent string
	a := Adapters{Client: &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		sent = r.Header.Get("traceparent")
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"event_id":%q,"status":"accepted","provider_message_id":"p-1"}`, e.EventId)))}, nil
	})}}
	_, err := a.Send(context.Background(), Attempt{e, Target{Provider: "sms_primary"}})
	require.NoError(t, err)
	require.Equal(t, e.Traceparent, sent)
}
