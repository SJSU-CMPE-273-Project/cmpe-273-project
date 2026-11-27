package delivery

import (
	"context"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/SJSU-CMPE-273-Project/cmpe-273-project/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/activity"
)

// startAttemptSpan starts one span per delivery attempt (each activity
// execution, so each retry and each failover target gets its own span),
// continuing the producer's trace from the event's traceparent. The adapter
// call is sent with this span's traceparent, so the adapter's and the
// provider's spans become its children.
func startAttemptSpan(ctx context.Context, a Attempt) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String("relayguard.provider", a.Target.Provider),
		attribute.String("relayguard.channel", a.Target.Channel.String()),
	}
	if activity.IsActivity(ctx) {
		attrs = append(attrs, attribute.Int64("temporal.activity.attempt", int64(activity.GetInfo(ctx).Attempt)))
	}
	if e := a.Event; e != nil {
		ctx = telemetry.FromTraceparent(ctx, e.Traceparent)
		attrs = append(attrs, telemetry.EventAttributes(e.EventId, e.AccountId, e.Sequence)...)
	}
	return telemetry.Tracer().Start(ctx, "delivery.attempt "+a.Target.Provider,
		trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
}

// endAttemptSpan records how the attempt ended and ends the span.
func endAttemptSpan(span trace.Span, r Result, err error) {
	span.SetAttributes(
		attribute.String("relayguard.result", resultCode(r)),
		attribute.Bool("relayguard.accepted", r.Accepted),
		attribute.Bool("relayguard.retryable", r.Retryable),
		attribute.Bool("relayguard.ambiguous", r.Ambiguous),
	)
	switch {
	case err != nil:
		span.SetStatus(codes.Error, "activity_error")
	case !r.Accepted && !r.Expired:
		// A failed attempt is an error on this span even when the workflow
		// will retry it or fail over: that is what an operator looks for.
		span.SetStatus(codes.Error, resultCode(r))
	}
	span.End()
}

// startEventSpan starts a span for a ledger step on one event, such as
// dead-lettering or expiring it, in the event's trace.
func startEventSpan(ctx context.Context, name string, e *pb.NotificationRequested) (context.Context, trace.Span) {
	if e == nil {
		return telemetry.Tracer().Start(ctx, name)
	}
	return telemetry.Tracer().Start(telemetry.FromTraceparent(ctx, e.Traceparent), name,
		trace.WithAttributes(telemetry.EventAttributes(e.EventId, e.AccountId, e.Sequence)...))
}

// outgoingTraceparent is the traceparent to send to the adapter: the
// attempt span's own when tracing is on, otherwise the event's unchanged.
func outgoingTraceparent(ctx context.Context, e *pb.NotificationRequested) string {
	if tp := telemetry.Traceparent(ctx); tp != "" {
		return tp
	}
	return e.Traceparent
}
