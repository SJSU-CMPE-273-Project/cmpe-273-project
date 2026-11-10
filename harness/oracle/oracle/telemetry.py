"""OpenTelemetry tracing for the oracle.

Off by default. It turns on when OTEL_EXPORTER_OTLP_ENDPOINT is set
(e.g. http://jaeger:4318), so tests and a plain `docker compose up` are unchanged.

When on, each provider call (POST /sms|/push|/email) is a server span that
continues the adapter's trace via the W3C `traceparent` header. That makes the
oracle the last hop of the trace: "the message reached the phone".
Span attributes are ids and the oracle's verdict only, never the phone number
or the text.
"""
from __future__ import annotations

import os

from opentelemetry import trace


def enabled() -> bool:
    return bool(os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()) and \
        os.environ.get("OTEL_SDK_DISABLED", "").lower() != "true"


def instrument(app, exporter=None) -> bool:
    """Trace the FastAPI app. Tests pass an in-memory exporter."""
    if exporter is None and not enabled():
        return False

    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor

    provider = TracerProvider(resource=Resource.create(
        {"service.name": os.environ.get("OTEL_SERVICE_NAME", "oracle")}))
    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))  # reads OTEL_EXPORTER_OTLP_*
        trace.set_tracer_provider(provider)

    # /health is polled every few seconds by Docker; tracing it would bury the real spans.
    # exclude_spans drops the per-chunk ASGI "http receive/send" spans, which are noise here.
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider, excluded_urls="health",
                                       exclude_spans=["receive", "send"])
    return True


def annotate(**attrs) -> None:
    span = trace.get_current_span()
    for k, v in attrs.items():
        if v is not None:
            span.set_attribute(f"relayguard.{k}", v)


def trace_id() -> str | None:
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else None
