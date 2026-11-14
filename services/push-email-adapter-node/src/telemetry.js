'use strict';

// OpenTelemetry tracing for the adapter. Same behaviour as the Ruby adapter's
// lib/telemetry.rb, so one message shows as one trace across both languages.
//
// Off by default. It turns on when OTEL_EXPORTER_OTLP_ENDPOINT is set
// (e.g. http://jaeger:4318), so tests and a plain `docker compose up` behave
// the same as without it. When on:
//   - every incoming request becomes a server span,
//   - the call to the provider becomes a child client span, and the W3C
//     `traceparent` header is added for us, so the oracle's span joins the trace.
// Span attributes are ids and outcomes only. Recipients and message text never
// go into a span (same rule as our logs).
//
// setup() must run before anything requires node:http, because the HTTP
// instrumentation works by patching that module when it is loaded.
// index.js takes care of that ordering.

const api = require('@opentelemetry/api');

function enabled(env = process.env) {
  return (env.OTEL_EXPORTER_OTLP_ENDPOINT || '').trim() !== '' && env.OTEL_SDK_DISABLED !== 'true';
}

// exporter: tests pass an in-memory exporter; normally we send OTLP over HTTP.
function setup({ exporter } = {}) {
  if (!exporter && !enabled()) return false;

  const { TracerProvider, BatchSpanProcessor, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace');
  const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
  const { W3CTraceContextPropagator } = require('@opentelemetry/core');
  const { resourceFromAttributes } = require('@opentelemetry/resources');
  const { registerInstrumentations } = require('@opentelemetry/instrumentation');
  const { HttpInstrumentation } = require('@opentelemetry/instrumentation-http');

  // OpenTelemetry JS 3.x: processors take an options object ({ exporter }).
  // Passing the exporter directly is silently ignored and no span is ever exported.
  let processor;
  if (exporter) {
    processor = new SimpleSpanProcessor({ exporter });
  } else {
    const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
    // reads OTEL_EXPORTER_OTLP_ENDPOINT
    processor = new BatchSpanProcessor({ exporter: new OTLPTraceExporter() });
  }

  const provider = new TracerProvider({
    resource: resourceFromAttributes({ 'service.name': process.env.OTEL_SERVICE_NAME || 'push-email-adapter' }),
    spanProcessors: [processor],
  });
  // Three global pieces: who makes spans, how the current span follows
  // async code (await, callbacks), and how trace ids travel in HTTP headers.
  api.trace.setGlobalTracerProvider(provider);
  api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  api.propagation.setGlobalPropagator(new W3CTraceContextPropagator());

  registerInstrumentations({
    instrumentations: [new HttpInstrumentation({
      // Docker's healthcheck hits /health every 10 s; those spans would bury the real ones.
      ignoreIncomingRequestHook: (req) => req.url === '/health',
    })],
  });

  // Flush buffered spans when the container stops.
  const shutdown = () => provider.shutdown().finally(() => process.exit(0));
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);
  return provider;
}

// Context to run the provider call in. The standard place for the trace id is
// the `traceparent` HTTP header (the instrumentation picks it up). If the worker
// only put it in the JSON body (the contract has a field for it), continue that
// trace instead of starting a new one.
function parentContext(headerValue, bodyValue) {
  if (headerValue || !bodyValue) return api.context.active();
  return api.propagation.extract(api.ROOT_CONTEXT, { traceparent: String(bodyValue) });
}

function annotate(span, attrs) {
  if (!span) return;
  for (const [k, v] of Object.entries(attrs)) {
    if (v !== undefined && v !== null) span.setAttribute(`relayguard.${k}`, v);
  }
}

// For log lines, so a log can be matched to its trace in Jaeger.
function traceId(span) {
  const ctx = span && span.spanContext();
  return ctx && api.isSpanContextValid(ctx) ? ctx.traceId : undefined;
}

module.exports = { enabled, setup, parentContext, annotate, traceId };
