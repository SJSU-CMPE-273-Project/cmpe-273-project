require "opentelemetry" # API only: a no-op until the SDK is configured below

# OpenTelemetry tracing for the adapter.
#
# Off by default. It turns on when OTEL_EXPORTER_OTLP_ENDPOINT is set
# (e.g. http://jaeger:4318), so tests and a plain `docker compose up` behave
# exactly as before. When on:
#   - every request becomes a server span (Rack/Sinatra instrumentation),
#   - the call to the provider becomes a child client span (Net::HTTP),
#     and the W3C `traceparent` header is injected for us, so the oracle's
#     span joins the same trace.
# Span attributes are ids and outcomes only. Phone numbers and message text
# never go into a span (same rule as our logs).
module Telemetry
  module_function

  def enabled?
    !ENV.fetch("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip.empty? && ENV["OTEL_SDK_DISABLED"] != "true"
  end

  # exporter: tests pass an in-memory exporter; normally we send OTLP over HTTP.
  def setup!(exporter: nil)
    return false unless exporter || enabled?

    require "sinatra/base" # must be loaded first, or the Sinatra instrumentation skips itself
    require "net/http"
    require "opentelemetry/sdk"
    require "opentelemetry/instrumentation/sinatra"
    require "opentelemetry/instrumentation/net/http"

    OpenTelemetry::SDK.configure do |c|
      c.service_name = ENV.fetch("OTEL_SERVICE_NAME", "sms-adapter")
      c.use "OpenTelemetry::Instrumentation::Sinatra" # also installs Rack
      c.use "OpenTelemetry::Instrumentation::Net::HTTP"
      if exporter
        c.add_span_processor(OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(exporter))
      else
        require "opentelemetry/exporter/otlp"
        c.add_span_processor(OpenTelemetry::SDK::Trace::Export::BatchSpanProcessor.new(
          OpenTelemetry::Exporter::OTLP::Exporter.new
        ))
      end
    end
    true
  end

  # Context to run the provider call in. The standard place for the trace id
  # is the `traceparent` HTTP header (Rack picks it up automatically). If the
  # worker only put it in the JSON body (the contract has a field for it),
  # continue that trace instead of starting a new one.
  def parent_context(header_value, body_value)
    return OpenTelemetry::Context.current unless header_value.to_s.empty? && !body_value.to_s.empty?

    OpenTelemetry.propagation.extract({ "traceparent" => body_value.to_s })
  end

  def annotate(**attrs)
    span = OpenTelemetry::Trace.current_span
    attrs.each { |k, v| span.set_attribute("relayguard.#{k}", v) unless v.nil? }
  end

  # For log lines, so a log can be matched to its trace in Jaeger.
  def trace_id
    ctx = OpenTelemetry::Trace.current_span.context
    ctx.valid? ? ctx.hex_trace_id : nil
  end
end
