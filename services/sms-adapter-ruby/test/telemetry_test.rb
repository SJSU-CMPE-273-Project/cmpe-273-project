ENV["RACK_ENV"] = "test"
ENV["PROVIDER_URL"] = "http://provider.test/sms"

require "minitest/autorun"
require "rack/test"
require "webmock/minitest"
require_relative "../lib/telemetry"

# Its own file (and process): tracing is configured once, before the app loads,
# exactly as config.ru does it.
require "opentelemetry/sdk"
EXPORTER = OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
Telemetry.setup!(exporter: EXPORTER)
require_relative "../app"

class TelemetryTest < Minitest::Test
  include Rack::Test::Methods

  def app = SmsAdapter

  TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
  INCOMING = "00-#{TRACE_ID}-00f067aa0ba902b7-01"
  MSG = { event_id: "evt-otel-1", account_id: "acct-1", sequence: 1,
          recipient: "+14085550123", body: "Your code is 482913" }.freeze

  def setup
    EXPORTER.reset
    SmsAdapter.settings.fault.update(mode: "ok")
    @sent_traceparent = nil
    stub_request(:post, "http://provider.test/sms").to_return do |req|
      @sent_traceparent = req.headers["Traceparent"]
      { status: 200, body: '{"provider_message_id":"p-1"}', headers: { "Content-Type" => "application/json" } }
    end
  end

  def teardown
    super
    WebMock.reset!
  end

  def send_msg(msg, env = {})
    post "/send", JSON.generate(msg), { "CONTENT_TYPE" => "application/json" }.merge(env)
  end

  def test_one_trace_from_worker_to_provider
    send_msg(MSG, "HTTP_TRACEPARENT" => INCOMING)
    assert_equal 200, last_response.status

    spans = EXPORTER.finished_spans
    server = spans.find { |s| s.kind == :server }
    client = spans.find { |s| s.kind == :client }
    refute_nil server, "expected a server span for POST /send"
    refute_nil client, "expected a client span for the provider call"

    # Both continue the worker's trace, and the provider call is a child of /send.
    assert_equal [TRACE_ID], spans.map(&:hex_trace_id).uniq
    assert_equal server.span_id, client.parent_span_id
    # The provider (oracle) receives the client span's id, so its span joins too.
    assert_equal "00-#{TRACE_ID}-#{client.hex_span_id}-01", @sent_traceparent

    assert_equal "evt-otel-1", server.attributes["relayguard.event_id"]
    assert_equal "accepted", server.attributes["relayguard.result"]
    assert_equal false, server.attributes["relayguard.retryable"]
  end

  def test_traceparent_in_body_is_continued_when_header_missing
    send_msg(MSG.merge(traceparent: INCOMING))
    client = EXPORTER.finished_spans.find { |s| s.kind == :client }
    assert_equal TRACE_ID, client.hex_trace_id
    assert_includes @sent_traceparent, TRACE_ID
  end

  def test_fault_is_visible_on_the_span
    SmsAdapter.settings.fault.update(mode: "fail")
    send_msg(MSG, "HTTP_TRACEPARENT" => INCOMING)
    server = EXPORTER.finished_spans.find { |s| s.kind == :server }
    assert_equal 503, last_response.status
    assert_equal "unavailable", server.attributes["relayguard.result"]
    assert_equal true, server.attributes["relayguard.retryable"]
  end

  def test_no_pii_in_spans
    send_msg(MSG, "HTTP_TRACEPARENT" => INCOMING)
    dump = EXPORTER.finished_spans.map { |s| [s.name, s.attributes, s.events].inspect }.join
    refute_includes dump, "4085550123"
    refute_includes dump, "482913"
  end

  def test_log_lines_carry_the_trace_id
    out, = capture_io { send_msg(MSG, "HTTP_TRACEPARENT" => INCOMING) }
    assert_includes out, %("trace_id":"#{TRACE_ID}")
  end
end
