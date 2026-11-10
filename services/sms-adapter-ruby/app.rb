require "json"
require "time"
require "sinatra/base"
require_relative "lib/pii"
require_relative "lib/fault_mode"
require_relative "lib/bulkhead"
require_relative "lib/provider_client"
require_relative "lib/telemetry"

# SMS adapter: the delivery worker POSTs one message here; we hand it to the
# SMS "provider" and translate the outcome into a status code the worker can act on.
#
# The same image runs twice (PROVIDER_NAME=sms_primary / sms_secondary) so the
# worker has somewhere to fail over to.
class SmsAdapter < Sinatra::Base
  REQUIRED = %w[event_id recipient body].freeze

  configure do
    set :provider_name, ENV.fetch("PROVIDER_NAME", "sms_primary")
    set :admin_token, ENV.fetch("ADMIN_TOKEN", "")
    set :provider, ProviderClient.new(
      url: ENV.fetch("PROVIDER_URL", ""),
      open_timeout: Float(ENV.fetch("PROVIDER_OPEN_TIMEOUT", "2")),
      read_timeout: Float(ENV.fetch("PROVIDER_READ_TIMEOUT", "5"))
    )
    set :fault, FaultMode.new(
      mode: ENV.fetch("FAULT_MODE", "ok"),
      fail_rate: ENV.fetch("FAIL_RATE", "0"),
      timeout_seconds: ENV.fetch("TIMEOUT_SECONDS", "30")
    )
    # Bulkhead: at most MAX_IN_FLIGHT provider calls at once. Kept below Puma's
    # thread count (8-16) so /health and /admin still answer when the provider
    # is slow. 0 turns it off.
    set :bulkhead, Bulkhead.new(ENV.fetch("MAX_IN_FLIGHT", "8"))
    set :retry_after, Integer(ENV.fetch("RETRY_AFTER_SECONDS", "1"))
    # We write our own masked logs; Rack's default access log is fine (no bodies),
    # but Sinatra's error pages could echo input, so keep them off.
    set :show_exceptions, false
    set :raise_errors, false
    set :host_authorization, { permitted_hosts: [] } # internal service, any Host header
  end

  before { content_type :json }

  get "/health" do
    json(status: "ok", provider: settings.provider_name,
         simulated: settings.provider.simulated?, fault: settings.fault.to_h,
         bulkhead: settings.bulkhead.to_h)
  end

  post "/send" do
    msg = parse_body
    missing = REQUIRED.select { |k| msg[k].to_s.strip.empty? }
    unless missing.empty?
      log("send.invalid", event_id: msg["event_id"], missing: missing)
      halt 400, json(status: "invalid", error: "missing fields: #{missing.join(', ')}", retryable: false)
    end

    event_id = msg["event_id"]
    started = now_ms

    # The block always ends in reply/halt, so try_run only returns (false)
    # when the bulkhead was full and the block never ran.
    admitted = settings.bulkhead.try_run { handle(msg, event_id, started) }
    busy(event_id, started) unless admitted
  end

  # ---- chaos control (used by harness/chaos scripts) ----------------------

  get "/admin/fault" do
    require_admin!
    json(settings.fault.to_h)
  end

  put "/admin/fault" do
    require_admin!
    body = parse_body
    settings.fault.update(mode: body["mode"], fail_rate: body["fail_rate"],
                          timeout_seconds: body["timeout_seconds"])
    log("fault.changed", **settings.fault.to_h)
    json(settings.fault.to_h)
  rescue ArgumentError, TypeError => e
    halt 400, json(error: e.message)
  end

  error do
    log("error", error: env["sinatra.error"].class.name)
    json(status: "error", error: "internal error", retryable: true)
  end

  private

  def handle(msg, event_id, started)
    case settings.fault.pick
    when "fail"
      reply(503, "unavailable", event_id, started, error: "fault injected: fail")
    when "reject"
      reply(422, "rejected", event_id, started, error: "fault injected: reject")
    when "timeout"
      # Holds a bulkhead slot while it hangs, like a real slow provider would.
      sleep settings.fault.timeout_seconds
      reply(504, "timeout", event_id, started, error: "fault injected: timeout")
    else
      forward(msg, event_id, started)
    end
  end

  # Back pressure: answer at once instead of queueing. The worker retries
  # after Retry-After (same event_id, so no duplicate), or fails over.
  def busy(event_id, started)
    headers "Retry-After" => settings.retry_after.to_s
    reply(429, "busy", event_id, started,
          error: "adapter at capacity (#{settings.bulkhead.max} calls in flight)")
  end

  def forward(msg, event_id, started)
    # Pass through the fields the oracle needs for its checks (order, latency).
    payload = {
      event_id: event_id,
      account_id: msg["account_id"],
      sequence: msg["sequence"],
      occurred_at: msg["occurred_at"],
      recipient: msg["recipient"],
      body: msg["body"],
      provider: settings.provider_name,
      adapter_received_at: Time.now.utc.iso8601(3)
    }
    header_tp = request.env["HTTP_TRACEPARENT"]
    traceparent = header_tp || msg["traceparent"]
    # With tracing on, Net::HTTP instrumentation replaces this header with the
    # client span's own traceparent (a child of ours). With it off, we pass the
    # incoming one through unchanged.
    result = OpenTelemetry::Context.with_current(Telemetry.parent_context(header_tp, msg["traceparent"])) do
      settings.provider.deliver(payload, traceparent: traceparent)
    end

    case result.kind
    when :accepted
      reply(200, "accepted", event_id, started, provider_message_id: result.provider_message_id,
            recipient: msg["recipient"], body: msg["body"])
    when :rejected then reply(422, "rejected", event_id, started, error: result.error)
    when :timeout then reply(504, "timeout", event_id, started, error: result.error)
    else reply(503, "unavailable", event_id, started, error: result.error)
    end
  end

  # One place builds both the HTTP answer and the (masked) log line.
  def reply(code, status, event_id, started, error: nil, provider_message_id: nil, recipient: nil, body: nil)
    retryable = [429, 503, 504].include?(code)
    Telemetry.annotate(event_id: event_id, provider: settings.provider_name, result: status, retryable: retryable)
    log("send.#{status}", event_id: event_id, code: code, ms: now_ms - started, error: error,
        recipient: recipient && PII.mask_recipient(recipient), body: body && PII.mask_body(body))
    out = { status: status, event_id: event_id, provider: settings.provider_name, retryable: retryable }
    out[:provider_message_id] = provider_message_id if provider_message_id
    out[:error] = error if error
    halt code, json(out)
  end

  def require_admin!
    token = settings.admin_token
    return if token.empty? # local dev: open. Set ADMIN_TOKEN when deployed.

    given = request.env["HTTP_AUTHORIZATION"].to_s.delete_prefix("Bearer ")
    halt 401, json(error: "unauthorized") unless Rack::Utils.secure_compare(given, token)
  end

  def parse_body
    # Rewind: if a client forgets Content-Type: application/json, Rack has
    # already read the body as a form, leaving the stream at the end.
    request.body.rewind
    data = JSON.parse(request.body.read)
    halt 400, json(status: "invalid", error: "body must be a JSON object", retryable: false) unless data.is_a?(Hash)
    data
  rescue JSON::ParserError
    halt 400, json(status: "invalid", error: "body is not valid JSON", retryable: false)
  end

  def log(event, **fields)
    line = { ts: Time.now.utc.iso8601(3), svc: "sms-adapter", provider: settings.provider_name, event: event,
             trace_id: Telemetry.trace_id }.compact
    $stdout.puts JSON.generate(line.merge(fields.compact))
  end

  def json(obj) = JSON.generate(obj)
  def now_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000).round
end
