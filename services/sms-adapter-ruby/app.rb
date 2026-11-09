require "json"
require "time"
require "sinatra/base"
require_relative "lib/pii"
require_relative "lib/fault_mode"
require_relative "lib/provider_client"

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
    # We write our own masked logs; Rack's default access log is fine (no bodies),
    # but Sinatra's error pages could echo input, so keep them off.
    set :show_exceptions, false
    set :raise_errors, false
    set :host_authorization, { permitted_hosts: [] } # internal service, any Host header
  end

  before { content_type :json }

  get "/health" do
    json(status: "ok", provider: settings.provider_name,
         simulated: settings.provider.simulated?, fault: settings.fault.to_h)
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

    case settings.fault.pick
    when "fail"
      reply(503, "unavailable", event_id, started, error: "fault injected: fail")
    when "reject"
      reply(422, "rejected", event_id, started, error: "fault injected: reject")
    when "timeout"
      sleep settings.fault.timeout_seconds
      reply(504, "timeout", event_id, started, error: "fault injected: timeout")
    else
      forward(msg, event_id, started)
    end
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
    traceparent = request.env["HTTP_TRACEPARENT"] || msg["traceparent"]
    result = settings.provider.deliver(payload, traceparent: traceparent)

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
    retryable = [503, 504].include?(code)
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
    line = { ts: Time.now.utc.iso8601(3), svc: "sms-adapter", provider: settings.provider_name, event: event }
    $stdout.puts JSON.generate(line.merge(fields.compact))
  end

  def json(obj) = JSON.generate(obj)
  def now_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000).round
end
