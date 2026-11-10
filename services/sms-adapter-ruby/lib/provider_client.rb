require "json"
require "net/http"
require "securerandom"
require "uri"

# Forwards one message to the downstream "provider" (the harness oracle / fake phone).
#
# Returns a Result whose `kind` tells the caller how to answer the worker:
#   :accepted    -> 200   (provider took it)
#   :rejected    -> 422   (provider said a permanent no, e.g. 4xx)
#   :unavailable -> 503   (5xx, refused connection: try again later)
#   :timeout     -> 504   (provider too slow: try again later)
class ProviderClient
  Result = Struct.new(:kind, :provider_message_id, :error, keyword_init: true)

  def initialize(url:, open_timeout: 2, read_timeout: 5)
    @uri = url.to_s.empty? ? nil : URI(url)
    @open_timeout = open_timeout
    @read_timeout = read_timeout
  end

  # No PROVIDER_URL means "simulate": accept without calling anyone. That lets the
  # Nov 11 happy path run before the oracle exists.
  def simulated?
    @uri.nil?
  end

  def deliver(payload, traceparent: nil)
    if simulated?
      return Result.new(kind: :accepted, provider_message_id: "sim-#{SecureRandom.uuid}")
    end

    req = Net::HTTP::Post.new(@uri)
    req["Content-Type"] = "application/json"
    # event_id doubles as the provider idempotency key, so if the worker retries
    # after a lost ack, the provider (oracle) can tell it's the same message.
    req["Idempotency-Key"] = payload[:event_id]
    req["traceparent"] = traceparent if traceparent && !traceparent.empty?
    req.body = JSON.generate(payload)

    res = Net::HTTP.start(@uri.host, @uri.port, use_ssl: @uri.scheme == "https",
                          open_timeout: @open_timeout, read_timeout: @read_timeout) do |http|
      http.request(req)
    end

    code = res.code.to_i
    if code.between?(200, 299)
      body = parse(res.body)
      Result.new(kind: :accepted, provider_message_id: body["provider_message_id"] || body["id"])
    elsif code.between?(400, 499)
      Result.new(kind: :rejected, error: "provider returned #{code}")
    else
      Result.new(kind: :unavailable, error: "provider returned #{code}")
    end
  rescue Net::OpenTimeout, Net::ReadTimeout => e
    Result.new(kind: :timeout, error: "#{e.class}: provider timed out")
  rescue SystemCallError, IOError, SocketError => e
    Result.new(kind: :unavailable, error: "#{e.class}: #{e.message}")
  end

  private

  def parse(body)
    JSON.parse(body.to_s)
  rescue JSON::ParserError
    {}
  end
end
