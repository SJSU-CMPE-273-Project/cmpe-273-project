ENV["RACK_ENV"] = "test"
require "minitest/autorun"
require "rack/test"
require "webmock/minitest"
require "stringio"
require_relative "../app"

class AppTest < Minitest::Test
  include Rack::Test::Methods

  PROVIDER = "http://oracle.test/sms".freeze
  MSG = {
    event_id: "0b9a6c1e-6f43-4c4a-9a51-2f2d8f8f1c11", account_id: "acct-1001", sequence: 42,
    occurred_at: "2026-10-26T18:05:00Z", recipient: "+14085550123",
    body: "Your RelayGuard code is 482913"
  }.freeze

  def app = SmsAdapter

  def setup
    SmsAdapter.set :provider, ProviderClient.new(url: PROVIDER, read_timeout: 1)
    SmsAdapter.set :fault, FaultMode.new
    SmsAdapter.set :bulkhead, Bulkhead.new(8)
    SmsAdapter.set :admin_token, ""
    @log = StringIO.new
    @real_stdout = $stdout
    $stdout = @log
  end

  def teardown
    $stdout = @real_stdout
    super # webmock resets its request history here
  end

  def send_msg(msg = MSG, headers = {})
    post "/send", JSON.generate(msg), { "CONTENT_TYPE" => "application/json" }.merge(headers)
    JSON.parse(last_response.body)
  end

  def test_health
    get "/health"
    assert_equal 200, last_response.status
    assert_equal "sms_primary", JSON.parse(last_response.body)["provider"]
  end

  def test_happy_path_forwards_with_idempotency_key_and_traceparent
    stub = stub_request(:post, PROVIDER)
           .with(headers: { "Idempotency-Key" => MSG[:event_id], "traceparent" => "00-abc-def-01" },
                 body: hash_including("event_id" => MSG[:event_id], "account_id" => "acct-1001",
                                      "sequence" => 42, "provider" => "sms_primary"))
           .to_return(status: 200, body: '{"provider_message_id":"pm-1"}')

    out = send_msg(MSG, "HTTP_TRACEPARENT" => "00-abc-def-01")

    assert_equal 200, last_response.status
    assert_equal "accepted", out["status"]
    assert_equal "pm-1", out["provider_message_id"]
    assert_requested stub
  end

  def test_logs_never_contain_raw_phone_or_body
    stub_request(:post, PROVIDER).to_return(status: 200, body: "{}")
    send_msg
    refute_includes @log.string, "+14085550123"
    refute_includes @log.string, "482913"
    assert_includes @log.string, "***0123"
  end

  def test_missing_fields_is_400_not_retryable
    out = send_msg({ event_id: "e1" })
    assert_equal 400, last_response.status
    assert_equal false, out["retryable"]
  end

  def test_invalid_json_is_400
    post "/send", "not json", { "CONTENT_TYPE" => "application/json" }
    assert_equal 400, last_response.status
  end

  def test_provider_4xx_is_422_not_retryable
    stub_request(:post, PROVIDER).to_return(status: 400)
    out = send_msg
    assert_equal 422, last_response.status
    assert_equal false, out["retryable"]
  end

  def test_provider_5xx_is_503_retryable
    stub_request(:post, PROVIDER).to_return(status: 500)
    out = send_msg
    assert_equal 503, last_response.status
    assert_equal true, out["retryable"]
  end

  def test_provider_down_is_503
    stub_request(:post, PROVIDER).to_raise(Errno::ECONNREFUSED)
    send_msg
    assert_equal 503, last_response.status
  end

  def test_provider_timeout_is_504
    stub_request(:post, PROVIDER).to_timeout
    out = send_msg
    assert_equal 504, last_response.status
    assert_equal true, out["retryable"]
  end

  def test_fault_modes_do_not_call_provider
    { "fail" => 503, "reject" => 422, "timeout" => 504 }.each do |mode, code|
      put "/admin/fault", JSON.generate(mode: mode, timeout_seconds: 0)
      assert_equal 200, last_response.status
      send_msg
      assert_equal code, last_response.status, "mode #{mode}"
    end
    assert_not_requested :post, PROVIDER
  end

  def test_bad_fault_mode_is_400
    put "/admin/fault", JSON.generate(mode: "explode")
    assert_equal 400, last_response.status
  end

  def test_admin_requires_token_when_set
    SmsAdapter.set :admin_token, "s3cret"
    put "/admin/fault", JSON.generate(mode: "fail")
    assert_equal 401, last_response.status
    put "/admin/fault", JSON.generate(mode: "fail"), "HTTP_AUTHORIZATION" => "Bearer s3cret"
    assert_equal 200, last_response.status
  end

  def test_simulated_mode_accepts_without_provider
    SmsAdapter.set :provider, ProviderClient.new(url: "")
    out = send_msg
    assert_equal 200, last_response.status
    assert_match(/\Asim-/, out["provider_message_id"])
  end

  # ---- bulkhead + back pressure ---------------------------------------------

  def test_full_bulkhead_answers_429_with_retry_after
    stub_request(:post, PROVIDER).to_return(status: 200, body: '{"provider_message_id":"p-1"}')
    SmsAdapter.set :bulkhead, Bulkhead.new(1)
    gate = Queue.new
    holder = Thread.new { SmsAdapter.settings.bulkhead.try_run { gate.pop } } # a slow call holds the only slot
    sleep 0.01 until SmsAdapter.settings.bulkhead.in_flight == 1

    out = send_msg
    assert_equal 429, last_response.status
    assert_equal "1", last_response.headers["Retry-After"]
    assert_equal({ "status" => "busy", "retryable" => true }, out.slice("status", "retryable"))
    assert_requested :post, PROVIDER, times: 0 # refused before touching the provider

    gate << :done
    holder.join
    assert_equal 200, (send_msg && last_response.status) # slot freed, back to normal
  end

  def test_slow_provider_only_uses_its_slots_and_health_stays_up
    SmsAdapter.set :bulkhead, Bulkhead.new(2)
    SmsAdapter.settings.fault.update(mode: "timeout", timeout_seconds: 0.5)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    results = Array.new(5) do
      Thread.new do
        res = Rack::MockRequest.new(SmsAdapter).post("/send", input: JSON.generate(MSG),
                                                      "CONTENT_TYPE" => "application/json")
        [res.status, Process.clock_gettime(Process::CLOCK_MONOTONIC) - started]
      end
    end
    sleep 0.1
    health = Rack::MockRequest.new(SmsAdapter).get("/health")
    results = results.map(&:value)

    assert_equal [429, 429, 429, 504, 504], results.map(&:first).sort
    assert results.select { |code, _| code == 429 }.all? { |_, secs| secs < 0.3 }, "429s must be immediate"
    assert_equal 200, health.status
    bh = JSON.parse(health.body)["bulkhead"]
    assert_equal [2, 2], [bh["max_in_flight"], bh["in_flight"]] # answered while both slots were busy
    assert_equal 3, SmsAdapter.settings.bulkhead.rejected
  end
end
