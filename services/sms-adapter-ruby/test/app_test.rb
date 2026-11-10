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
end
