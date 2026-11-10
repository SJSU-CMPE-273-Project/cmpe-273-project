require "minitest/autorun"
require_relative "../lib/pii"
require_relative "../lib/fault_mode"

class PIITest < Minitest::Test
  def test_phone_keeps_last_four
    assert_equal "***0123", PII.mask_recipient("+14085550123")
  end

  def test_email_keeps_first_letter_and_domain
    assert_equal "p***@example.com", PII.mask_recipient("pooja@example.com")
  end

  def test_body_shows_only_length
    assert_equal "[redacted len=6]", PII.mask_body("482913")
  end
end

class FaultModeTest < Minitest::Test
  def test_flaky_uses_fail_rate
    always = FaultMode.new(mode: "flaky", fail_rate: 1.0)
    never = FaultMode.new(mode: "flaky", fail_rate: 0.0)
    assert_equal "fail", always.pick
    assert_nil never.pick
  end

  def test_rejects_unknown_mode_and_bad_rate
    fm = FaultMode.new
    assert_raises(ArgumentError) { fm.update(mode: "nope") }
    assert_raises(ArgumentError) { fm.update(fail_rate: 2) }
    assert_equal "ok", fm.mode
  end
end
