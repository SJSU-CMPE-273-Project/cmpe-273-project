require "minitest/autorun"
require_relative "../lib/pii"
require_relative "../lib/fault_mode"
require_relative "../lib/bulkhead"

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


class BulkheadTest < Minitest::Test
  def test_refuses_when_full_and_frees_the_slot_after
    bh = Bulkhead.new(1)
    inner = nil
    outer = bh.try_run { inner = bh.try_run { flunk "must not run while full" } }
    assert_equal true, outer
    assert_equal false, inner
    assert_equal 0, bh.in_flight
    assert_equal 1, bh.rejected
    assert_equal true, bh.try_run { :ok }
  end

  def test_slot_is_released_when_the_block_throws
    bh = Bulkhead.new(1)
    catch(:halt) { bh.try_run { throw :halt } } # what Sinatra's halt does
    assert_raises(RuntimeError) { bh.try_run { raise "boom" } }
    assert_equal 0, bh.in_flight
  end

  def test_zero_means_unlimited
    bh = Bulkhead.new(0)
    refute bh.enabled?
    assert_equal true, bh.try_run { bh.try_run { :nested } }
  end
end
