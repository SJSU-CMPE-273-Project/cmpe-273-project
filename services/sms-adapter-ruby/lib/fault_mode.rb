# Application-level faults the chaos harness can switch on at runtime.
#
# Toxiproxy already breaks the *network* (down, latency, cut connections).
# These modes break the *provider's answer*, which the network can't fake:
#   ok      - accept every message
#   fail    - 503, a temporary outage: the worker should retry, then fail over
#   timeout - hang, then 504: the worker's client timeout should fire first
#   reject  - 422, a permanent "no": retrying is pointless, go to the next channel / DLQ
#   flaky   - fail randomly with probability fail_rate (exercises retry + backoff)
#
# State lives in memory on purpose: the adapter is stateless (ephemeral storage
# only), and a restart resetting to the env default is the behaviour we want.
class FaultMode
  MODES = %w[ok fail timeout reject flaky].freeze

  attr_reader :mode, :fail_rate, :timeout_seconds

  def initialize(mode: "ok", fail_rate: 0.0, timeout_seconds: 30, rng: Random.new)
    @lock = Mutex.new # Puma runs requests on several threads
    @rng = rng
    update(mode: mode, fail_rate: fail_rate, timeout_seconds: timeout_seconds)
  end

  def update(mode: nil, fail_rate: nil, timeout_seconds: nil)
    @lock.synchronize do
      if mode
        raise ArgumentError, "mode must be one of #{MODES.join(', ')}" unless MODES.include?(mode.to_s)
        @mode = mode.to_s
      end
      if fail_rate
        rate = Float(fail_rate)
        raise ArgumentError, "fail_rate must be between 0 and 1" unless rate.between?(0, 1)
        @fail_rate = rate
      end
      if timeout_seconds
        secs = Float(timeout_seconds)
        raise ArgumentError, "timeout_seconds must be >= 0" if secs.negative?
        @timeout_seconds = secs
      end
    end
    self
  end

  # Returns the fault to apply to this request, or nil to send normally.
  def pick
    @lock.synchronize do
      case @mode
      when "ok" then nil
      when "flaky" then @rng.rand < @fail_rate ? "fail" : nil
      else @mode
      end
    end
  end

  def to_h
    @lock.synchronize { { mode: @mode, fail_rate: @fail_rate, timeout_seconds: @timeout_seconds } }
  end
end
