# Bulkhead: cap how many provider calls this adapter runs at once.
#
# Why: Puma has a fixed pool of threads (8-16). If the provider gets slow,
# every thread ends up waiting on it, and then even /health stops answering:
# the adapter looks dead, and requests pile up in front of it. With a cap
# below the thread count, a slow provider can only use up its own slots and
# the rest of the adapter stays responsive.
#
# Why it never waits: when all slots are busy, the caller gets an immediate
# "no" (the app turns it into 429 + Retry-After). That is back pressure: the
# overload goes back to the delivery worker as "slow down", and the message
# waits safely in Kafka/Temporal instead of in a queue inside this process.
class Bulkhead
  attr_reader :max, :rejected

  # max: 0 or less means unlimited (bulkhead off).
  def initialize(max)
    @max = Integer(max)
    @in_flight = 0
    @rejected = 0
    @lock = Mutex.new # Puma runs requests on several threads
  end

  def enabled? = @max.positive?

  # Runs the block if a slot is free and returns true; returns false at once
  # (without running it) if the bulkhead is full.
  def try_run
    return yield.then { true } unless enabled?

    acquired = @lock.synchronize do
      if @in_flight < @max
        @in_flight += 1
      else
        @rejected += 1
        false
      end
    end
    return false unless acquired

    begin
      yield
    ensure
      # `ensure` also runs when the block exits via Sinatra's `halt` (a throw),
      # so a slot can never leak.
      @lock.synchronize { @in_flight -= 1 }
    end
    true
  end

  def in_flight = @lock.synchronize { @in_flight }

  def to_h
    @lock.synchronize { { max_in_flight: @max, in_flight: @in_flight, rejected: @rejected } }
  end
end
