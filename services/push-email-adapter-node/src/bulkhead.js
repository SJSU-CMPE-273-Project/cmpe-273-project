'use strict';

// Bulkhead: cap how many provider calls this adapter has open at once.
//
// Node runs on one thread, so a slow provider can't use up worker threads the
// way it can in Puma (the SMS adapter). /health keeps answering either way.
// What a slow provider *does* pile up here is open sockets, timers and request
// bodies in memory, all waiting on it. The cap bounds that, and the overflow
// gets an immediate "no" (the app turns it into 429 + Retry-After).
// That is back pressure: the message waits safely in Kafka/Temporal, and the
// delivery worker slows down, instead of a queue growing inside this process.
class Bulkhead {
  // max: 0 or less means unlimited (bulkhead off).
  constructor(max) {
    this.max = Number.parseInt(max, 10) || 0;
    this.inFlight = 0;
    this.rejected = 0;
  }

  get enabled() {
    return this.max > 0;
  }

  // Runs fn if a slot is free and resolves to true; resolves to false at once
  // (without running fn) if the bulkhead is full. No lock needed: Node runs
  // this check-and-increment without interruption.
  async tryRun(fn) {
    if (this.enabled && this.inFlight >= this.max) {
      this.rejected += 1;
      return false;
    }
    this.inFlight += 1;
    try {
      await fn();
    } finally {
      // Released even if fn throws, so a slot can never leak.
      this.inFlight -= 1;
    }
    return true;
  }

  toJSON() {
    return { max_in_flight: this.max, in_flight: this.inFlight, rejected: this.rejected };
  }
}

module.exports = { Bulkhead };
