'use strict';

// Application-level faults the chaos harness can switch on at runtime.
// Same modes as the SMS adapter, so the chaos scripts drive both the same way.
//
// Toxiproxy already breaks the *network* (down, latency, cut connections).
// These modes break the *provider's answer*, which the network can't fake:
//   ok      - forward every message
//   fail    - 503, a temporary outage: the worker retries, then dead-letters
//   timeout - hang, then 504: the worker's client timeout should fire first
//   reject  - 422, a permanent "no": retrying is pointless
//   flaky   - fail randomly with probability fail_rate (exercises retry + backoff)
//
// State lives in memory on purpose: the adapter is stateless, and a restart
// resetting to the env default is the behaviour we want.

const MODES = ['ok', 'fail', 'timeout', 'reject', 'flaky'];

class FaultMode {
  constructor({ mode = 'ok', failRate = 0, timeoutSeconds = 30, random = Math.random } = {}) {
    this.random = random;
    this.update({ mode, failRate, timeoutSeconds });
  }

  // Validates everything first, then applies, so a bad request changes nothing.
  update({ mode, failRate, timeoutSeconds } = {}) {
    const next = { mode: this.mode, failRate: this.failRate, timeoutSeconds: this.timeoutSeconds };
    if (mode != null) {
      if (!MODES.includes(String(mode))) throw new RangeError(`mode must be one of ${MODES.join(', ')}`);
      next.mode = String(mode);
    }
    if (failRate != null) {
      const rate = toNumber(failRate, 'fail_rate');
      if (rate < 0 || rate > 1) throw new RangeError('fail_rate must be between 0 and 1');
      next.failRate = rate;
    }
    if (timeoutSeconds != null) {
      const secs = toNumber(timeoutSeconds, 'timeout_seconds');
      if (secs < 0) throw new RangeError('timeout_seconds must be >= 0');
      next.timeoutSeconds = secs;
    }
    Object.assign(this, next);
    return this;
  }

  // Returns the fault to apply to this request, or null to send normally.
  pick() {
    switch (this.mode) {
      case 'ok': return null;
      case 'flaky': return this.random() < this.failRate ? 'fail' : null;
      default: return this.mode;
    }
  }

  toJSON() {
    return { mode: this.mode, fail_rate: this.failRate, timeout_seconds: this.timeoutSeconds };
  }
}

function toNumber(value, name) {
  const n = typeof value === 'number' ? value : Number(String(value).trim());
  if (String(value).trim() === '' || !Number.isFinite(n)) throw new RangeError(`${name} must be a number`);
  return n;
}

module.exports = { FaultMode, MODES };
