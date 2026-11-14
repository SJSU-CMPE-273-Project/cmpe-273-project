'use strict';

// PII guardrail: anything that leaves this process as a log line goes through here.
// Same rules as the SMS adapter (services/sms-adapter-ruby/lib/pii.rb).
//
// Why not hash the body? OTP bodies are short ("Your code is 482913"), so a hash
// can be brute-forced back to the code. Length is enough to debug with.

function maskRecipient(value) {
  const s = String(value ?? '');
  if (s === '') return '';
  if (s.includes('@')) {
    const at = s.indexOf('@');
    return `${s[0]}***@${s.slice(at + 1)}`;
  }
  // Push tokens are long opaque strings; the last 4 chars are enough to tell them apart.
  return `***${s.slice(-4)}`;
}

function maskBody(value) {
  return `[redacted len=${String(value ?? '').length}]`;
}

module.exports = { maskRecipient, maskBody };
