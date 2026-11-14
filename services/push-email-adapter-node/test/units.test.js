'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { maskRecipient, maskBody } = require('../src/pii');
const { FaultMode } = require('../src/fault-mode');
const { Bulkhead } = require('../src/bulkhead');
const { resolveChannel } = require('../src/app');

test('PII: recipients and bodies are masked', () => {
  assert.equal(maskRecipient('pooja@example.com'), 'p***@example.com');
  assert.equal(maskRecipient('device-token-abcdef9876'), '***9876');
  assert.equal(maskRecipient(''), '');
  assert.equal(maskBody('Your code is 482913'), '[redacted len=19]');
});

test('channel: explicit, enum name, or inferred from the recipient', () => {
  assert.equal(resolveChannel('push', 'x'), 'push');
  assert.equal(resolveChannel('EMAIL', 'a@b.c'), 'email');
  assert.equal(resolveChannel('CHANNEL_PUSH', 'x'), 'push');
  assert.equal(resolveChannel(undefined, 'a@b.c'), 'email');
  assert.equal(resolveChannel('', 'token123'), 'push');
  assert.equal(resolveChannel('sms', '+14085550123'), null); // SMS has its own adapter
  assert.equal(resolveChannel('fax', 'x'), null);
});

test('fault mode: modes, flaky rate, and validation', () => {
  const f = new FaultMode({ random: () => 0.2 });
  assert.equal(f.pick(), null);
  f.update({ mode: 'flaky', failRate: 0.3 });
  assert.equal(f.pick(), 'fail'); // 0.2 < 0.3
  f.update({ failRate: 0.1 });
  assert.equal(f.pick(), null);
  f.update({ mode: 'reject' });
  assert.equal(f.pick(), 'reject');
  assert.throws(() => f.update({ mode: 'explode' }), /mode must be one of/);
  assert.throws(() => f.update({ failRate: 2 }), /between 0 and 1/);
  assert.throws(() => f.update({ timeoutSeconds: 'abc' }), /must be a number/);
  // A rejected update changes nothing, even fields that were valid.
  assert.throws(() => f.update({ mode: 'ok', failRate: 5 }));
  assert.equal(f.mode, 'reject');
});

test('bulkhead: refuses overflow at once and never leaks a slot', async () => {
  const b = new Bulkhead(1);
  let release;
  const first = b.tryRun(() => new Promise((r) => { release = r; }));
  assert.equal(await b.tryRun(async () => assert.fail('must not run')), false);
  assert.deepEqual(b.toJSON(), { max_in_flight: 1, in_flight: 1, rejected: 1 });
  release();
  assert.equal(await first, true);

  await assert.rejects(b.tryRun(async () => { throw new Error('boom'); }), /boom/);
  assert.equal(b.inFlight, 0, 'slot released after a throw');

  const off = new Bulkhead(0);
  assert.equal(off.enabled, false);
  assert.equal(await off.tryRun(async () => {}), true);
});
