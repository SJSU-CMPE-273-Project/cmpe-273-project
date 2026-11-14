'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { fakeProvider, startAdapter, post, put, get, MSG } = require('./helpers');

// Each test starts its own fake provider + adapter on free ports and stops them after.
async function setup(t, { respond, env = {} } = {}) {
  const provider = await fakeProvider(respond);
  const adapter = await startAdapter({ PROVIDER_BASE_URL: provider.url, ...env });
  t.after(async () => { await adapter.close(); await provider.close(); });
  return { provider, adapter };
}

test('push: forwards to /push with idempotency key and oracle fields', async (t) => {
  const { provider, adapter } = await setup(t);
  const r = await post(`${adapter.url}/send`, MSG, { traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' });

  assert.equal(r.status, 200);
  assert.deepEqual(r.body, {
    status: 'accepted', event_id: MSG.event_id, provider: 'push', retryable: false, provider_message_id: 'pm-1',
  });
  assert.equal(provider.calls.length, 1);
  const call = provider.calls[0];
  assert.equal(call.path, '/push');
  assert.equal(call.headers['idempotency-key'], MSG.event_id);
  assert.equal(call.headers.traceparent, '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
  assert.equal(call.body.provider, 'push');
  assert.equal(call.body.account_id, 'acct-1001');
  assert.equal(call.body.sequence, 42);
  assert.equal(call.body.occurred_at, MSG.occurred_at);
  assert.ok(call.body.adapter_received_at);
});

test('email: inferred from an address, forwards to /email with subject', async (t) => {
  const { provider, adapter } = await setup(t);
  const r = await post(`${adapter.url}/send`, {
    ...MSG, channel: undefined, recipient: 'pooja@example.com', subject: 'Fraud alert',
  });
  assert.equal(r.status, 200);
  assert.equal(r.body.provider, 'email');
  assert.equal(provider.calls[0].path, '/email');
  assert.equal(provider.calls[0].body.subject, 'Fraud alert');
});

test('traceparent in the JSON body is passed on when there is no header', async (t) => {
  const { provider, adapter } = await setup(t);
  const tp = '00-11111111111111111111111111111111-2222222222222222-01';
  await post(`${adapter.url}/send`, { ...MSG, traceparent: tp });
  assert.equal(provider.calls[0].headers.traceparent, tp);
});

test('provider answers map to the worker contract', async (t) => {
  let next = 200;
  const { adapter } = await setup(t, { respond: () => ({ status: next }) });
  for (const [providerStatus, code, status, retryable] of [
    [409, 422, 'rejected', false], // provider 4xx = permanent no
    [500, 503, 'unavailable', true],
    [204, 200, 'accepted', false],
  ]) {
    next = providerStatus;
    const r = await post(`${adapter.url}/send`, MSG);
    assert.equal(r.status, code, `provider ${providerStatus}`);
    assert.equal(r.body.status, status);
    assert.equal(r.body.retryable, retryable);
  }
});

test('provider down -> 503 retryable', async (t) => {
  const adapter = await startAdapter({ PROVIDER_BASE_URL: 'http://127.0.0.1:1' });
  t.after(() => adapter.close());
  const r = await post(`${adapter.url}/send`, MSG);
  assert.equal(r.status, 503);
  assert.equal(r.body.retryable, true);
});

test('slow provider -> 504 timeout', async (t) => {
  const { adapter } = await setup(t, {
    respond: () => 'hang',
    env: { PROVIDER_OPEN_TIMEOUT: '0.5', PROVIDER_READ_TIMEOUT: '0.2' },
  });
  const r = await post(`${adapter.url}/send`, MSG);
  assert.equal(r.status, 504);
  assert.equal(r.body.status, 'timeout');
  assert.equal(r.body.retryable, true);
});

test('bad requests -> 400, not retryable, provider never called', async (t) => {
  const { provider, adapter } = await setup(t);
  const cases = [
    ['not json', /not valid JSON/],
    ['[1,2]', /JSON object/],
    [{ ...MSG, body: '' }, /missing fields: body/],
    [{ ...MSG, channel: 'sms' }, /channel must be one of push, email/],
    [{ ...MSG, channel: 'email', recipient: 'device-token' }, /email address/],
    [JSON.stringify({ ...MSG, body: 'x'.repeat(70 * 1024) }), /larger than/],
  ];
  for (const [body, error] of cases) {
    const r = await post(`${adapter.url}/send`, body);
    assert.equal(r.status, 400);
    assert.equal(r.body.status, 'invalid');
    assert.equal(r.body.retryable, false);
    assert.match(r.body.error, error);
  }
  assert.equal(provider.calls.length, 0);
});

test('fault modes: fail, reject, flaky, timeout', async (t) => {
  const { provider, adapter } = await setup(t);
  const fault = (body) => put(`${adapter.url}/admin/fault`, body);

  await fault({ mode: 'fail' });
  assert.equal((await post(`${adapter.url}/send`, MSG)).status, 503);
  await fault({ mode: 'reject' });
  assert.equal((await post(`${adapter.url}/send`, MSG)).status, 422);
  await fault({ mode: 'flaky', fail_rate: 1 });
  assert.equal((await post(`${adapter.url}/send`, MSG)).status, 503);
  await fault({ mode: 'timeout', timeout_seconds: 0.05 });
  assert.equal((await post(`${adapter.url}/send`, MSG)).status, 504);
  assert.equal(provider.calls.length, 0, 'faults answer without calling the provider');

  assert.equal((await fault({ mode: 'nope' })).status, 400);
  assert.equal((await fault({ mode: 'ok' })).body.mode, 'ok');
  assert.equal((await post(`${adapter.url}/send`, MSG)).status, 200);
});

test('bulkhead: overflow gets 429 + Retry-After at once, /health still answers', async (t) => {
  const { adapter } = await setup(t, {
    respond: () => 'hang',
    env: { MAX_IN_FLIGHT: '2', RETRY_AFTER_SECONDS: '3', PROVIDER_OPEN_TIMEOUT: '0.5', PROVIDER_READ_TIMEOUT: '0.5' },
  });
  // Two calls fill the bulkhead (the provider hangs); don't await them.
  const held = [post(`${adapter.url}/send`, MSG), post(`${adapter.url}/send`, MSG)];
  while (adapter.state.bulkhead.inFlight < 2) await new Promise((r) => setTimeout(r, 10));

  const started = Date.now();
  const r = await post(`${adapter.url}/send`, MSG);
  assert.equal(r.status, 429);
  assert.equal(r.headers.get('retry-after'), '3');
  assert.deepEqual([r.body.status, r.body.retryable], ['busy', true]);
  assert.ok(Date.now() - started < 500, 'refused at once, not queued');

  const h = await get(`${adapter.url}/health`);
  assert.equal(h.status, 200);
  assert.deepEqual(h.body.bulkhead, { max_in_flight: 2, in_flight: 2, rejected: 1 });

  await Promise.all(held); // they time out (504) and release their slots
  assert.equal(adapter.state.bulkhead.inFlight, 0);
});

test('admin token protects /admin/fault', async (t) => {
  const { adapter } = await setup(t, { env: { ADMIN_TOKEN: 's3cret' } });
  assert.equal((await get(`${adapter.url}/admin/fault`)).status, 401);
  assert.equal((await get(`${adapter.url}/admin/fault`, { Authorization: 'Bearer wrong!' })).status, 401);
  const ok = await put(`${adapter.url}/admin/fault`, { mode: 'fail' }, { Authorization: 'Bearer s3cret' });
  assert.equal(ok.status, 200);
  assert.equal(ok.body.mode, 'fail');
});

test('simulated mode: no PROVIDER_BASE_URL accepts without calling anyone', async (t) => {
  const adapter = await startAdapter({});
  t.after(() => adapter.close());
  const r = await post(`${adapter.url}/send`, MSG);
  assert.equal(r.status, 200);
  assert.match(r.body.provider_message_id, /^sim-/);
  assert.equal((await get(`${adapter.url}/health`)).body.simulated, true);
});

test('logs never contain the recipient or the message text', async (t) => {
  const { adapter } = await setup(t);
  await post(`${adapter.url}/send`, { ...MSG, channel: 'email', recipient: 'pooja@example.com' });
  await post(`${adapter.url}/send`, MSG);
  const text = JSON.stringify(adapter.logs);
  assert.ok(!text.includes('pooja@example.com'));
  assert.ok(!text.includes(MSG.recipient));
  assert.ok(!text.includes('482913'));
  assert.ok(text.includes('p***@example.com'));
  assert.ok(text.includes('[redacted len=19]'));
});
