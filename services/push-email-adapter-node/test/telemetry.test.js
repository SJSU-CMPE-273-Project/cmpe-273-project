'use strict';

// Runs in its own process (node --test isolates files), so tracing can be
// switched on before node:http is loaded, just like index.js does.
const { InMemorySpanExporter } = require('@opentelemetry/sdk-trace');
const exporter = new InMemorySpanExporter();
require('../src/telemetry').setup({ exporter });

const test = require('node:test');
const assert = require('node:assert/strict');
const { fakeProvider, startAdapter, post, MSG } = require('./helpers');

const TRACE = '0af7651916cd43dd8448eb211c80319c';

test('one trace: worker -> adapter server span -> provider call, with ids only', async (t) => {
  const provider = await fakeProvider();
  const adapter = await startAdapter({ PROVIDER_BASE_URL: provider.url });
  t.after(async () => { await adapter.close(); await provider.close(); });

  const r = await post(`${adapter.url}/send`, MSG, { traceparent: `00-${TRACE}-b7ad6b7169203331-01` });
  assert.equal(r.status, 200);

  // The provider saw a traceparent from the same trace, but a new parent span (ours).
  const tp = provider.calls[0].headers.traceparent;
  assert.match(tp, new RegExp(`^00-${TRACE}-[0-9a-f]{16}-01$`));
  assert.notEqual(tp, `00-${TRACE}-b7ad6b7169203331-01`);

  // Spans end slightly after the response is sent.
  await new Promise((res) => setTimeout(res, 50));
  const spans = exporter.getFinishedSpans().filter((s) => s.spanContext().traceId === TRACE);
  const server = spans.find((s) => s.attributes['relayguard.event_id']);
  assert.ok(server, 'server span carries relayguard attributes');
  assert.equal(server.attributes['relayguard.provider'], 'push');
  assert.equal(server.attributes['relayguard.result'], 'accepted');
  assert.equal(server.attributes['relayguard.retryable'], false);
  assert.ok(spans.length >= 3, 'server span (adapter) + client span (to provider) + provider-side span');

  // Never the recipient or the message text, in any span attribute.
  const all = JSON.stringify(exporter.getFinishedSpans().map((s) => s.attributes));
  assert.ok(!all.includes(MSG.recipient));
  assert.ok(!all.includes('482913'));
});

test('traceparent only in the JSON body still continues that trace', async (t) => {
  const provider = await fakeProvider();
  const adapter = await startAdapter({ PROVIDER_BASE_URL: provider.url });
  t.after(async () => { await adapter.close(); await provider.close(); });

  const bodyTrace = '22222222222222222222222222222222';
  await post(`${adapter.url}/send`, { ...MSG, traceparent: `00-${bodyTrace}-3333333333333333-01` });
  assert.match(provider.calls[0].headers.traceparent, new RegExp(`^00-${bodyTrace}-`));
});
