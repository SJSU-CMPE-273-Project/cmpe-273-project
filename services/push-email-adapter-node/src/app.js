'use strict';

const { timingSafeEqual } = require('node:crypto');
const api = require('@opentelemetry/api');
const { FaultMode } = require('./fault-mode');
const { Bulkhead } = require('./bulkhead');
const { ProviderClient } = require('./provider-client');
const { maskRecipient, maskBody } = require('./pii');
const telemetry = require('./telemetry');

// Push/email adapter: the delivery worker POSTs one message here; we hand it to
// the push or email "provider" and translate the outcome into a status code the
// worker can act on. Same HTTP contract and fault modes as the SMS adapter, so
// the worker and the chaos scripts treat every channel the same way.
//
// It is the last channel in the failover chain:
//   SMS primary -> SMS secondary -> push/email -> DLQ   (T6)

const CHANNELS = ['push', 'email'];
const REQUIRED = ['event_id', 'recipient', 'body'];
const MAX_BODY_BYTES = 64 * 1024; // a notification is a few hundred bytes
const RETRYABLE = new Set([429, 503, 504]);

function configFromEnv(env = process.env) {
  return {
    providerBaseUrl: env.PROVIDER_BASE_URL || '',
    openTimeoutMs: seconds(env.PROVIDER_OPEN_TIMEOUT, 2),
    readTimeoutMs: seconds(env.PROVIDER_READ_TIMEOUT, 5),
    faultMode: env.FAULT_MODE || 'ok',
    failRate: env.FAIL_RATE || '0',
    timeoutSeconds: env.TIMEOUT_SECONDS || '30',
    adminToken: env.ADMIN_TOKEN || '',
    maxInFlight: env.MAX_IN_FLIGHT ?? '32',
    retryAfterSeconds: Number.parseInt(env.RETRY_AFTER_SECONDS || '1', 10),
  };
}

function seconds(value, fallback) {
  const n = Number(value);
  return Math.round((value && Number.isFinite(n) ? n : fallback) * 1000);
}

function createApp(config = configFromEnv(), { log = defaultLog } = {}) {
  const provider = new ProviderClient({
    baseUrl: config.providerBaseUrl,
    openTimeoutMs: config.openTimeoutMs,
    readTimeoutMs: config.readTimeoutMs,
  });
  const fault = new FaultMode({
    mode: config.faultMode, failRate: config.failRate, timeoutSeconds: config.timeoutSeconds,
  });
  const bulkhead = new Bulkhead(config.maxInFlight);

  async function handler(req, res) {
    // The HTTP instrumentation (when tracing is on) has made this request's
    // server span the active one. Keep a handle: the provider call may run in
    // a different context (see telemetry.parentContext).
    const span = api.trace.getActiveSpan();
    const ctx = { req, res, span, started: Date.now() };
    const path = new URL(req.url, 'http://adapter').pathname;
    try {
      if (req.method === 'GET' && path === '/health') {
        return send(res, 200, {
          status: 'ok', provider: 'push_email', channels: CHANNELS,
          simulated: provider.simulated, fault, bulkhead,
        });
      }
      if (req.method === 'POST' && path === '/send') return await postSend(ctx);
      if (path === '/admin/fault' && (req.method === 'GET' || req.method === 'PUT')) {
        if (!authorized(req)) return send(res, 401, { error: 'unauthorized' });
        if (req.method === 'GET') return send(res, 200, fault);
        const body = await readJson(req);
        if (body.error) return send(res, 400, { error: body.error });
        try {
          fault.update({
            mode: body.value.mode, failRate: body.value.fail_rate, timeoutSeconds: body.value.timeout_seconds,
          });
        } catch (e) {
          return send(res, 400, { error: e.message });
        }
        log('fault.changed', fault.toJSON(), span);
        return send(res, 200, fault);
      }
      return send(res, 404, { error: 'not found' });
    } catch (e) {
      // Never echo the error message: it could contain request data.
      log('error', { error: e.name }, span);
      if (!res.headersSent) send(res, 500, { status: 'error', error: 'internal error', retryable: true });
    }
  }

  async function postSend(ctx) {
    const parsed = await readJson(ctx.req);
    if (parsed.error) return invalid(ctx, undefined, undefined, parsed.error);
    const msg = parsed.value;
    const eventId = msg.event_id;

    const missing = REQUIRED.filter((k) => String(msg[k] ?? '').trim() === '');
    if (missing.length) return invalid(ctx, eventId, undefined, `missing fields: ${missing.join(', ')}`, missing);

    const channel = resolveChannel(msg.channel, msg.recipient);
    if (!channel) {
      return invalid(ctx, eventId, undefined, `channel must be one of ${CHANNELS.join(', ')}`);
    }
    if (channel === 'email' && !String(msg.recipient).includes('@')) {
      return invalid(ctx, eventId, channel, 'email recipient must be an email address');
    }

    const admitted = await bulkhead.tryRun(() => handle(ctx, msg, eventId, channel));
    if (!admitted) {
      // Back pressure: answer at once instead of queueing. The worker retries
      // after Retry-After (same event_id, so no duplicate), or dead-letters.
      ctx.res.setHeader('Retry-After', String(config.retryAfterSeconds));
      reply(ctx, 429, 'busy', eventId, channel, {
        error: `adapter at capacity (${bulkhead.max} calls in flight)`,
      });
    }
  }

  async function handle(ctx, msg, eventId, channel) {
    switch (fault.pick()) {
      case 'fail':
        return reply(ctx, 503, 'unavailable', eventId, channel, { error: 'fault injected: fail' });
      case 'reject':
        return reply(ctx, 422, 'rejected', eventId, channel, { error: 'fault injected: reject' });
      case 'timeout':
        // Holds a bulkhead slot while it hangs, like a real slow provider would.
        await new Promise((r) => setTimeout(r, fault.timeoutSeconds * 1000));
        return reply(ctx, 504, 'timeout', eventId, channel, { error: 'fault injected: timeout' });
      default:
        return forward(ctx, msg, eventId, channel);
    }
  }

  async function forward(ctx, msg, eventId, channel) {
    // Pass through the fields the oracle needs for its checks (order, latency).
    const payload = {
      event_id: eventId,
      account_id: msg.account_id,
      sequence: msg.sequence,
      occurred_at: msg.occurred_at,
      recipient: msg.recipient,
      body: msg.body,
      subject: channel === 'email' ? msg.subject : undefined,
      provider: channel, // the oracle records this as the provider ("push" / "email")
      adapter_received_at: new Date().toISOString(),
    };
    const headerTp = ctx.req.headers.traceparent;
    const traceparent = headerTp || msg.traceparent;
    // With tracing on, the HTTP instrumentation replaces this header with the
    // client span's own traceparent (a child of ours). With it off, we pass the
    // incoming one through unchanged.
    const result = await api.context.with(
      telemetry.parentContext(headerTp, msg.traceparent),
      () => provider.deliver(channel, payload, { traceparent }),
    );

    switch (result.kind) {
      case 'accepted':
        return reply(ctx, 200, 'accepted', eventId, channel, {
          providerMessageId: result.providerMessageId, recipient: msg.recipient, body: msg.body,
        });
      case 'rejected': return reply(ctx, 422, 'rejected', eventId, channel, { error: result.error });
      case 'timeout': return reply(ctx, 504, 'timeout', eventId, channel, { error: result.error });
      default: return reply(ctx, 503, 'unavailable', eventId, channel, { error: result.error });
    }
  }

  function invalid(ctx, eventId, channel, error, missing) {
    log('send.invalid', { event_id: eventId, channel, missing }, ctx.span);
    send(ctx.res, 400, { status: 'invalid', event_id: eventId, provider: channel || 'push_email', retryable: false, error });
  }

  // One place builds both the HTTP answer and the (masked) log line.
  function reply(ctx, code, status, eventId, channel, { error, providerMessageId, recipient, body } = {}) {
    const retryable = RETRYABLE.has(code);
    telemetry.annotate(ctx.span, { event_id: eventId, provider: channel, result: status, retryable });
    log(`send.${status}`, {
      event_id: eventId, channel, code, ms: Date.now() - ctx.started, error,
      recipient: recipient && maskRecipient(recipient), body: body && maskBody(body),
    }, ctx.span);
    const out = { status, event_id: eventId, provider: channel, retryable };
    if (providerMessageId) out.provider_message_id = providerMessageId;
    if (error) out.error = error;
    send(ctx.res, code, out);
  }

  function authorized(req) {
    const token = config.adminToken;
    if (!token) return true; // local dev: open. Set ADMIN_TOKEN when deployed.
    const given = Buffer.from(String(req.headers.authorization || '').replace(/^Bearer /, ''));
    const want = Buffer.from(token);
    return given.length === want.length && timingSafeEqual(given, want);
  }

  handler.state = { fault, bulkhead, provider };
  return handler;
}

// Accepts "push" / "email", any case, and the Protobuf enum names
// (CHANNEL_PUSH / CHANNEL_EMAIL) in case the worker passes those through.
// With no channel, an address with "@" goes to email and anything else to push.
function resolveChannel(raw, recipient) {
  const value = String(raw ?? '').trim().toLowerCase().replace(/^channel_/, '');
  if (value === '') return String(recipient).includes('@') ? 'email' : 'push';
  return CHANNELS.includes(value) ? value : null;
}

// Resolves to { value } or { error }; never throws on bad input.
function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let tooBig = false;
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY_BYTES) tooBig = true;
      else chunks.push(c);
    });
    req.on('error', reject);
    req.on('end', () => {
      if (tooBig) return resolve({ error: `body larger than ${MAX_BODY_BYTES} bytes` });
      let value;
      try {
        value = JSON.parse(Buffer.concat(chunks).toString('utf8') || 'null');
      } catch {
        return resolve({ error: 'body is not valid JSON' });
      }
      if (value === null || typeof value !== 'object' || Array.isArray(value)) {
        return resolve({ error: 'body must be a JSON object' });
      }
      resolve({ value });
    });
  });
}

function send(res, code, obj) {
  const text = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(text) });
  res.end(text);
}

function defaultLog(event, fields, span) {
  const line = { ts: new Date().toISOString(), svc: 'push-email-adapter', event, trace_id: telemetry.traceId(span) };
  for (const [k, v] of Object.entries(fields)) if (v !== undefined && v !== null) line[k] = v;
  for (const k of Object.keys(line)) if (line[k] === undefined) delete line[k];
  process.stdout.write(`${JSON.stringify(line)}\n`);
}

module.exports = { createApp, configFromEnv, resolveChannel, CHANNELS };
