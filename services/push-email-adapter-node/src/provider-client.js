'use strict';

const http = require('node:http');
const https = require('node:https');
const { randomUUID } = require('node:crypto');

// Forwards one message to the downstream "provider" (the harness oracle / fake phone).
//
// Returns { kind, providerMessageId?, error? } where `kind` tells the app how
// to answer the worker:
//   accepted    -> 200   (provider took it)
//   rejected    -> 422   (provider said a permanent no, e.g. 4xx)
//   unavailable -> 503   (5xx, refused connection: try again later)
//   timeout     -> 504   (provider too slow: try again later)
//
// Uses node:http rather than fetch on purpose: the OpenTelemetry HTTP
// instrumentation traces node:http calls and adds the `traceparent` header
// for us, so the oracle's span joins the same trace.
class ProviderClient {
  // baseUrl: e.g. http://oracle:8080. Each channel posts to `${baseUrl}/${channel}`.
  constructor({ baseUrl = '', openTimeoutMs = 2000, readTimeoutMs = 5000 } = {}) {
    this.base = baseUrl.trim() === '' ? null : new URL(baseUrl);
    this.openTimeoutMs = openTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
  }

  // No PROVIDER_BASE_URL means "simulate": accept without calling anyone, so the
  // adapter can be tried before the oracle is up.
  get simulated() {
    return this.base === null;
  }

  deliver(channel, payload, { traceparent } = {}) {
    if (this.simulated) {
      return Promise.resolve({ kind: 'accepted', providerMessageId: `sim-${randomUUID()}` });
    }

    const url = new URL(`${this.base.pathname.replace(/\/$/, '')}/${channel}`, this.base);
    const body = JSON.stringify(payload);
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
      // event_id doubles as the provider idempotency key, so if the worker retries
      // after a lost ack, the provider (oracle) can tell it's the same message.
      'Idempotency-Key': payload.event_id,
    };
    if (traceparent) headers.traceparent = traceparent;

    return new Promise((resolve) => {
      let settled = false;
      const finish = (result) => {
        if (settled) return;
        settled = true;
        clearTimeout(openTimer);
        clearTimeout(totalTimer);
        resolve(result);
      };

      const lib = url.protocol === 'https:' ? https : http;
      const req = lib.request(url, { method: 'POST', headers }, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('error', (e) => finish({ kind: 'unavailable', error: `${e.code || e.name}: ${e.message}` }));
        res.on('end', () => finish(classify(res.statusCode, Buffer.concat(chunks).toString('utf8'))));
      });

      // Two deadlines, like the Ruby adapter: one for connecting, one for the
      // whole call. Either one firing counts as a timeout (504, retryable).
      const timeout = (what) => () => {
        finish({ kind: 'timeout', error: `provider ${what} timed out` });
        req.destroy();
      };
      const openTimer = setTimeout(timeout('connect'), this.openTimeoutMs);
      const totalTimer = setTimeout(timeout('response'), this.openTimeoutMs + this.readTimeoutMs);
      req.on('socket', (socket) => {
        if (!socket.connecting) clearTimeout(openTimer); // reused keep-alive socket
        else socket.once('connect', () => clearTimeout(openTimer));
      });

      req.on('error', (e) => finish({ kind: 'unavailable', error: `${e.code || e.name}: ${e.message}` }));
      req.end(body);
    });
  }
}

function classify(status, text) {
  if (status >= 200 && status < 300) {
    let parsed = {};
    try {
      parsed = JSON.parse(text) || {};
    } catch {
      // A 2xx with a non-JSON body still means the provider took it.
    }
    return { kind: 'accepted', providerMessageId: parsed.provider_message_id || parsed.id || null };
  }
  if (status >= 400 && status < 500) return { kind: 'rejected', error: `provider returned ${status}` };
  return { kind: 'unavailable', error: `provider returned ${status}` };
}

module.exports = { ProviderClient };
