'use strict';

const http = require('node:http');
const { createApp, configFromEnv } = require('../src/app');

// Starts an http server on a free port and returns { url, close }.
function listen(handler) {
  return new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        server,
        url: `http://127.0.0.1:${port}`,
        close: () => new Promise((r) => { server.closeAllConnections(); server.close(r); }),
      });
    });
  });
}

// A fake provider (stands in for the oracle). `respond(req, body)` decides the
// answer; every call is recorded with its path, headers and parsed body.
async function fakeProvider(respond = () => ({ status: 200, body: { provider_message_id: 'pm-1' } })) {
  const calls = [];
  const srv = await listen((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', async () => {
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8') || 'null');
      calls.push({ path: req.url, headers: req.headers, body });
      const out = await respond(req, body);
      if (out === 'hang') return; // never answer
      res.writeHead(out.status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(out.body ?? {}));
    });
  });
  return { ...srv, calls };
}

// Starts the adapter with env-style overrides and a log capture.
async function startAdapter(env = {}) {
  const logs = [];
  const handler = createApp(configFromEnv(env), {
    log: (event, fields) => logs.push({ event, ...fields }),
  });
  const srv = await listen(handler);
  return { ...srv, logs, state: handler.state };
}

async function post(url, body, headers = {}) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });
  return { status: res.status, headers: res.headers, body: await res.json() };
}

async function put(url, body, headers = {}) {
  const res = await fetch(url, { method: 'PUT', headers, body: JSON.stringify(body) });
  return { status: res.status, body: await res.json() };
}

async function get(url, headers = {}) {
  const res = await fetch(url, { headers });
  return { status: res.status, body: await res.json() };
}

const MSG = Object.freeze({
  event_id: '0b9a6c1e-1111-4222-8333-444455556666',
  channel: 'push',
  recipient: 'device-token-abcdef9876',
  body: 'Your code is 482913',
  account_id: 'acct-1001',
  sequence: 42,
  occurred_at: '2026-11-13T18:05:00Z',
});

module.exports = { listen, fakeProvider, startAdapter, post, put, get, MSG };
