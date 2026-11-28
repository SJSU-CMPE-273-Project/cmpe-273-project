'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');

// Runs in its own process (node --test), so env set here only affects this file.
test('open console allows local writes but refuses proxied ones', async () => {
  delete process.env.PUBLIC_DEMO;
  delete process.env.OPERATOR_TOKEN;
  const { app, pool } = require('../server');
  const originalQuery = pool.query;
  const originalConnect = pool.connect;
  let databaseCalls = 0;
  pool.query = async () => { databaseCalls++; return { rows: [] }; };
  pool.connect = async () => { databaseCalls++; throw new Error('blocked writes must not reach the database'); };
  const server = app.listen(0, '127.0.0.1');
  try {
    await once(server, 'listening');
    const base = `http://127.0.0.1:${server.address().port}`;
    // Open mode behind a reverse proxy (public deploy without PUBLIC_DEMO or a token).
    for (const headers of [{ 'X-Forwarded-For': '203.0.113.9' }, { Forwarded: 'for=203.0.113.9' }]) {
      const response = await fetch(`${base}/api/dlq/event/replay`, { method: 'POST', headers });
      assert.equal(response.status, 403);
      assert.match((await response.json()).error, /OPERATOR_TOKEN/);
    }
    assert.equal(databaseCalls, 0);
    // Open mode from this machine still works for the local demo (reaches the database).
    const local = await fetch(`${base}/api/dlq/event/replay`, { method: 'POST' });
    assert.equal(local.status, 500);
    assert.equal(databaseCalls, 1);
  } finally {
    pool.query = originalQuery;
    pool.connect = originalConnect;
    await new Promise(resolve => server.close(resolve));
    await pool.end();
  }
});
