'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');

test('public demo blocks operator writes before database or model access', async () => {
  process.env.PUBLIC_DEMO = 'true';
  const { app, pool } = require('../server');
  const originalQuery = pool.query;
  let databaseCalls = 0;
  pool.query = async () => { databaseCalls++; throw new Error('Public demo must not write'); };
  const server = app.listen(0, '127.0.0.1');
  try {
    await once(server, 'listening');
    const base = `http://127.0.0.1:${server.address().port}`;
    const config = await (await fetch(`${base}/api/config`)).json();
    assert.equal(config.publicDemo, true);
    for (const [method, route] of [
      ['POST', '/api/dlq/event/replay'],
      ['PUT', '/api/controls/producers/test'],
      ['PUT', '/api/controls/guardrails/test'],
      ['POST', '/api/agent/evaluate/event'],
      ['POST', '/api/agent/proposals/1/approve'],
    ]) {
      const response = await fetch(`${base}${route}`, { method });
      assert.equal(response.status, 403, route);
      assert.match((await response.json()).error, /public demo mode/);
    }
    assert.equal(databaseCalls, 0);
  } finally {
    pool.query = originalQuery;
    await new Promise(resolve => server.close(resolve));
    await pool.end();
  }
});
