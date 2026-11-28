'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');

test('with OPERATOR_TOKEN set, only the exact bearer token is accepted', async () => {
  delete process.env.PUBLIC_DEMO;
  process.env.OPERATOR_TOKEN = 'test-only-token';
  const { app, pool } = require('../server');
  const originalQuery = pool.query;
  pool.query = async () => ({ rows: [] });
  const server = app.listen(0, '127.0.0.1');
  try {
    await once(server, 'listening');
    const base = `http://127.0.0.1:${server.address().port}`;
    for (const authorization of [undefined, 'Bearer wrong', 'Bearer test-only-toke', 'Bearer test-only-token-extra']) {
      const headers = authorization ? { Authorization: authorization } : {};
      assert.equal((await fetch(`${base}/api/controls`, { headers })).status, 401, String(authorization));
    }
    // A valid token also works through a proxy: the local-only rule applies to open mode only.
    const ok = await fetch(`${base}/api/controls`, { headers: { Authorization: 'Bearer test-only-token', 'X-Forwarded-For': '203.0.113.9' } });
    assert.equal(ok.status, 200);
  } finally {
    pool.query = originalQuery;
    await new Promise(resolve => server.close(resolve));
    await pool.end();
  }
});
