'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { once } = require('node:events');
const database = process.env.AGENT_TEST_DATABASE_URL;
test('PostgreSQL migrations, concurrent budget, evaluate API and approval boundary', { skip: !database }, async () => {
  if (new URL(database).pathname !== '/relayguard_agent_test') throw new Error('Requires dedicated relayguard_agent_test database');
  process.env.DATABASE_URL = database;
  process.env.AGENT_PROVIDER = 'gemini';
  process.env.AGENT_PROMPT_VERSION = 'dlq-v2';
  process.env.AGENT_MODEL = 'gemini-3.5-flash-lite';
  process.env.GEMINI_API_KEY = 'test-only';
  process.env.AGENT_INPUT_USD_PER_MILLION = '0';
  process.env.AGENT_OUTPUT_USD_PER_MILLION = '0';
  const { app, pool } = require('../server');
  const agent = require('../agent');
  const nativeFetch = global.fetch;
  let server;
  try {
    for (const file of ['001_schema.sql', '002_sequence_range.sql', '003_ingest_recovery.sql',
      '004_ingest_rejection_identity.sql', '005_delivery_replay.sql', '005_ops_console.sql', '006_agent_harness.sql']) {
      await pool.query(fs.readFileSync(path.join(__dirname, '../../../infra/postgres/init', file), 'utf8'));
    }
    // Upgrade is idempotent on existing Lane 4 databases.
    await pool.query(fs.readFileSync(path.join(__dirname, '../../../infra/postgres/init/006_agent_harness.sql'), 'utf8'));
    const budget = agent.accounting(pool);
    const tickets = await Promise.all(Array.from({ length: 20 }, () => budget.reserve(100, 500)));
    assert.equal(tickets.filter(Boolean).length, 5);
    await pool.query('DELETE FROM agent_daily_usage');
    const id = '00000000-0000-4000-8000-000000000019';
    await pool.query(`INSERT INTO ingested_events(event_id,account_id,sequence,producer,payload_sha256)
      VALUES($1,'secret-account',1,'test',decode(repeat('00',32),'hex'))`, [id]);
    await pool.query(`INSERT INTO deliveries(event_id,account_id,sequence,priority,state) VALUES($1,'secret-account',1,2,'dead_lettered')`, [id]);
    await pool.query(`INSERT INTO dead_letters(event_id,reason) VALUES($1,'poison_after_fix')`, [id]);
    await pool.query(`INSERT INTO delivery_attempts(event_id,attempt,channel,provider,succeeded,error)
      VALUES($1,1,'sms','sms_primary',false,'rejected')`, [id]);
    let calls = 0;
    global.fetch = async (url, options) => {
      calls++;
      assert.equal(url, 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions');
      const body = JSON.parse(options.body);
      assert.equal(body.messages[0].role, 'system');
      assert.ok(body.messages[0].content.includes('dlq-v2'));
      assert.ok(!options.body.includes('secret-account'));
      assert.ok(!options.body.includes(id));
      return { ok: true, json: async () => ({ choices: [{ finish_reason: 'stop', message: {
        content: JSON.stringify({ action: 'replay', rationale: 'Fixed; human approval required.', risk: 'medium' }) } }],
        usage: { prompt_tokens: 300, completion_tokens: 40 } }) };
    };
    server = app.listen(0, '127.0.0.1'); await once(server, 'listening');
    const base = `http://127.0.0.1:${server.address().port}`;
    const response = await nativeFetch(`${base}/api/agent/evaluate/${id}`, { method: 'POST' });
    assert.equal(response.status, 201);
    const proposal = await response.json();
    assert.equal(proposal.model, 'gemini-3.5-flash-lite'); assert.equal(proposal.prompt_version, 'dlq-v2');
    assert.equal(Number(proposal.input_tokens), 300); assert.equal(Number(proposal.output_tokens), 40);
    assert.equal(proposal.approved, null); assert.equal(calls, 1);
    const usage = await (await nativeFetch(`${base}/api/agent/usage`)).json();
    assert.equal(Number(usage.budget_tokens), 340);
    assert.equal(Number(usage.cost_usd), 0);
    const listed = await (await nativeFetch(`${base}/api/agent/proposals`)).json();
    assert.equal(listed.items[0].model, 'gemini-3.5-flash-lite');
    const approved = await nativeFetch(`${base}/api/agent/proposals/${proposal.id}/approve`, { method: 'POST' });
    assert.equal(approved.status, 200);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM replay_requests')).rows[0].n, 0);
    // Only an explicit human request enters the existing Issue B replay boundary.
    const replay = await nativeFetch(`${base}/api/dlq/${id}/replay`, { method: 'POST' });
    assert.equal(replay.status, 202);
    assert.equal((await pool.query('SELECT status FROM replay_requests')).rows[0].status, 'approved');
  } finally {
    global.fetch = nativeFetch;
    if (server) await new Promise(resolve => server.close(resolve));
    await pool.end();
  }
});
