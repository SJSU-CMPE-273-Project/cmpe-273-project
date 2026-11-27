'use strict';

const express = require('express');
const http = require('node:http');
const { Pool } = require('pg');
const fs = require('node:fs');
const path = require('node:path');
const agent = require('./agent');
const agentConfig = agent.config();

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
});

const operatorToken = process.env.OPERATOR_TOKEN || '';
const replayDailyCap = Number.parseInt(process.env.REPLAY_DAILY_CAP || '20', 10);
const promptVersion = agentConfig.version;

function operatorName(req) {
  return String(req.headers['x-relayguard-operator'] || 'anonymous').slice(0, 100);
}

function audit(operator, action, target, result) {
  return pool.query(
    'INSERT INTO ops_audit (operator, action, target, result) VALUES ($1,$2,$3,$4)',
    [operator, action, target, result],
  );
}

function authorized(req, res, next) {
  if (!operatorToken) return next();
  const given = String(req.headers.authorization || '').replace(/^Bearer /i, '');
  if (given !== operatorToken) return res.status(401).json({ error: 'unauthorized' });
  next();
}

app.get('/api/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/api/deliveries', authorized, async (req, res, next) => {
  try {
    const account = String(req.query.account || '').slice(0, 200);
    const values = [];
    const where = account ? 'WHERE d.account_id = $1' : '';
    if (account) values.push(account);
    const result = await pool.query(`
      SELECT d.event_id::text, d.account_id, d.sequence, d.priority, d.state,
             d.channel, d.provider, d.attempts, d.expires_at, d.created_at,
             d.delivered_at, d.updated_at
      FROM deliveries d
      ${where}
      ORDER BY d.account_id, d.sequence DESC
      LIMIT 500`, values);
    res.json({ items: result.rows });
  } catch (error) { next(error); }
});

app.get('/api/deliveries/:eventId', authorized, async (req, res, next) => {
  try {
    const eventId = req.params.eventId;
    const delivery = await pool.query(`
      SELECT d.event_id::text, d.account_id, d.sequence, d.priority, d.state,
             d.channel, d.provider, d.attempts, d.expires_at, d.created_at,
             d.delivered_at, d.updated_at
      FROM deliveries d WHERE d.event_id=$1`, [eventId]);
    if (!delivery.rows[0]) return res.status(404).json({ error: 'delivery not found' });
    const attempts = await pool.query(`
      SELECT attempt, channel, provider, succeeded, error, latency_ms, at
      FROM delivery_attempts WHERE event_id=$1 ORDER BY attempt ASC`, [eventId]);
    res.json({ ...delivery.rows[0], attempts: attempts.rows });
  } catch (error) { next(error); }
});

app.get('/api/dlq', authorized, async (_req, res, next) => {
  try {
    const result = await pool.query(`
      SELECT d.event_id::text, d.account_id, d.sequence, d.priority, d.state,
             d.attempts, d.expires_at, d.updated_at, dl.reason,
             dl.dead_lettered_at, dl.replayed_at, dl.replayed_by
      FROM dead_letters dl
      JOIN deliveries d ON d.event_id=dl.event_id
      ORDER BY dl.dead_lettered_at DESC LIMIT 500`);
    const rows = await Promise.all(result.rows.map(async row => {
      const attempts = await pool.query(`
        SELECT attempt, channel, provider, succeeded, error, latency_ms, at
        FROM delivery_attempts WHERE event_id=$1 ORDER BY attempt ASC`, [row.event_id]);
      return { ...row, attempts: attempts.rows };
    }));
    res.json({ items: rows });
  } catch (error) { next(error); }
});

app.post('/api/dlq/:eventId/replay', authorized, async (req, res, next) => {
  const operator = operatorName(req);
  const eventId = req.params.eventId;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const dlq = await client.query(`
      SELECT dl.event_id, dl.replayed_at, d.expires_at, d.priority
      FROM dead_letters dl JOIN deliveries d ON d.event_id=dl.event_id
      WHERE dl.event_id=$1 FOR UPDATE`, [eventId]);
    if (!dlq.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'dead-letter item not found' });
    }
    if (dlq.rows[0].replayed_at) {
      await audit(operator, 'replay_refused', eventId, 'already_replayed');
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'event has already been replayed' });
    }
    if (dlq.rows[0].expires_at && new Date(dlq.rows[0].expires_at) <= new Date()) {
      await audit(operator, 'replay_refused', eventId, 'expired');
      await client.query('ROLLBACK');
      return res.status(422).json({ error: 'expired message cannot be replayed' });
    }
    const cap = await client.query("SELECT count(*)::int AS n FROM ops_audit WHERE action='replay_approved' AND at >= now()-interval '24 hours'");
    if (cap.rows[0].n >= replayDailyCap) {
      await audit(operator, 'replay_refused', eventId, 'daily_cap');
      await client.query('ROLLBACK');
      return res.status(429).json({ error: 'daily replay cap reached' });
    }

    // Lane 2 owns execution. This audit/proposal table is a durable approval
    // intent and can be consumed by a worker to start the exact replay workflow.
    await client.query(`
      INSERT INTO replay_requests (event_id, operator, status, reason, prompt_version)
      VALUES ($1,$2,'approved','operator-approved',$3)
      ON CONFLICT (event_id) DO NOTHING`, [eventId, operator, promptVersion]);
    const replayIntent = await client.query('SELECT event_id FROM replay_requests WHERE event_id=$1 AND operator=$2', [eventId, operator]);
    if (!replayIntent.rows[0]) {
      await audit(operator, 'replay_refused', eventId, 'approval_already_exists');
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'a replay has already been approved or started' });
    }
    await audit(operator, 'replay_approved', eventId, 'operator_approval_written');
    await client.query('COMMIT');
    res.status(202).json({ event_id: eventId, status: 'approved', reused_idempotency_key: true });
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    next(error);
  } finally { client.release(); }
});

app.get('/api/controls', authorized, async (_req, res, next) => {
  try {
    const [producers, guardrails] = await Promise.all([
      pool.query('SELECT producer, enabled, max_per_minute FROM producer_controls ORDER BY producer'),
      pool.query('SELECT key, value, note FROM guardrail_settings ORDER BY key'),
    ]);
    res.json({ producers: producers.rows, guardrails: guardrails.rows });
  } catch (error) { next(error); }
});

app.put('/api/controls/producers/:producer', authorized, async (req, res, next) => {
  const operator = operatorName(req);
  const producer = req.params.producer;
  const enabled = typeof req.body.enabled === 'boolean' ? req.body.enabled : null;
  const max = Number(req.body.max_per_minute);
  if (enabled === null || !Number.isInteger(max) || max < 1 || max > 100000) {
    return res.status(400).json({ error: 'enabled and max_per_minute are required' });
  }
  try {
    const result = await pool.query(`
      UPDATE producer_controls SET enabled=$2, max_per_minute=$3
      WHERE producer=$1 RETURNING producer, enabled, max_per_minute`, [producer, enabled, max]);
    if (!result.rows[0]) return res.status(404).json({ error: 'producer not found' });
    await audit(operator, 'producer_control_updated', producer, enabled ? 'enabled' : 'disabled');
    res.json(result.rows[0]);
  } catch (error) { next(error); }
});

app.put('/api/controls/guardrails/:key', authorized, async (req, res, next) => {
  const operator = operatorName(req);
  const key = req.params.key;
  const value = String(req.body.value ?? '').trim();
  if (!value) return res.status(400).json({ error: 'value is required' });
  try {
    const result = await pool.query(`
      UPDATE guardrail_settings SET value=$2 WHERE key=$1
      RETURNING key, value, note`, [key, value]);
    if (!result.rows[0]) return res.status(404).json({ error: 'guardrail not found' });
    await audit(operator, 'guardrail_updated', key, value);
    res.json(result.rows[0]);
  } catch (error) { next(error); }
});

app.get('/api/metrics', authorized, async (_req, res, next) => {
  try {
    const [states, dlq, rejects, latency] = await Promise.all([
      pool.query('SELECT state, count(*)::int AS count FROM deliveries GROUP BY state'),
      pool.query('SELECT count(*)::int AS count FROM dead_letters'),
      pool.query('SELECT total FROM ingest_rejection_counts'),
      pool.query(`SELECT COALESCE(percentile_cont(0.99) within group (order by latency_ms),0)::int AS p99_ms
                  FROM delivery_attempts WHERE at >= now()-interval '15 minutes'`),
    ]);
    const counts = Object.fromEntries(states.rows.map(r => [r.state, r.count]));
    res.json({
      delivered: counts.delivered || 0,
      retrying: counts.retrying || 0,
      pending: counts.pending || 0,
      dlq: dlq.rows[0].count,
      ingest_rejections: rejects.rows[0]?.total || 0,
      p99_latency_ms: latency.rows[0]?.p99_ms || 0,
    });
  } catch (error) { next(error); }
});

app.get('/api/agent/proposals', authorized, async (_req, res, next) => {
  try {
    const result = await pool.query(`
      SELECT id, event_id::text, action, rationale, risk, prompt_version, model, input_tokens, output_tokens, source, cost_usd, usage_uncertain, approved, approved_by, created_at
      FROM agent_proposals ORDER BY created_at DESC LIMIT 100`);
    res.json({ items: result.rows });
  } catch (error) { next(error); }
});

app.get('/api/agent/usage', authorized, async (_req, res, next) => {
  try {
    const result = await pool.query(`SELECT day, input_tokens, output_tokens, budget_tokens, cost_usd
      FROM agent_daily_usage WHERE day=(now() AT TIME ZONE 'UTC')::date`);
    res.json({ day: new Date().toISOString().slice(0, 10), input_tokens: 0, output_tokens: 0,
      budget_tokens: 0, cost_usd: 0, ...result.rows[0], daily_budget: agentConfig.budget });
  } catch (error) { next(error); }
});

app.post('/api/agent/evaluate/:eventId', authorized, async (req, res, next) => {
  const eventId = req.params.eventId;
  try {
    const dlq = await pool.query(`
      SELECT d.event_id::text, d.priority, d.expires_at, dl.reason,
             d.attempts, d.updated_at
      FROM dead_letters dl JOIN deliveries d ON d.event_id=dl.event_id
      WHERE dl.event_id=$1`, [eventId]);
    if (!dlq.rows[0]) return res.status(404).json({ error: 'dead-letter item not found' });

    const attempts = await pool.query(`SELECT provider, error, at FROM delivery_attempts
      WHERE event_id=$1 ORDER BY attempt DESC LIMIT 20`, [eventId]);
    const proposal = await agent.evaluate(dlq.rows[0], attempts.rows.reverse(), agentConfig, agent.accounting(pool));
    const result = await pool.query(`
      INSERT INTO agent_proposals (event_id, action, rationale, risk, prompt_version, model,
        input_tokens, output_tokens, source, cost_usd, usage_uncertain)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [eventId, proposal.action, proposal.rationale, proposal.risk, proposal.prompt_version,
        proposal.model, proposal.input_tokens, proposal.output_tokens, proposal.source,
        proposal.cost_usd, proposal.usage_uncertain || false]);
    await audit(operatorName(req), 'agent_proposal_created', eventId, proposal.action);
    res.status(201).json(result.rows[0]);
  } catch (error) { next(error); }
});

app.post('/api/agent/proposals/:id/approve', authorized, async (req, res, next) => {
  const operator = operatorName(req);
  const id = Number(req.params.id);
  try {
    const result = await pool.query(`
      UPDATE agent_proposals SET approved=true, approved_by=$2
      WHERE id=$1 AND approved IS NULL
      RETURNING id, event_id::text, action, rationale, prompt_version`, [id, operator]);
    if (!result.rows[0]) return res.status(409).json({ error: 'proposal missing or already decided' });
    await audit(operator, 'agent_proposal_approved', result.rows[0].event_id, result.rows[0].action);
    res.json(result.rows[0]);
  } catch (error) { next(error); }
});

const dist = path.join(__dirname, 'dist');
if (fs.existsSync(dist)) app.use(express.static(dist, { maxAge: '1h', index: 'index.html' }));

app.use((_req, res) => res.status(404).json({ error: 'not found' }));
app.use((error, _req, res, _next) => {
  console.error(JSON.stringify({ event: 'api.error', name: error.name }));
  res.status(500).json({ error: 'internal server error' });
});

const port = Number.parseInt(process.env.PORT || '3000', 10);
if (require.main === module) http.createServer(app).listen(port, '0.0.0.0', () => {
  console.log(JSON.stringify({ event: 'ops-console.listening', port }));
});

module.exports = { app, pool };
