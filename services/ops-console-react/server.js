'use strict';

const express = require('express');
const http = require('node:http');
const { Pool } = require('pg');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
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
const publicDemo = /^(1|true|yes)$/i.test(process.env.PUBLIC_DEMO || 'false');
const oracleUrl = (process.env.ORACLE_URL || '').replace(/\/$/, '');
const demoTriggerUrl = (process.env.DEMO_TRIGGER_URL || '').replace(/\/$/, '');

function operatorName(req) {
  return String(req.headers['x-relayguard-operator'] || 'anonymous').slice(0, 100);
}

function audit(operator, action, target, result) {
  return pool.query(
    'INSERT INTO ops_audit (operator, action, target, result) VALUES ($1,$2,$3,$4)',
    [operator, action, target, result],
  );
}

function tokenMatches(given) {
  const a = crypto.createHash('sha256').update(given).digest();
  const b = crypto.createHash('sha256').update(operatorToken).digest();
  return crypto.timingSafeEqual(a, b);
}

function authorized(req, res, next) {
  if (!operatorToken) return next();
  const given = String(req.headers.authorization || '').replace(/^Bearer /i, '');
  if (!tokenMatches(given)) return res.status(401).json({ error: 'unauthorized' });
  next();
}

// With no OPERATOR_TOKEN and PUBLIC_DEMO off, the console is open. Allow writes in
// that mode only from this machine or a private (Docker) network, never through a
// reverse proxy, so an accidental public deploy cannot replay or flip kill switches.
function localAddress(address = '') {
  const ip = address.replace(/^::ffff:/, '');
  return ip === '::1' || /^(127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)/.test(ip) || /^f[cd]/i.test(ip);
}

function openModeWritesLocalOnly(req, res, next) {
  if (operatorToken) return next();
  if (localAddress(req.socket.remoteAddress) && !req.headers['x-forwarded-for'] && !req.headers.forwarded) return next();
  return res.status(403).json({ error: 'set OPERATOR_TOKEN or PUBLIC_DEMO=true before exposing operator actions' });
}

function mutationsDisabledInPublicDemo(_req, res, next) {
  if (publicDemo) return res.status(403).json({ error: 'operator actions are disabled in public demo mode' });
  next();
}

app.get('/api/health', (_req, res) => res.json({ status: 'ok' }));
app.get('/api/config', (_req, res) => res.json({ publicDemo }));

app.get('/api/results', authorized, async (_req, res) => {
  if (!oracleUrl) return res.status(503).type('text').send('Oracle results are not configured.');
  try {
    const response = await fetch(`${oracleUrl}/report?format=text`, { signal: AbortSignal.timeout(5000) });
    const report = await response.text();
    res.status(response.status).type('text').send(report);
  } catch {
    res.status(502).type('text').send('Oracle results are temporarily unavailable.');
  }
});

app.get('/api/demo/status', authorized, async (_req, res) => {
  if (!demoTriggerUrl) return res.status(503).json({ error: 'demo trigger is not configured' });
  try {
    const response = await fetch(`${demoTriggerUrl}/status`, { signal: AbortSignal.timeout(3000) });
    res.status(response.status).json(await response.json());
  } catch {
    res.status(502).json({ error: 'demo trigger is temporarily unavailable' });
  }
});

app.post('/api/demo/run', authorized, async (_req, res) => {
  if (!demoTriggerUrl) return res.status(503).json({ error: 'demo trigger is not configured' });
  try {
    const response = await fetch(`${demoTriggerUrl}/run`, {
      method: 'POST',
      signal: AbortSignal.timeout(5000),
    });
    res.status(response.status).json(await response.json());
  } catch {
    res.status(502).json({ error: 'demo trigger is temporarily unavailable' });
  }
});

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
             dl.dead_lettered_at, dl.replayed_at, dl.replayed_by,
             rr.status AS replay_status, rr.reason AS replay_reason, rr.workflow_id AS replay_workflow_id
      FROM dead_letters dl
      JOIN deliveries d ON d.event_id=dl.event_id
      LEFT JOIN replay_requests rr ON rr.event_id=dl.event_id
      WHERE dl.discarded_at IS NULL
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

class ReplayRefusal extends Error {
  constructor(status, message, reason) { super(message); this.status = status; this.reason = reason; }
}

// Both direct operator replay and proposal approval use this same transaction.
async function queueReplay(client, eventId, operator, version) {
  // Serialize approvals across instances so concurrent requests cannot exceed the cap.
  await client.query("SELECT pg_advisory_xact_lock(hashtextextended('console-replay-approvals',0))");
  const result = await client.query(`SELECT dl.replayed_at, dl.discarded_at, d.state,
    COALESCE(d.expires_at<=clock_timestamp(),false) AS expired
    FROM dead_letters dl JOIN deliveries d USING(event_id)
    WHERE dl.event_id=$1 FOR UPDATE OF dl,d`, [eventId]);
  const row = result.rows[0];
  if (!row) throw new ReplayRefusal(404, 'dead-letter item not found', 'not_found');
  if (row.replayed_at) throw new ReplayRefusal(409, 'event has already been replayed', 'already_replayed');
  if (row.discarded_at) throw new ReplayRefusal(409, 'event was discarded', 'discarded');
  if (row.expired) throw new ReplayRefusal(422, 'expired message cannot be replayed', 'expired');
  if (row.state !== 'dead_lettered') throw new ReplayRefusal(422, 'event is not in the DLQ', 'not_in_dlq');
  const previous = await client.query('SELECT event_id FROM replay_requests WHERE event_id=$1', [eventId]);
  if (previous.rows[0]) throw new ReplayRefusal(409, 'a replay has already been approved or started', 'approval_already_exists');
  const cap = await client.query("SELECT count(*)::int AS n FROM replay_requests WHERE created_at>=now()-interval '24 hours'");
  if (cap.rows[0].n >= replayDailyCap) throw new ReplayRefusal(429, 'daily replay cap reached', 'daily_cap');
  await client.query(`INSERT INTO replay_requests(event_id,operator,status,reason,prompt_version)
    VALUES($1,$2,'approved','operator-approved',$3)`, [eventId, operator, version]);
  await client.query(`INSERT INTO ops_audit(operator,action,target,result)
    VALUES($1,'replay_approved',$2,'operator_approval_written')`, [operator,eventId]);
}

// An approved discard closes the DLQ item: it leaves the list and can never be replayed.
async function discard(client, eventId, operator) {
  const result = await client.query(`SELECT replayed_at, discarded_at
    FROM dead_letters WHERE event_id=$1 FOR UPDATE`, [eventId]);
  const row = result.rows[0];
  if (!row) throw new ReplayRefusal(404, 'dead-letter item not found', 'not_found');
  if (row.discarded_at) throw new ReplayRefusal(409, 'event was already discarded', 'already_discarded');
  if (row.replayed_at) throw new ReplayRefusal(409, 'event has already been replayed', 'already_replayed');
  const previous = await client.query('SELECT event_id FROM replay_requests WHERE event_id=$1', [eventId]);
  if (previous.rows[0]) throw new ReplayRefusal(409, 'a replay has already been approved or started', 'approval_already_exists');
  await client.query('UPDATE dead_letters SET discarded_at=now(), discarded_by=$2 WHERE event_id=$1', [eventId, operator]);
  await client.query(`INSERT INTO ops_audit(operator,action,target,result)
    VALUES($1,'dlq_discarded',$2,'operator_approval_written')`, [operator, eventId]);
}

app.post('/api/dlq/:eventId/replay', mutationsDisabledInPublicDemo, openModeWritesLocalOnly, authorized, async (req, res, next) => {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await queueReplay(client, req.params.eventId, operatorName(req), promptVersion);
    await client.query('COMMIT');
    res.status(202).json({ event_id: req.params.eventId, status: 'approved', reused_idempotency_key: true });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    if (error instanceof ReplayRefusal) {
      await client.query(`INSERT INTO ops_audit(operator,action,target,result)
        VALUES($1,'replay_refused',$2,$3)`, [operatorName(req),req.params.eventId,error.reason]).catch(() => {});
      return res.status(error.status).json({ error: error.message });
    }
    next(error);
  } finally { client?.release(); }
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

app.put('/api/controls/producers/:producer', mutationsDisabledInPublicDemo, openModeWritesLocalOnly, authorized, async (req, res, next) => {
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

app.put('/api/controls/guardrails/:key', mutationsDisabledInPublicDemo, openModeWritesLocalOnly, authorized, async (req, res, next) => {
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
      SELECT p.id, p.event_id::text, p.action, p.rationale, p.risk, p.prompt_version, p.model,
             p.input_tokens, p.output_tokens, p.source, p.cost_usd, p.usage_uncertain,
             p.approved, p.approved_by, p.created_at, rr.status AS replay_status,
             rr.reason AS replay_reason, rr.workflow_id AS replay_workflow_id
      FROM agent_proposals p LEFT JOIN replay_requests rr ON rr.event_id=p.event_id
      ORDER BY p.created_at DESC LIMIT 100`);
    res.json({ items: result.rows });
  } catch (error) { next(error); }
});

app.get('/api/agent/usage', authorized, async (_req, res, next) => {
  try {
    const result = await pool.query(`SELECT day, input_tokens, output_tokens, budget_tokens, cost_usd
      FROM agent_daily_usage WHERE day=(now() AT TIME ZONE 'UTC')::date`);
    res.json({ day: new Date().toISOString().slice(0, 10), input_tokens: 0, output_tokens: 0,
      budget_tokens: 0, cost_usd: 0, ...result.rows[0], daily_budget: agentConfig.budget,
      priced: agentConfig.inputRate > 0 || agentConfig.outputRate > 0 });
  } catch (error) { next(error); }
});

app.post('/api/agent/evaluate/:eventId', mutationsDisabledInPublicDemo, openModeWritesLocalOnly, authorized, async (req, res, next) => {
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

app.post('/api/agent/proposals/:id/approve', mutationsDisabledInPublicDemo, openModeWritesLocalOnly, authorized, async (req, res, next) => {
  const operator = operatorName(req);
  const id = Number(req.params.id);
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const found = await client.query(`SELECT id,event_id::text,action,prompt_version FROM agent_proposals
      WHERE id=$1 AND approved IS NULL FOR UPDATE`, [id]);
    const proposal = found.rows[0];
    if (!proposal) throw new ReplayRefusal(409, 'proposal missing or already decided', 'already_decided');
    if (proposal.action === 'replay') await queueReplay(client, proposal.event_id, operator, proposal.prompt_version);
    if (proposal.action === 'discard') await discard(client, proposal.event_id, operator);
    const result = await client.query(`UPDATE agent_proposals SET approved=true,approved_by=$2
      WHERE id=$1 RETURNING id,event_id::text,action,rationale,prompt_version`, [id,operator]);
    await client.query(`INSERT INTO ops_audit(operator,action,target,result)
      VALUES($1,'agent_proposal_approved',$2,$3)`, [operator,proposal.event_id,proposal.action]);
    await client.query('COMMIT');
    res.json({ ...result.rows[0], replay_status: proposal.action === 'replay' ? 'approved' : null,
      discarded: proposal.action === 'discard' });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    if (error instanceof ReplayRefusal) return res.status(error.status).json({ error: error.message });
    next(error);
  } finally { client?.release(); }
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
