'use strict';
const fs = require('node:fs');
const path = require('node:path');

const schema = {
  type: 'object', additionalProperties: false, required: ['action', 'rationale', 'risk'],
  properties: { action: { type: 'string', enum: ['replay', 'discard', 'escalate'] },
    rationale: { type: 'string' }, risk: { type: 'string', enum: ['low', 'medium', 'high'] } },
};
function config(env = process.env) {
  const version = env.AGENT_PROMPT_VERSION || 'dlq-v1';
  if (!/^dlq-v[1-9][0-9]*$/.test(version)) throw new Error('Invalid prompt version');
  const number = (key, value, integer = false) => {
    const n = Number(env[key] ?? value);
    if (!Number.isFinite(n) || n < 0 || (integer && !Number.isSafeInteger(n))) throw new Error(`Invalid ${key}`);
    return n;
  };
  return { version, model: env.AGENT_MODEL || 'gpt-4.1-mini', key: env.OPENAI_API_KEY,
    prompt: fs.readFileSync(path.join(__dirname, '../../agent/prompts', `${version}.md`), 'utf8'),
    budget: number('AGENT_DAILY_TOKEN_BUDGET', 100000, true),
    inputRate: number('AGENT_INPUT_USD_PER_MILLION', 0.40),
    outputRate: number('AGENT_OUTPUT_USD_PER_MILLION', 1.60), maxOutput: 256 };
}
// Only closed vocabularies cross the model boundary. Raw logs may contain PII.
function summary(row, attempts, now = new Date()) {
  const reasons = ['poison_after_fix', 'poison', 'provider_rejection_after_fix', 'provider_rejection',
    'provider_outage', 'channels_exhausted', 'invalid_kafka_record'];
  const providers = ['sms_primary', 'sms_secondary', 'push', 'email', 'sms-primary', 'sms-secondary', 'push-email', 'primary', 'secondary', 'oracle'];
  const codes = ['accepted', 'provider_recovered', 'activity_error', 'interrupted_or_pending',
    'expired', 'ambiguous', 'invalid_request', 'rate_limited', 'unavailable', 'rejected', '', 'permanent_rejection', 'transient_error', 'timeout', 'circuit_open', 'bulkhead_full'];
  return { priority: [0, 1, 2, 3].includes(row.priority) ? row.priority : 0,
    reason: reasons.includes(row.reason) ? row.reason : 'unknown',
    expired: Boolean(row.expires_at && new Date(row.expires_at) <= now),
    attempts: attempts.slice(-20).map(a => ({
      provider: providers.includes(a.provider) ? a.provider : 'other',
      code: codes.includes(a.code ?? a.error) || /^[1-5][0-9]{2}$/.test(a.code ?? a.error ?? '') ? String(a.code ?? a.error) : 'unknown',
      time: Number.isFinite(new Date(a.at).getTime()) ? new Date(a.at).toISOString() : null,
    })) };
}
function fallback(data, source) {
  let action = 'escalate';
  let rationale = 'The failure is unclear; keep it in the DLQ for operator review.';
  if (data.expired) { action = 'discard'; rationale = 'The message has expired and must not be delivered late.'; }
  else if (/reject|poison/.test(data.reason)) {
    action = 'replay'; rationale = 'Provider rejection; replay only after the provider is fixed and a human approves.';
  } else if (/provider|outage/.test(data.reason)) rationale = 'Verify provider recovery before replay.';
  return { action, rationale, risk: action === 'discard' ? 'low' : 'high', source };
}
function request(config, data) {
  return { model: config.model, messages: [{ role: 'system', content: config.prompt },
    { role: 'user', content: JSON.stringify(data) }], max_completion_tokens: config.maxOutput,
    response_format: { type: 'json_schema', json_schema: { name: 'dlq_proposal', strict: true, schema } } };
}
function validate(value) {
  return value && Object.keys(value).sort().join(',') === 'action,rationale,risk' &&
    schema.properties.action.enum.includes(value.action) && schema.properties.risk.enum.includes(value.risk) &&
    typeof value.rationale === 'string' && value.rationale.trim().length > 0 && value.rationale.length <= 1000;
}
function cost(input, output, c) { return (input * c.inputRate + output * c.outputRate) / 1e6; }
async function evaluate(row, attempts, c, accounting, fetcher = fetch) {
  const data = summary(row, attempts);
  const base = { prompt_version: c.version, model: 'rules', input_tokens: 0, output_tokens: 0, cost_usd: 0 };
  if (!c.key) return { ...base, ...fallback(data, 'model_unavailable') };
  const body = request(c, data);
  // UTF-8 bytes bound text tokens conservatively; extra space covers schema/chat framing.
  const inputBound = Buffer.byteLength(JSON.stringify(body)) + 4096;
  const reserve = inputBound + c.maxOutput;
  const ticket = await accounting.reserve(reserve, c.budget);
  if (!ticket) return { ...base, ...fallback(data, 'budget_exhausted') };
  let usage;
  let result;
  try {
    const response = await fetcher('https://api.openai.com/v1/chat/completions', {
      method: 'POST', headers: { Authorization: `Bearer ${c.key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body), redirect: 'error', signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error('Model unavailable');
    const json = (await response.json().catch(() => null)) || {};
    if (Number.isSafeInteger(json.usage?.prompt_tokens) && json.usage.prompt_tokens >= 0 &&
        Number.isSafeInteger(json.usage?.completion_tokens) && json.usage.completion_tokens >= 0) usage = json.usage;
    const choice = json.choices?.[0];
    let value;
    try { value = JSON.parse(choice?.message?.content); } catch { /* fail closed */ }
    result = choice?.finish_reason === 'stop' && !choice?.message?.refusal && validate(value) && usage
      ? { ...value, source: 'model' }
      : { action: 'escalate', rationale: 'Model response failed validation; human review required.', risk: 'high', source: 'invalid_output' };
    // TTL is enforced even if the model returns a valid but unsafe recommendation.
    if (data.expired && result.action === 'replay') result = {
      action: 'escalate', rationale: 'Model attempted replay of an expired message; human review required.', risk: 'high', source: 'invalid_output' };
  } catch {
    result = fallback(data, 'model_unavailable');
  }
  const input = usage?.prompt_tokens || 0, output = usage?.completion_tokens || 0;
  // A timeout may have been billed: keep its reservation, never retry automatically.
  const usd = usage ? cost(input, output, c) : cost(inputBound, c.maxOutput, c);
  await accounting.settle(ticket, reserve, usage ? input + output : reserve, input, output, usd);
  return { ...base, ...result, model: c.model, input_tokens: input, output_tokens: output,
    cost_usd: usd, usage_uncertain: !usage };
}
function accounting(pool) {
  return {
    async reserve(tokens, budget) {
      const r = await pool.query(`INSERT INTO agent_daily_usage (day, budget_tokens)
        SELECT (now() AT TIME ZONE 'UTC')::date, $1::bigint WHERE $1::bigint <= $2::bigint
        ON CONFLICT (day) DO UPDATE SET budget_tokens=agent_daily_usage.budget_tokens + $1
        WHERE agent_daily_usage.budget_tokens + $1 <= $2 RETURNING day::text`, [tokens, budget]);
      return r.rows[0]?.day;
    },
    async settle(day, reserve, charged, input, output, cost) {
      await pool.query(`UPDATE agent_daily_usage SET budget_tokens=budget_tokens-$2+$3,
        input_tokens=input_tokens+$4, output_tokens=output_tokens+$5, cost_usd=cost_usd+$6
        WHERE day=$1`, [day, reserve, charged, input, output, cost]);
    },
  };
}
module.exports = { config, summary, fallback, request, validate, evaluate, accounting, cost };
