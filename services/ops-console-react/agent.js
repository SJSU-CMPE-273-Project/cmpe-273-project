'use strict';
const fs = require('node:fs');
const path = require('node:path');

const schema = {
  type: 'object', additionalProperties: false, required: ['action', 'rationale', 'risk'],
  properties: { action: { type: 'string', enum: ['replay', 'discard', 'escalate'] },
    rationale: { type: 'string' }, risk: { type: 'string', enum: ['low', 'medium', 'high'] } },
};
function config(env = process.env) {
  const version = env.AGENT_PROMPT_VERSION || 'dlq-v2';
  if (!/^dlq-v[1-9][0-9]*$/.test(version)) throw new Error('Invalid prompt version');
  const provider = env.AGENT_PROVIDER || 'gemini';
  if (!['gemini', 'openai'].includes(provider)) throw new Error('Invalid AGENT_PROVIDER');
  const gemini = provider === 'gemini';
  const number = (key, value, integer = false) => {
    const n = Number(env[key] === '' ? value : (env[key] ?? value));
    if (!Number.isFinite(n) || n < 0 || (integer && !Number.isSafeInteger(n))) throw new Error(`Invalid ${key}`);
    return n;
  };
  return { version, provider, model: env.AGENT_MODEL || (gemini ? 'gemini-3.5-flash-lite' : 'gpt-4.1-mini'),
    key: gemini ? env.GEMINI_API_KEY : env.OPENAI_API_KEY,
    keyVariable: gemini ? 'GEMINI_API_KEY' : 'OPENAI_API_KEY',
    endpoint: gemini ? 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions'
      : 'https://api.openai.com/v1/chat/completions',
    prompt: fs.readFileSync(path.join(__dirname, '../../agent/prompts', `${version}.md`), 'utf8'),
    budget: number('AGENT_DAILY_TOKEN_BUDGET', 100000, true),
    inputRate: number('AGENT_INPUT_USD_PER_MILLION', gemini ? 0 : 0.40),
    outputRate: number('AGENT_OUTPUT_USD_PER_MILLION', gemini ? 0 : 1.60), maxOutput: gemini ? 1024 : 256 };
}
// Only closed vocabularies cross the model boundary. Raw logs may contain PII.
function summary(row, attempts, now = new Date()) {
  const reasons = ['poison_after_fix', 'poison', 'provider_rejection_after_fix', 'provider_rejection',
    'provider_outage', 'channels_exhausted', 'invalid_kafka_record'];
  const providers = ['sms_primary', 'sms_secondary', 'push', 'email', 'sms-primary', 'sms-secondary', 'push-email', 'primary', 'secondary', 'oracle'];
  const codes = ['accepted', 'provider_recovered', 'activity_error', 'interrupted_or_pending',
    'expired', 'ambiguous', 'invalid_request', 'rate_limited', 'unavailable', 'rejected', '', 'permanent_rejection', 'transient_error', 'timeout', 'circuit_open', 'bulkhead_full'];
  const history = attempts.slice(-20).map(a => ({
    provider: providers.includes(a.provider) ? a.provider : 'other',
    code: codes.includes(a.code ?? a.error) || /^[1-5][0-9]{2}$/.test(a.code ?? a.error ?? '') ? String(a.code ?? a.error) : 'unknown',
    time: Number.isFinite(new Date(a.at).getTime()) ? new Date(a.at).toISOString() : null,
  }));
  let reason = reasons.includes(row.reason) ? row.reason : 'unknown';
  // The worker records channels_exhausted for any message that ran out of channels.
  // When every attempt was a permanent rejection (e.g. T5's poison: rejected on SMS
  // primary, SMS secondary and push), say so, so the model and the rules see a
  // rejection rather than an unclear failure. Mixed or transient failures stay as is.
  if (reason === 'channels_exhausted' && history.length > 0 && history.every(a => isRejection(a.code))) {
    reason = 'provider_rejection';
  }
  return { priority: [0, 1, 2, 3].includes(row.priority) ? row.priority : 0,
    reason, expired: Boolean(row.expires_at && new Date(row.expires_at) <= now), attempts: history };
}
function isRejection(code) {
  return ['rejected', 'permanent_rejection', 'invalid_request'].includes(code) || (/^4[0-9]{2}$/.test(code) && !['408', '429'].includes(code));
}
function fallback(data, source) {
  let action = 'escalate';
  let rationale = 'The failure is unclear; keep it in the DLQ for operator review.';
  // Same order as the dlq-v2 prompt: expired → discard; confirmed fix → replay;
  // a rejection without a confirmed fix stays escalated until a human confirms the fix.
  if (data.expired) { action = 'discard'; rationale = 'The message has expired and must not be delivered late.'; }
  else if (/_after_fix$/.test(data.reason)) {
    action = 'replay'; rationale = 'Provider rejection was fixed; replay once with human approval.';
  } else if (/reject|poison/.test(data.reason)) {
    rationale = 'Every channel rejected this message. Fix the cause, then replay it once with human approval.';
  } else if (/provider|outage/.test(data.reason)) rationale = 'Verify provider recovery before replay.';
  return { action, rationale, risk: action === 'discard' ? 'low' : 'high', source };
}
function request(config, data) {
  return { model: config.model, messages: [{ role: 'system', content: config.prompt },
    { role: 'user', content: JSON.stringify(data) }],
    ...(config.provider === 'gemini' ? { max_tokens: config.maxOutput, reasoning_effort: 'minimal' }
      : { max_completion_tokens: config.maxOutput }),
    response_format: { type: 'json_schema', json_schema: { name: 'dlq_proposal', strict: true, schema } } };
}
function validate(value) {
  return value && Object.keys(value).sort().join(',') === 'action,rationale,risk' &&
    schema.properties.action.enum.includes(value.action) && schema.properties.risk.enum.includes(value.risk) &&
    typeof value.rationale === 'string' && value.rationale.trim().length > 0 && value.rationale.length <= 1000;
}
function cost(input, output, c) { return (input * c.inputRate + output * c.outputRate) / 1e6; }
// Diagnostics contain only closed categories, never provider messages or credentials.
async function httpFailure(response) {
  const status = Number.isInteger(response.status) ? response.status : 0;
  const types = { 400: 'invalid_request', 401: 'authentication', 403: 'permission',
    404: 'model_not_found', 429: 'quota_or_rate_limit' };
  let json;
  try { json = typeof response.json === 'function' ? await response.json() : null; } catch { /* omit raw body */ }
  const codes = ['INVALID_ARGUMENT', 'UNAUTHENTICATED', 'PERMISSION_DENIED', 'NOT_FOUND',
    'RESOURCE_EXHAUSTED', 'FAILED_PRECONDITION', 'UNAVAILABLE', 'INTERNAL'];
  const reasons = ['API_KEY_INVALID', 'API_KEY_EXPIRED', 'SERVICE_DISABLED',
    'API_KEY_SERVICE_BLOCKED', 'API_KEY_HTTP_REFERRER_BLOCKED', 'API_KEY_IP_ADDRESS_BLOCKED'];
  const reason = Array.isArray(json?.error?.details)
    ? json.error.details.map(detail => detail?.reason).find(reason => reasons.includes(reason)) : undefined;
  return { type: types[status] || (status >= 500 ? 'provider_unavailable' : 'http_error'),
    http_status: status, ...(codes.includes(json?.error?.status) ? { provider_code: json.error.status } : {}),
    ...(reason ? { provider_reason: reason } : {}) };
}
function networkFailure(error) {
  const code = error?.cause?.code ?? error?.code;
  const known = ['ENOTFOUND', 'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT',
    'UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'SELF_SIGNED_CERT_IN_CHAIN', 'CERT_HAS_EXPIRED'];
  return { type: ['TimeoutError', 'AbortError'].includes(error?.name) ? 'timeout' : 'network_error',
    ...(known.includes(code) ? { network_code: code } : {}) };
}
async function evaluate(row, attempts, c, accounting, fetcher = fetch) {
  const data = summary(row, attempts);
  const base = { prompt_version: c.version, model: 'rules', input_tokens: 0, output_tokens: 0, cost_usd: 0 };
  if (!c.key) return { ...base, ...fallback(data, 'model_unavailable'), model_error: { type: 'missing_key' } };
  const body = request(c, data);
  // UTF-8 bytes bound text tokens conservatively; extra space covers schema/chat framing.
  const inputBound = Buffer.byteLength(JSON.stringify(body)) + 4096;
  const reserve = inputBound + c.maxOutput;
  const ticket = await accounting.reserve(reserve, c.budget);
  if (!ticket) return { ...base, ...fallback(data, 'budget_exhausted') };
  let usage;
  let result;
  let modelError;
  try {
    const response = await fetcher(c.endpoint, {
      method: 'POST', headers: { Authorization: `Bearer ${c.key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body), redirect: 'error', signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) {
      modelError = await httpFailure(response);
      throw new Error('Model unavailable');
    }
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
  } catch (error) {
    modelError ||= networkFailure(error);
    result = fallback(data, 'model_unavailable');
  }
  const input = usage?.prompt_tokens || 0, output = usage?.completion_tokens || 0;
  // A timeout may have been billed: keep its reservation, never retry automatically.
  const usd = usage ? cost(input, output, c) : cost(inputBound, c.maxOutput, c);
  await accounting.settle(ticket, reserve, usage ? input + output : reserve, input, output, usd);
  return { ...base, ...result, model: c.model, input_tokens: input, output_tokens: output,
    cost_usd: usd, usage_uncertain: !usage, ...(modelError ? { model_error: modelError } : {}) };
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
module.exports = { config, summary, fallback, request, validate, evaluate, accounting, cost, httpFailure, networkFailure };
