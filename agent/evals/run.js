'use strict';
// Real model evaluation. No mock mode: this must fail when credentials are absent.
const fs = require('node:fs');
const assert = require('node:assert/strict');
const agent = require('../../services/ops-console-react/agent');
const cases = require('./dlq.json');
async function main() {
  const config = agent.config();
  if (!config.key) throw new Error('OPENAI_API_KEY is required for live prompt evaluation');
  let used = 0;
  const accounting = { async reserve(tokens, budget) { if (used + tokens > budget) return null; used += tokens; return true; },
    async settle(_ticket, reserve, charged) { used += charged - reserve; } };
  const results = [];
  for (const item of cases) {
    const result = await agent.evaluate(item.row, item.attempts, config, accounting);
    results.push({ name: item.name, expected: item.expected, ...result });
  }
  const input = results.reduce((n, r) => n + r.input_tokens, 0);
  const output = results.reduce((n, r) => n + r.output_tokens, 0);
  const report = { at: new Date().toISOString(), kind: 'live', prompt_version: config.version,
    model: config.model, input_tokens: input, output_tokens: output,
    cost_usd: results.reduce((n, r) => n + r.cost_usd, 0), results };
  if (process.env.AGENT_EVAL_REPORT) fs.writeFileSync(process.env.AGENT_EVAL_REPORT, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify(report, null, 2));
  for (const result of results) {
    assert.equal(result.source, 'model', `${result.name}: a fallback is not a passing model eval`);
    assert.equal(result.action, result.expected, result.name);
  }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
