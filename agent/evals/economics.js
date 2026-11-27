'use strict';
// Reproducible zero-call baseline and conservative request bounds, not model measurements.
const agent = require('../../services/ops-console-react/agent');
const cases = require('./dlq.json');
(async () => {
  const c = { ...agent.config(), key: '' };
  const rows = [];
  for (const item of cases) {
    const data = agent.summary(item.row, item.attempts);
    const inputBound = Buffer.byteLength(JSON.stringify(agent.request(c, data))) + 4096;
    const result = await agent.evaluate(item.row, item.attempts, c, {
      reserve() { throw new Error('No external calls allowed for baseline'); },
    });
    rows.push({ name: item.name, input_bound: inputBound, output_cap: c.maxOutput,
      total_reserved: inputBound + c.maxOutput, cost_bound_usd: agent.cost(inputBound, c.maxOutput, c),
      baseline: result });
  }
  console.log(JSON.stringify({ kind: 'offline-baseline-and-projection', prompt_version: c.version,
    model: c.model, scenarios: rows.length, external_calls: 0, measured_tokens: 0, measured_cost_usd: 0,
    daily_token_budget: c.budget, daily_cost_bound_usd: c.budget * Math.max(c.inputRate, c.outputRate) / 1e6,
    rows }, null, 2));
})();
