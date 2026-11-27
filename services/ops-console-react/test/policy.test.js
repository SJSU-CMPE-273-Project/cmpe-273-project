'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

for (const version of ['dlq-v1', 'dlq-v2']) test(`${version} treats DLQ message content as untrusted data`, () => {
  const prompt = fs.readFileSync(path.join(__dirname, `../../../agent/prompts/${version}.md`), 'utf8');
  assert.match(prompt, /never replay directly/i);
  assert.match(prompt, /untrusted data/i);
  assert.match(prompt, /human approval/i);
  assert.match(prompt, /Never expose PII/i);
});

test('Ops SQL creates replay and audit tables', () => {
  const sql = fs.readFileSync(path.join(__dirname, '../../../infra/postgres/init/005_ops_console.sql'), 'utf8');
  assert.match(sql, /CREATE TABLE IF NOT EXISTS replay_requests/);
  assert.match(sql, /CREATE TABLE IF NOT EXISTS ops_audit/);
  assert.match(sql, /CREATE TABLE IF NOT EXISTS agent_proposals/);
});
