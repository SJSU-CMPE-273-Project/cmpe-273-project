-- Apply after 005_ops_console.sql, including on existing volumes.
ALTER TABLE agent_proposals ADD COLUMN IF NOT EXISTS model TEXT NOT NULL DEFAULT 'rules';
ALTER TABLE agent_proposals ADD COLUMN IF NOT EXISTS input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens >= 0);
ALTER TABLE agent_proposals ADD COLUMN IF NOT EXISTS output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens >= 0);
ALTER TABLE agent_proposals ADD COLUMN IF NOT EXISTS risk TEXT NOT NULL DEFAULT 'high' CHECK (risk IN ('low','medium','high'));
ALTER TABLE agent_proposals ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'legacy_rules';
ALTER TABLE agent_proposals ADD COLUMN IF NOT EXISTS cost_usd NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE agent_proposals ADD COLUMN IF NOT EXISTS usage_uncertain BOOLEAN NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS agent_daily_usage (
  day DATE PRIMARY KEY,
  budget_tokens BIGINT NOT NULL DEFAULT 0 CHECK (budget_tokens >= 0),
  input_tokens BIGINT NOT NULL DEFAULT 0,
  output_tokens BIGINT NOT NULL DEFAULT 0,
  cost_usd NUMERIC NOT NULL DEFAULT 0
);
