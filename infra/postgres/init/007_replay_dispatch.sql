-- Additive dispatch metadata; apply after 005_ops_console.sql.
ALTER TABLE replay_requests ADD COLUMN IF NOT EXISTS workflow_id TEXT;
ALTER TABLE replay_requests ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE replay_requests ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS replay_requests_approved
  ON replay_requests (created_at, event_id) WHERE status='approved';
