-- Apply to existing installations with psql; init scripts only run on fresh volumes.
-- Original payload is needed for replay. It contains PII; restrict DB/DLQ access.
ALTER TABLE dead_letters ADD COLUMN IF NOT EXISTS original BYTEA;
ALTER TABLE dead_letters ADD COLUMN IF NOT EXISTS replay_token TEXT;
