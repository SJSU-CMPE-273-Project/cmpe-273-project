-- An approved discard closes a DLQ item without delivering it. Additive; safe to rerun.
ALTER TABLE dead_letters ADD COLUMN IF NOT EXISTS discarded_at TIMESTAMPTZ;
ALTER TABLE dead_letters ADD COLUMN IF NOT EXISTS discarded_by TEXT;
