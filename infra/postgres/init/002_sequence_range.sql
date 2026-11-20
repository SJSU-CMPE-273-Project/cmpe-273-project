-- Apply explicitly to an existing volume with psql -v ON_ERROR_STOP=1 -f ... .
-- A failed constraint validation requires fixing existing data, never truncation.
DO $$ BEGIN
 IF NOT EXISTS (SELECT FROM pg_constraint WHERE conname='ingested_sequence_positive') THEN
  ALTER TABLE ingested_events ADD CONSTRAINT ingested_sequence_positive CHECK (sequence > 0);
 END IF;
 IF NOT EXISTS (SELECT FROM pg_constraint WHERE conname='delivery_sequence_positive') THEN
  ALTER TABLE deliveries ADD CONSTRAINT delivery_sequence_positive CHECK (sequence > 0);
 END IF;
END $$;
