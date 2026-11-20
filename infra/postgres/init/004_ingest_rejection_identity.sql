-- Apply to existing databases with psql -v ON_ERROR_STOP=1.
BEGIN;

ALTER TABLE ingest_rejections
    ADD COLUMN IF NOT EXISTS event_id UUID,
    ADD COLUMN IF NOT EXISTS topic_created_at NUMERIC(20,0) NOT NULL DEFAULT 0
        CHECK (topic_created_at BETWEEN 0 AND 18446744073709551615);

-- Zero marks legacy audit rows whose topic instance cannot be established.
ALTER TABLE ingest_rejections
    ALTER COLUMN topic_created_at DROP DEFAULT,
    DROP CONSTRAINT ingest_rejections_pkey,
    ADD PRIMARY KEY (topic_created_at, partition_id, source_offset);

CREATE INDEX IF NOT EXISTS ingest_rejections_event_id
    ON ingest_rejections (event_id) WHERE event_id IS NOT NULL;

CREATE OR REPLACE VIEW ingest_rejection_counts AS
    SELECT count(*) AS total FROM ingest_rejections;

COMMIT;
