-- Apply to existing databases with psql -v ON_ERROR_STOP=1.
BEGIN;

-- Old rows carry neither a content fingerprint nor proof of a Kafka ack.
-- Adding the required fingerprint intentionally fails if such rows exist.
ALTER TABLE ingested_events
    ADD COLUMN IF NOT EXISTS payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    ADD COLUMN IF NOT EXISTS publish_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (publish_state IN ('pending', 'published'));

CREATE TABLE IF NOT EXISTS ingest_rejections (
    partition_id  INTEGER       NOT NULL CHECK (partition_id >= 0),
    source_offset NUMERIC(20,0) NOT NULL CHECK (source_offset BETWEEN 0 AND 18446744073709551615),
    reason        TEXT          NOT NULL CHECK (reason IN (
        'invalid_json_or_schema', 'invalid_fields', 'invalid_expiry',
        'wrong_partition', 'record_too_large', 'producer_disabled', 'event_id_conflict', 'sequence_conflict'
    )),
    rejected_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_id, source_offset)
);

COMMIT;
