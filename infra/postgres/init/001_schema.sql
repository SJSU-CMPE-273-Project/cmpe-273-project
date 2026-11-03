-- RelayGuard schema (Milestone 1). Runs once, on first Postgres start.
-- To re-run after edits: docker compose down -v && docker compose up -d

-- -------------------------------------------------------------------------
-- 1. Ingest dedup. The bridge inserts here BEFORE producing to Kafka:
--      INSERT ... ON CONFLICT (event_id) DO NOTHING
--    0 rows inserted = duplicate from a producer retry -> drop it.
-- -------------------------------------------------------------------------
CREATE TABLE ingested_events (
    event_id      UUID        PRIMARY KEY,
    account_id    TEXT        NOT NULL,
    sequence      BIGINT      NOT NULL,
    producer      TEXT        NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, sequence)          -- one event per account slot
);

-- -------------------------------------------------------------------------
-- 2. Delivery ledger. One row per event; the delivery worker claims a send
--    by moving it to 'delivered' exactly once. This is what makes retries,
--    worker crashes (T2) and replays (T3) safe.
-- -------------------------------------------------------------------------
CREATE TYPE delivery_state AS ENUM (
    'pending', 'retrying', 'delivered', 'expired', 'dead_lettered'
);

CREATE TABLE deliveries (
    event_id      UUID           PRIMARY KEY REFERENCES ingested_events (event_id),
    account_id    TEXT           NOT NULL,
    sequence      BIGINT         NOT NULL,
    priority      SMALLINT       NOT NULL,
    state         delivery_state NOT NULL DEFAULT 'pending',
    channel       TEXT,                     -- channel that finally succeeded
    provider      TEXT,                     -- provider that finally succeeded
    attempts      INT            NOT NULL DEFAULT 0,
    expires_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX deliveries_account_seq ON deliveries (account_id, sequence);
CREATE INDEX deliveries_state       ON deliveries (state);

-- -------------------------------------------------------------------------
-- 3. Attempt history (audit trail + what the DLQ shows an operator).
--    Never store the phone number or message body here (PII guardrail).
-- -------------------------------------------------------------------------
CREATE TABLE delivery_attempts (
    id            BIGSERIAL   PRIMARY KEY,
    event_id      UUID        NOT NULL REFERENCES deliveries (event_id),
    attempt       INT         NOT NULL,
    channel       TEXT        NOT NULL,
    provider      TEXT        NOT NULL,
    succeeded     BOOLEAN     NOT NULL,
    error         TEXT,
    latency_ms    INT,
    at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, attempt)
);

-- -------------------------------------------------------------------------
-- 4. Dead letters + replay. Replay re-publishes with the SAME event_id, so
--    the ledger above still guarantees at most one send.
-- -------------------------------------------------------------------------
CREATE TABLE dead_letters (
    event_id         UUID        PRIMARY KEY REFERENCES deliveries (event_id),
    reason           TEXT        NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at      TIMESTAMPTZ,
    replayed_by      TEXT
);

-- -------------------------------------------------------------------------
-- 5. Guardrail switches, editable from the Ops Console.
-- -------------------------------------------------------------------------
CREATE TABLE producer_controls (
    producer          TEXT    PRIMARY KEY,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,   -- kill switch
    max_per_minute    INT     NOT NULL DEFAULT 600
);

INSERT INTO producer_controls (producer) VALUES
    ('core-banking-java'),
    ('fraud-engine-python'),
    ('auth-otp-ruby');

CREATE TABLE guardrail_settings (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL,
    note   TEXT
);

INSERT INTO guardrail_settings (key, value, note) VALUES
    ('per_account_max_per_minute', '10',  'Per-customer rate limit'),
    ('max_attempts_per_channel',   '5',   'Retries before failing over'),
    ('retry_initial_ms',           '500', 'Backoff base'),
    ('retry_max_ms',               '30000', 'Backoff cap'),
    ('otp_ttl_seconds',            '300', 'OTPs are never delivered late'),
    ('provider_retry_budget_pct',  '20',  'Max share of provider calls that may be retries');
