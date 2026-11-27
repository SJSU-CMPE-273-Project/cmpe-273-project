-- Lane 4 console data for replay proposals and operator audit.
CREATE TABLE IF NOT EXISTS replay_requests (
  event_id UUID PRIMARY KEY,
  operator TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('approved','started','refused','failed')),
  reason TEXT,
  prompt_version TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops_audit (
  id BIGSERIAL PRIMARY KEY,
  operator TEXT NOT NULL,
  action TEXT NOT NULL,
  target TEXT,
  result TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_proposals (
  id BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('replay','discard','escalate')),
  rationale TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  approved BOOLEAN,
  approved_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Console metrics also work on older volumes before the ingest identity upgrade.
CREATE OR REPLACE VIEW ingest_rejection_counts AS
  SELECT count(*) AS total FROM ingest_rejections;
