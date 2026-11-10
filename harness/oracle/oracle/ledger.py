"""Reads which events the system *deliberately* did not deliver.

"No loss" means delivered OR expired OR dead-lettered. The oracle sees
deliveries itself, but expiry and dead-lettering happen inside the delivery
worker, which records them in Postgres (deliveries.state). We read only those
two states: we never trust the worker's own word that something was delivered.
"""
from __future__ import annotations

import logging

log = logging.getLogger("oracle.ledger")

QUERY = "SELECT event_id::text, state::text FROM deliveries WHERE state IN ('expired', 'dead_lettered')"


class Ledger:
    def __init__(self, dsn: str | None):
        self.dsn = dsn or None

    def accounted(self) -> tuple[dict[str, str], str]:
        """Returns ({event_id: state}, status note for the report)."""
        if not self.dsn:
            return {}, "not configured (DATABASE_URL unset): expired/dead-lettered events count as missing"
        try:
            import psycopg  # imported lazily so unit tests don't need a database driver

            with psycopg.connect(self.dsn, connect_timeout=3) as conn:
                rows = conn.execute(QUERY).fetchall()
            return {eid: state for eid, state in rows}, f"connected ({len(rows)} expired/dead-lettered)"
        except Exception as e:  # report it rather than crash the report
            log.warning("ledger unavailable: %s", e.__class__.__name__)
            return {}, f"unavailable ({e.__class__.__name__}): expired/dead-lettered events count as missing"
