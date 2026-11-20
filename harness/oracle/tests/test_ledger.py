"""Exercise the actual ledger query against an isolated Postgres schema."""
import os
import uuid

import pytest

from oracle.ledger import Ledger
from oracle.checks import Expected, check_no_loss


def test_postgres_accounts_for_expiry_dlq_and_replay():
    dsn = os.getenv("ORACLE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("set ORACLE_TEST_DATABASE_URL for the ledger integration test")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    schema = "oracle_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            conn.execute("CREATE TABLE deliveries(event_id text PRIMARY KEY, state text)")
            conn.execute("CREATE TABLE dead_letters(event_id text PRIMARY KEY)")
            conn.execute("INSERT INTO deliveries VALUES ('expired', 'expired'), ('poison', 'dead_lettered'), ('normal', 'delivered')")
            conn.execute("INSERT INTO dead_letters VALUES ('poison')")
            ledger = Ledger(make_conninfo(dsn, options=f"-c search_path={schema}"))
            accounted, status = ledger.accounted()
            assert accounted == {"expired": "expired", "poison": "dead_lettered"}
            assert "connected" in status
            # The worker's delivered assertion alone never passes no-loss.
            assert check_no_loss({"normal": Expected("normal")}, [], accounted).result == "FAIL"
            assert check_no_loss({"poison": Expected("poison"), "expired": Expected("expired")}, [], accounted).result == "PASS"
            conn.execute("UPDATE deliveries SET state = 'delivered' WHERE event_id = 'poison'")
            # Historical DLQ exemption survives successful or in-progress replay.
            assert ledger.accounted()[0]["poison"] == "dead_lettered"
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
