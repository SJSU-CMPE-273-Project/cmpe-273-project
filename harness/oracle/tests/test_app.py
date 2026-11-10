from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from oracle.app import create_app
from oracle.recorder import Recorder


class FakeLedger:
    def __init__(self, accounted=None):
        self.data = accounted or {}

    def accounted(self):
        return dict(self.data), "fake"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


@pytest.fixture
def ledger():
    return FakeLedger()


@pytest.fixture
def client(ledger):
    return TestClient(create_app(recorder=Recorder(), ledger=ledger))


def deliver(client, eid, provider="sms_primary", acct="a1", seq=1, path="/sms", key=None):
    msg = {"event_id": eid, "account_id": acct, "sequence": seq, "occurred_at": now_iso(),
           "recipient": "+14085550123", "body": "code 482913", "provider": provider}
    return client.post(path, json=msg, headers={"Idempotency-Key": key or eid})


def checks(client):
    return {c["name"]: c for c in client.get("/report").json()["checks"]}


def test_happy_path_all_pass(client):
    client.post("/expected", json=[{"eventId": "e1", "accountId": "a1", "sequence": "1"},
                                   {"event_id": "e2", "account_id": "a1", "sequence": 2}])
    assert deliver(client, "e1", seq=1).status_code == 200
    assert deliver(client, "e2", seq="2").status_code == 200  # proto JSON sends uint64 as string
    rep = client.get("/report").json()
    assert rep["passed"] is True
    assert {c["name"]: c["result"] for c in rep["checks"]} == {
        "no loss": "PASS", "no duplicates": "PASS", "per-account order": "PASS",
        "latency": "PASS", "no late delivery": "SKIP"}


def test_same_provider_retry_is_absorbed_not_a_duplicate(client):
    first = deliver(client, "e1").json()
    again = deliver(client, "e1").json()   # worker retried after a lost ack
    assert again["idempotent_replay"] is True
    assert again["provider_message_id"] == first["provider_message_id"]
    assert checks(client)["no duplicates"]["result"] == "PASS"
    assert client.get("/report").json()["provider_calls"]["idempotent_replay"] == 1


def test_failover_after_success_is_a_duplicate(client):
    deliver(client, "e1", provider="sms_primary")
    deliver(client, "e1", provider="sms_secondary")  # primary had delivered, worker failed over anyway
    assert checks(client)["no duplicates"]["result"] == "FAIL"


def test_naive_provider_counts_every_retry():
    c = TestClient(create_app(recorder=Recorder(honor_idempotency=False), ledger=FakeLedger()))
    deliver(c, "e1")
    deliver(c, "e1")
    assert checks(c)["no duplicates"]["result"] == "FAIL"


def test_missing_message_fails_no_loss(client):
    client.post("/expected", json=[{"event_id": "e1"}, {"event_id": "e2"}])
    deliver(client, "e1")
    c = checks(client)["no loss"]
    assert c["result"] == "FAIL" and c["detail"]["missing_sample"] == ["e2"]


def test_t5_poison_goes_to_dlq_and_replay_delivers_once(client, ledger):
    client.post("/expected", json=[{"event_id": "p", "account_id": "a1", "sequence": 2}])
    client.post("/admin/rules", json={"event_id": "p", "action": "reject"})
    deliver(client, "e1", seq=1)
    assert deliver(client, "p", seq=2).status_code == 422
    deliver(client, "e3", seq=3)
    ledger.data["p"] = "dead_lettered"                 # worker dead-letters it
    assert checks(client)["no loss"]["result"] == "PASS"
    client.delete("/admin/rules")                      # fix the poison, operator replays
    assert deliver(client, "p", seq=2).status_code == 200
    c = checks(client)
    assert c["no duplicates"]["result"] == "PASS"
    assert c["per-account order"]["result"] == "PASS"  # replay exempt from order


def test_t4_fail_twice_then_accept(client):
    client.post("/admin/rules", json={"event_id": "n", "action": "fail", "times": 2})
    assert deliver(client, "n").status_code == 503
    assert deliver(client, "n").status_code == 503
    assert deliver(client, "n").status_code == 200
    assert client.get("/admin/rules").json()[0]["used"] == 2


def test_out_of_order_fails(client):
    deliver(client, "e2", seq=2)
    deliver(client, "e1", seq=1)
    assert checks(client)["per-account order"]["result"] == "FAIL"


def test_reset_starts_a_new_run(client):
    deliver(client, "e1")
    old = client.get("/health").json()["run_id"]
    new = client.post("/admin/reset").json()["run_id"]
    assert new != old and client.get("/health").json()["delivered"] == 0


def test_text_report(client):
    deliver(client, "e1")
    text = client.get("/report?format=text").text
    assert "no duplicates" in text and "OVERALL: PASS" in text


def test_report_and_logs_never_contain_pii(client, capsys):
    deliver(client, "e1")
    assert "4085550123" not in client.get("/report").text
    assert "482913" not in client.get("/deliveries").text
    out = capsys.readouterr()
    assert "4085550123" not in out.out + out.err


def test_bad_inputs(client):
    assert client.post("/sms", content=b"nope").status_code == 400
    assert client.post("/sms", json={"body": "x"}).status_code == 400
    assert client.post("/admin/rules", json={"event_id": "x", "action": "explode"}).status_code == 400
    assert client.post("/expected", json=[{"nope": 1}]).status_code == 400
    assert client.post("/fax", json={"event_id": "x"}).status_code in (404, 405)


def test_admin_token(monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", "s3cret")
    c = TestClient(create_app(recorder=Recorder(), ledger=FakeLedger()))
    assert c.post("/admin/reset").status_code == 401
    assert c.post("/admin/reset", headers={"Authorization": "Bearer s3cret"}).status_code == 200
    assert deliver(c, "e1").status_code == 200  # provider endpoints stay open for the adapters
