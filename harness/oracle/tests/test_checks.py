from datetime import datetime, timedelta, timezone

from oracle.checks import FAIL, PASS, SKIP, Expected, Receipt, check_latency, check_no_duplicates, \
    check_no_loss, check_order, check_ttl, percentile

T0 = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)


def r(eid, acct="a1", seq=None, provider="sms_primary", ms=100):
    return Receipt(event_id=eid, provider=provider, received_at=T0 + timedelta(milliseconds=ms),
                   account_id=acct, sequence=seq, occurred_at=T0)


def test_percentile_nearest_rank():
    vals = list(range(1, 101))
    assert percentile(vals, 50) == 50
    assert percentile(vals, 99) == 99
    assert percentile([7], 99) == 7


def test_no_loss_counts_dead_lettered_and_expired_as_accounted():
    exp = {e: Expected(e) for e in ["e1", "e2", "e3", "e4"]}
    c = check_no_loss(exp, [r("e1")], {"e2": "dead_lettered", "e3": "expired"})
    assert c.result == FAIL
    assert c.detail["missing_sample"] == ["e4"]
    assert "1 delivered" in c.summary and "1 dead-lettered" in c.summary and "1 missing" in c.summary


def test_no_loss_passes_and_skips():
    assert check_no_loss({"e1": Expected("e1")}, [r("e1")], {}).result == PASS
    assert check_no_loss({}, [], {}).result == SKIP


def test_duplicate_across_providers_fails():
    c = check_no_duplicates([r("e1", provider="sms_primary"), r("e1", provider="sms_secondary"), r("e2")])
    assert c.result == FAIL
    assert c.detail["sample"] == [{"event_id": "e1", "providers": ["sms_primary", "sms_secondary"]}]


def test_order_detects_regression_per_account_only():
    rs = [r("e1", "a1", 1), r("e5", "b2", 5), r("e3", "a1", 3), r("e2", "a1", 2), r("e6", "b2", 6)]
    c = check_order(rs, exempt=set())
    assert c.result == FAIL
    assert c.detail["violations"] == 1
    assert c.detail["sample"][0] == {"account_id": "a1", "sequence": 2, "arrived_after": 3, "event_id": "e2"}


def test_order_exempts_dead_lettered_replay():
    # e2 was poison -> DLQ; replayed later, so it arrives after e3 by design.
    rs = [r("e1", "a1", 1), r("e3", "a1", 3), r("e2", "a1", 2)]
    assert check_order(rs, exempt={"e2"}).result == PASS


def test_order_gaps_are_fine():
    assert check_order([r("e1", "a1", 1), r("e9", "a1", 9)], exempt=set()).result == PASS


def test_latency_against_target():
    rs = [r(f"e{i}", ms=i * 10) for i in range(1, 101)]  # 10..1000 ms
    ok = check_latency(rs, p99_target_ms=1000)
    assert ok.result == PASS and ok.detail["p99_ms"] == 990 and ok.detail["p50_ms"] == 500
    assert check_latency(rs, p99_target_ms=500).result == FAIL


def test_latency_uses_first_delivery_only():
    rs = [r("e1", ms=100), r("e1", provider="sms_secondary", ms=9000)]
    assert check_latency(rs, 1000).detail["max_ms"] == 100


def test_ttl_flags_late_delivery():
    exp = {"otp": Expected("otp", expires_at=T0 + timedelta(milliseconds=50)),
           "ok": Expected("ok", expires_at=T0 + timedelta(seconds=5))}
    c = check_ttl(exp, [r("otp", ms=100), r("ok", ms=100)])
    assert c.result == FAIL and c.detail["sample"] == [{"event_id": "otp", "late_by_ms": 50}]
