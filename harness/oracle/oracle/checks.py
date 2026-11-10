"""The oracle's verdicts: pure functions over a snapshot of what happened.

Kept free of web/DB code so they are easy to unit-test and to explain:
each function takes plain data and returns one check result.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"


@dataclass(frozen=True)
class Expected:
    """An event a producer says it sent (registered by the load generator)."""
    event_id: str
    account_id: str | None = None
    sequence: int | None = None
    occurred_at: datetime | None = None
    expires_at: datetime | None = None


@dataclass(frozen=True)
class Receipt:
    """One message that reached the 'phone' (first call per provider + idempotency key)."""
    event_id: str
    provider: str
    received_at: datetime
    account_id: str | None = None
    sequence: int | None = None
    occurred_at: datetime | None = None


@dataclass
class Check:
    name: str
    result: str
    summary: str
    detail: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"name": self.name, "result": self.result, "summary": self.summary, "detail": self.detail}


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile: the smallest value with at least pct% of values at or below it."""
    if not values:
        raise ValueError("no values")
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def first_receipts(receipts: Iterable[Receipt]) -> list[Receipt]:
    """The first time each event reached the phone, in arrival order."""
    seen: set[str] = set()
    out = []
    for r in receipts:
        if r.event_id not in seen:
            seen.add(r.event_id)
            out.append(r)
    return out


def check_no_loss(expected: dict[str, Expected], receipts: list[Receipt], accounted: dict[str, str]) -> Check:
    """Every expected event is delivered, or the system deliberately expired / dead-lettered it."""
    if not expected:
        return Check("no loss", SKIP, "no expected events registered")
    delivered = {r.event_id for r in receipts}
    states = defaultdict(int)
    missing = []
    for eid in expected:
        if eid in delivered:
            states["delivered"] += 1
        elif eid in accounted:
            states[accounted[eid]] += 1
        else:
            missing.append(eid)
    parts = [f"{states['delivered']} delivered"]
    parts += [f"{n} {s.replace('_', '-')}" for s, n in sorted(states.items()) if s != "delivered"]
    parts.append(f"{len(missing)} missing")
    return Check(
        "no loss",
        PASS if not missing else FAIL,
        f"{len(expected)} expected: " + ", ".join(parts),
        {"expected": len(expected), **dict(states), "missing": len(missing), "missing_sample": missing[:20]},
    )


def check_no_duplicates(receipts: list[Receipt]) -> Check:
    """Each event reaches the phone at most once (across all providers and channels)."""
    by_event: dict[str, list[str]] = defaultdict(list)
    for r in receipts:
        by_event[r.event_id].append(r.provider)
    dups = {eid: provs for eid, provs in by_event.items() if len(provs) > 1}
    sample = [{"event_id": e, "providers": p} for e, p in list(dups.items())[:20]]
    return Check(
        "no duplicates",
        PASS if not dups else FAIL,
        f"{len(by_event)} unique events, {len(dups)} delivered more than once",
        {"unique": len(by_event), "duplicated": len(dups), "sample": sample},
    )


def check_order(receipts: list[Receipt], exempt: set[str]) -> Check:
    """Per account, delivered sequence numbers never go backwards.

    `exempt` holds events the system dead-lettered: when an operator replays
    one later, it arrives after its successors by design, not by mistake.
    """
    last: dict[str, tuple[int, str]] = {}
    violations = []
    checked = 0
    for r in first_receipts(receipts):
        if r.account_id is None or r.sequence is None or r.event_id in exempt:
            continue
        checked += 1
        prev = last.get(r.account_id)
        if prev and r.sequence < prev[0]:
            violations.append({"account_id": r.account_id, "sequence": r.sequence,
                               "arrived_after": prev[0], "event_id": r.event_id})
        if not prev or r.sequence > prev[0]:
            last[r.account_id] = (r.sequence, r.event_id)
    if checked == 0:
        return Check("per-account order", SKIP, "no deliveries with account_id + sequence")
    return Check(
        "per-account order",
        PASS if not violations else FAIL,
        f"{checked} deliveries across {len(last)} accounts, {len(violations)} out of order",
        {"checked": checked, "accounts": len(last), "violations": len(violations), "sample": violations[:20]},
    )


def check_latency(receipts: list[Receipt], p99_target_ms: float) -> Check:
    """Time from the bank event (occurred_at) to the phone, p50/p95/p99 against a p99 target."""
    lat = [(r.received_at - r.occurred_at).total_seconds() * 1000
           for r in first_receipts(receipts) if r.occurred_at]
    if not lat:
        return Check("latency", SKIP, "no deliveries with occurred_at")
    stats = {"count": len(lat), "p50_ms": round(percentile(lat, 50)), "p95_ms": round(percentile(lat, 95)),
             "p99_ms": round(percentile(lat, 99)), "max_ms": round(max(lat)), "p99_target_ms": p99_target_ms}
    return Check(
        "latency",
        PASS if stats["p99_ms"] <= p99_target_ms else FAIL,
        f"p50 {stats['p50_ms']} ms, p99 {stats['p99_ms']} ms (target {p99_target_ms:g} ms)",
        stats,
    )


def check_ttl(expected: dict[str, Expected], receipts: list[Receipt]) -> Check:
    """Guardrail: nothing arrives after its expires_at (an OTP delivered late is useless and risky)."""
    late = []
    with_ttl = 0
    for r in first_receipts(receipts):
        exp = expected.get(r.event_id)
        if not exp or not exp.expires_at:
            continue
        with_ttl += 1
        if r.received_at > exp.expires_at:
            late.append({"event_id": r.event_id,
                         "late_by_ms": round((r.received_at - exp.expires_at).total_seconds() * 1000)})
    if with_ttl == 0:
        return Check("no late delivery", SKIP, "no delivered events with expires_at")
    return Check(
        "no late delivery",
        PASS if not late else FAIL,
        f"{with_ttl} deliveries with a TTL, {len(late)} arrived after expiry",
        {"checked": with_ttl, "late": len(late), "sample": late[:20]},
    )


def run_all(expected, receipts, accounted, p99_target_ms) -> list[Check]:
    dead = {eid for eid, s in accounted.items() if s == "dead_lettered"}
    return [
        check_no_loss(expected, receipts, accounted),
        check_no_duplicates(receipts),
        check_order(receipts, exempt=dead),
        check_latency(receipts, p99_target_ms),
        check_ttl(expected, receipts),
    ]
