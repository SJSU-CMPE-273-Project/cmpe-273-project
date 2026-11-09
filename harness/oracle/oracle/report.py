"""Plain-text report: the table that goes into docs/results/ and the demo slides."""
from __future__ import annotations


def render_text(data: dict) -> str:
    lines = [
        f"RelayGuard oracle report   run {data['run_id']}   started {data['started_at'][:19]}Z",
        "",
        f"{'CHECK':<20} {'RESULT':<7} DETAIL",
        f"{'-' * 20} {'-' * 7} {'-' * 50}",
    ]
    for c in data["checks"]:
        lines.append(f"{c['name']:<20} {c['result']:<7} {c['summary']}")
    calls = data["provider_calls"]
    by_prov = ", ".join(f"{p} {n}" for p, n in sorted(data["calls_by_provider"].items())) or "none"
    lines += [
        "",
        f"OVERALL: {'PASS' if data['passed'] else 'FAIL'}",
        "",
        f"provider calls: {sum(calls.values())} total ({by_prov})",
        "  " + ", ".join(f"{k.replace('_', ' ')} {v}" for k, v in sorted(calls.items())) if calls else "",
        f"idempotency honoured: {data['honor_idempotency']}",
        f"ledger: {data['ledger']}",
    ]
    return "\n".join(l for l in lines if l is not None) + "\n"
