"""Oracle web service: plays the SMS/push/email provider and grades each test run.

Provider side (called by the adapters):
  POST /sms | /push | /email      record a delivery, honouring Idempotency-Key per provider
Harness side (called by the load generator and chaos scripts):
  POST /expected                  register events the producers sent
  GET  /report[?format=text]      pass/fail for the invariants
  POST /admin/reset               start a new run
  POST /admin/rules               per-event faults (T4 fail-then-accept, T5 poison)
"""
from __future__ import annotations

import hmac
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from .checks import Expected, Receipt, run_all
from .ledger import Ledger
from .recorder import Recorder, Rule
from .report import render_text
from . import telemetry

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("oracle")

CHANNELS = {"sms", "push", "email"}


def _env_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def parse_int(value: Any) -> int | None:
    # proto3 JSON sends uint64 as a string ("42"), plain JSON as a number.
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def pick(d: dict, *names: str) -> Any:
    """Accept snake_case and proto-JSON camelCase (event_id / eventId)."""
    for n in names:
        if n in d:
            return d[n]
    return None


def create_app(recorder: Recorder | None = None, ledger: Ledger | None = None, span_exporter=None) -> FastAPI:
    app = FastAPI(title="RelayGuard oracle", docs_url="/docs")
    app.state.recorder = recorder or Recorder(honor_idempotency=_env_bool("HONOR_IDEMPOTENCY", True))
    app.state.ledger = ledger or Ledger(os.environ.get("DATABASE_URL"))
    app.state.p99_target_ms = float(os.environ.get("P99_TARGET_MS", "2000"))
    app.state.admin_token = os.environ.get("ADMIN_TOKEN", "")

    def rec() -> Recorder:
        return app.state.recorder

    def require_admin(authorization: str | None = Header(default=None)) -> None:
        token = app.state.admin_token
        if not token:
            return  # local dev: open. Set ADMIN_TOKEN when deployed.
        given = (authorization or "").removeprefix("Bearer ")
        if not hmac.compare_digest(given, token):
            raise HTTPException(401, "unauthorized")

    async def json_body(request: Request) -> Any:
        try:
            return json.loads(await request.body() or b"null")
        except json.JSONDecodeError:
            raise HTTPException(400, "body is not valid JSON")

    @app.get("/health")
    def health():
        s = rec().snapshot()
        return {"status": "ok", "run_id": s["run_id"], "expected": len(s["expected"]),
                "delivered": len(s["receipts"]), "honor_idempotency": s["honor_idempotency"]}

    # ---- provider side --------------------------------------------------

    async def provider_call(request: Request, idempotency_key: str | None = Header(default=None)):
        channel = request.url.path.strip("/")
        msg = await json_body(request)
        if not isinstance(msg, dict) or not msg.get("event_id"):
            raise HTTPException(400, "event_id is required")

        provider = str(msg.get("provider") or channel)
        now = datetime.now(timezone.utc)
        receipt = Receipt(
            event_id=str(msg["event_id"]),
            provider=provider,
            received_at=now,
            account_id=msg.get("account_id"),
            sequence=parse_int(msg.get("sequence")),
            occurred_at=parse_time(msg.get("occurred_at")),
        )
        out = rec().receive(provider=provider, idempotency_key=idempotency_key, receipt=receipt)
        telemetry.annotate(event_id=receipt.event_id, account_id=receipt.account_id,
                           sequence=receipt.sequence, provider=provider, verdict=out.verdict)
        # Log ids only. The oracle never stores or prints the phone number or text.
        line = {"svc": "oracle", "event": "provider.call", "provider": provider,
                "event_id": receipt.event_id, "account_id": receipt.account_id,
                "sequence": receipt.sequence, "code": out.status, "verdict": out.verdict,
                "trace_id": telemetry.trace_id()}
        log.info(json.dumps({k: v for k, v in line.items() if v is not None}))
        return JSONResponse(out.body, status_code=out.status)

    # Explicit paths, not "/{channel}", so they can't shadow the harness routes below.
    for ch in sorted(CHANNELS):
        app.add_api_route(f"/{ch}", provider_call, methods=["POST"], name=f"provider_{ch}")

    # ---- harness side ---------------------------------------------------

    @app.post("/expected", dependencies=[Depends(require_admin)])
    async def expected(request: Request):
        body = await json_body(request)
        items = body if isinstance(body, list) else [body]
        parsed = []
        for d in items:
            eid = pick(d, "event_id", "eventId") if isinstance(d, dict) else None
            if not eid:
                raise HTTPException(400, "each expected event needs event_id")
            parsed.append(Expected(
                event_id=str(eid),
                account_id=pick(d, "account_id", "accountId"),
                sequence=parse_int(pick(d, "sequence")),
                occurred_at=parse_time(pick(d, "occurred_at", "occurredAt")),
                expires_at=parse_time(pick(d, "expires_at", "expiresAt")),
            ))
        total = rec().expect(parsed)
        return {"registered": len(parsed), "expected_total": total}

    @app.get("/report")
    def report(format: str = "json"):
        s = rec().snapshot()
        accounted, ledger_note = app.state.ledger.accounted()
        checks = run_all(s["expected"], s["receipts"], accounted, app.state.p99_target_ms)
        data = {
            "run_id": s["run_id"],
            "started_at": s["started_at"].isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "passed": all(c.result != "FAIL" for c in checks),
            "checks": [c.to_dict() for c in checks],
            "provider_calls": s["calls"],
            "calls_by_provider": s["calls_by_provider"],
            "honor_idempotency": s["honor_idempotency"],
            "ledger": ledger_note,
        }
        if format == "text":
            return PlainTextResponse(render_text(data))
        return data

    @app.get("/deliveries")
    def deliveries(account_id: str | None = None, limit: int = 200):
        rows = [r for r in rec().snapshot()["receipts"] if account_id is None or r.account_id == account_id]
        return [{"event_id": r.event_id, "provider": r.provider, "account_id": r.account_id,
                 "sequence": r.sequence, "received_at": r.received_at.isoformat()} for r in rows[-limit:]]

    @app.post("/admin/reset", dependencies=[Depends(require_admin)])
    def reset():
        rec().reset()
        log.info(json.dumps({"svc": "oracle", "event": "run.reset", "run_id": rec().run_id}))
        return {"run_id": rec().run_id}

    @app.get("/admin/rules", dependencies=[Depends(require_admin)])
    def list_rules():
        return rec().rules_list()

    @app.post("/admin/rules", dependencies=[Depends(require_admin)])
    async def add_rule(request: Request):
        d = await json_body(request)
        if not isinstance(d, dict) or not d.get("event_id"):
            raise HTTPException(400, "event_id is required")
        try:
            rec().add_rule(Rule(event_id=str(d["event_id"]), action=str(d.get("action", "")),
                                times=parse_int(d.get("times"))))
        except ValueError as e:
            raise HTTPException(400, str(e))
        return rec().rules_list()

    @app.delete("/admin/rules", dependencies=[Depends(require_admin)])
    def clear_rules():
        rec().clear_rules()
        return []

    telemetry.instrument(app, exporter=span_exporter)
    return app


app = create_app()
