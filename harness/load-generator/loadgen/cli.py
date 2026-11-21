"""Command line. Logs go to stderr; the last stdout line is a JSON summary scripts can parse.

  run     generate traffic by phases:   run --target iggy --phase 30s@20 --phase 2m@200
  plan    write events to a file without sending (T4/T5 need the event_ids first)
  replay  send events from a file again, same event_ids (T3, DLQ replay)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import secrets
import sys
from contextlib import nullcontext

from .events import EventFactory, parse_mix, restamp
from .runner import run_schedule
from .schedule import Phase, offsets, parse_phase, total
from .sinks import AdapterSink, IggySink, KafkaSink, NullSink, Oracle

log = logging.getLogger("loadgen")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="loadgen", description="RelayGuard load generator")
    sub = p.add_subparsers(dest="cmd", required=True)

    def traffic(sp):
        sp.add_argument("--accounts", type=int, default=50, help="distinct accounts (default 50)")
        sp.add_argument("--mix", help="kind weights, e.g. deposit=0.6,otp=0.3,fraud=0.1")
        sp.add_argument("--seed", type=int, help="same seed + run tag -> same events")
        sp.add_argument("--run-tag", default=None, help="account prefix lg-<tag>-NNNN (default random)")

    def sending(sp):
        sp.add_argument("--target", choices=["iggy", "kafka", "adapter", "none"], default="iggy",
                        help="iggy = full path through the ingest bridge (what the worker admits)")
        sp.add_argument("--iggy", default=os.environ.get("IGGY_CONNECTION_STRING", "iggy://iggy:Secret123@iggy:8090"),
                        help="Iggy connection string (iggy target)")
        sp.add_argument("--adapter-url", default=os.environ.get("ADAPTER_URL", "http://toxiproxy:21001"),
                        help="adapter target; default goes through Toxiproxy so chaos applies")
        sp.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
        sp.add_argument("--topic", default="notifications.requested")
        sp.add_argument("--max-attempts", type=int, default=8, help="adapter target only")
        sp.add_argument("--oracle", default=os.environ.get("ORACLE_URL", "http://oracle:8080"),
                        help="register expected events here; empty string to skip")
        sp.add_argument("--reset-oracle", action="store_true", help="start a new oracle run first")
        sp.add_argument("--out", help="also write every event sent (JSON lines) for a later replay")

    r = sub.add_parser("run", help="generate traffic")
    traffic(r)
    sending(r)
    r.add_argument("--phase", action="append", type=parse_phase, metavar="DUR@RATE",
                   help="repeatable, e.g. --phase 30s@20 --phase 2m@200 (events/s)")
    r.add_argument("--rate", type=float, default=10.0, help="events/s when no --phase (default 10)")
    r.add_argument("--duration", type=float, default=30.0, help="seconds when no --phase (default 30)")

    pl = sub.add_parser("plan", help="write events to a file, send nothing")
    traffic(pl)
    pl.add_argument("--count", type=int, required=True)
    pl.add_argument("--out", required=True)

    rp = sub.add_parser("replay", help="send events from a file again (same event_ids)")
    sending(rp)
    rp.add_argument("--from", dest="src", required=True)
    rp.add_argument("--last", type=int, default=0, help="only the last N events (default all)")
    rp.add_argument("--only", action="append", default=[], metavar="EVENT_ID", help="only these event_ids")
    rp.add_argument("--rate", type=float, default=200.0, help="events/s (default 200)")
    rp.add_argument("--fresh-timestamps", action="store_true",
                    help="set occurredAt to now (for sending a plan); keep off for a true replay")
    return p


def make_sink(a):
    if a.target == "iggy":
        return IggySink(a.iggy)
    if a.target == "kafka":
        return KafkaSink(a.kafka_bootstrap, a.topic)
    if a.target == "adapter":
        return AdapterSink(a.adapter_url, max_attempts=a.max_attempts)
    return NullSink()


async def _send(a, due, make, extra: dict) -> dict:
    oracle = Oracle(a.oracle, os.environ.get("ORACLE_ADMIN_TOKEN", "")) if a.oracle else None
    try:
        if oracle and a.reset_oracle:
            extra["oracle_run_id"] = await oracle.reset()
        sink = make_sink(a)
        with (open(a.out, "w") if a.out else nullcontext()) as out:
            result = await run_schedule(due, make, sink, oracle, out)
        return {**extra, **result, "sink": sink.stats()}
    finally:
        if oracle:
            await oracle.close()


def _factory(a) -> EventFactory:
    a.run_tag = a.run_tag or secrets.token_hex(3)
    return EventFactory(run_tag=a.run_tag, accounts=a.accounts, mix=parse_mix(a.mix), seed=a.seed)


def cmd_run(a) -> dict:
    phases: list[Phase] = a.phase or [Phase(a.duration, a.rate)]
    factory = _factory(a)
    count, seconds = total(phases)
    log.info(json.dumps({"event": "run.start", "target": a.target, "run_tag": a.run_tag,
                         "events": count, "seconds": seconds,
                         "phases": [f"{p.seconds:g}s@{p.rate:g}" for p in phases]}))
    return asyncio.run(_send(a, offsets(phases), factory.next, {"cmd": "run", "run_tag": a.run_tag}))


def cmd_plan(a) -> dict:
    factory = _factory(a)
    with open(a.out, "w") as f:
        for _ in range(a.count):
            e = factory.next()
            f.write(json.dumps(e, separators=(",", ":")) + "\n")
            print(e["eventId"], e["accountId"], e["sequence"])
    return {"cmd": "plan", "run_tag": a.run_tag, "events": a.count, "out": a.out}


def cmd_replay(a) -> dict:
    with open(a.src) as f:
        events = [json.loads(line) for line in f if line.strip()]
    if a.only:
        events = [e for e in events if e["eventId"] in set(a.only)]
    if a.last:
        events = events[-a.last:]
    it = iter(events)
    make = (lambda: restamp(next(it))) if a.fresh_timestamps else (lambda: next(it))
    due = (i / a.rate for i in range(len(events)))
    return asyncio.run(_send(a, due, make, {"cmd": "replay", "from": a.src}))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is too much at 200/s
    a = build_parser().parse_args(argv)
    summary = {"run": cmd_run, "plan": cmd_plan, "replay": cmd_replay}[a.cmd](a)
    print(json.dumps({"summary": summary}))
    sink = summary.get("sink", {})
    # Non-zero exit when sending itself broke, so chaos scripts notice.
    return 1 if sink.get("gave_up") or sink.get("failed") or sink.get("unflushed") else 0
