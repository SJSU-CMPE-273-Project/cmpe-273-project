#!/usr/bin/env python3
"""Small questions the chaos scripts ask the oracle. Standard library only (runs on the host).

  oracle_query.py check "no loss"          -> PASS / FAIL / SKIP
  oracle_query.py passed [--ignore NAME]   -> exit 0 if every check passed (NAME reported, not graded)
  oracle_query.py p99                      -> end-to-end p99 in ms for this run
  oracle_query.py last-delivery            -> ISO time of the newest delivery
  oracle_query.py first-after PROVIDERS T  -> seconds from T to the first delivery by one of
                                              PROVIDERS (comma list) after T
  oracle_query.py sequences ACCOUNT        -> delivered sequences for ACCOUNT, in arrival order
  oracle_query.py count EVENT_ID           -> how many times EVENT_ID reached the phone
  oracle_query.py calls                    -> provider call breakdown (delivered, idempotent replay, ...)
env: ORACLE (default http://localhost:18080), ORACLE_ADMIN_TOKEN
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime

ORACLE = os.environ.get("ORACLE", "http://localhost:18080").rstrip("/")


def get(path: str):
    req = urllib.request.Request(ORACLE + path)
    if os.environ.get("ORACLE_ADMIN_TOKEN"):
        req.add_header("Authorization", "Bearer " + os.environ["ORACLE_ADMIN_TOKEN"])
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def deliveries(account: str | None = None) -> list[dict]:
    q = f"?limit=1000000" + (f"&account_id={account}" if account else "")
    return get("/deliveries" + q)


def main(cmd: str, *args: str) -> int:
    if cmd == "check":
        rows = {c["name"]: c["result"] for c in get("/report")["checks"]}
        print(rows.get(args[0], "MISSING"))
    elif cmd == "passed":
        ignore = set(args[1:]) if args[:1] == ("--ignore",) else set()
        checks = [c for c in get("/report")["checks"] if c["name"] not in ignore]
        return 0 if all(c["result"] != "FAIL" for c in checks) else 1
    elif cmd == "p99":
        lat = next((c for c in get("/report")["checks"] if c["name"] == "latency"), None)
        print((lat or {}).get("detail", {}).get("p99_ms", ""))
    elif cmd == "last-delivery":
        rows = deliveries()
        print(max(r["received_at"] for r in rows) if rows else "")
    elif cmd == "first-after":
        providers, since = set(args[0].split(",")), ts(args[1])
        later = [ts(r["received_at"]) for r in deliveries()
                 if r["provider"] in providers and ts(r["received_at"]) >= since]
        print(f"{(min(later) - since).total_seconds():.2f}" if later else "")
    elif cmd == "sequences":
        print(" ".join(str(r["sequence"]) for r in deliveries(args[0])))
    elif cmd == "count":
        print(sum(1 for r in deliveries() if r["event_id"] == args[0]))
    elif cmd == "calls":
        print(json.dumps(get("/report")["provider_calls"]))
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]) if len(sys.argv) > 1 else main("help"))
