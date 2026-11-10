"""In-memory record of one test run: expected events, every provider call, fault rules.

Memory, not a database, on purpose: the oracle is test equipment. Each run
starts with POST /admin/reset, and the report is saved to docs/results/.
A lock guards everything because the web server handles calls on many threads.
"""
from __future__ import annotations

import threading
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone

from .checks import Expected, Receipt


@dataclass
class Rule:
    """Per-event fault for tests T4/T5. `times` = how many calls it applies to (None = forever)."""
    event_id: str
    action: str  # "fail" -> 503 (retryable), "reject" -> 422 (permanent)
    times: int | None = None
    used: int = 0


@dataclass
class Outcome:
    status: int
    body: dict


class Recorder:
    def __init__(self, honor_idempotency: bool = True):
        self._lock = threading.Lock()
        self.honor_idempotency = honor_idempotency
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self.run_id = uuid.uuid4().hex[:8]
            self.started_at = datetime.now(timezone.utc)
            self.expected: dict[str, Expected] = {}
            self.receipts: list[Receipt] = []          # reached the phone
            self.calls = Counter()                       # every provider call, by outcome
            self.calls_by_provider = Counter()
            self.idem: dict[tuple[str, str], str] = {}   # (provider, key) -> provider_message_id
            self.rules: dict[str, Rule] = {}

    # ---- expectations ------------------------------------------------------

    def expect(self, items: list[Expected]) -> int:
        with self._lock:
            for e in items:
                self.expected[e.event_id] = e
            return len(self.expected)

    # ---- fault rules -------------------------------------------------------

    def add_rule(self, rule: Rule) -> None:
        if rule.action not in ("fail", "reject"):
            raise ValueError("action must be 'fail' or 'reject'")
        if rule.times is not None and rule.times < 1:
            raise ValueError("times must be >= 1")
        with self._lock:
            self.rules[rule.event_id] = rule

    def clear_rules(self) -> None:
        with self._lock:
            self.rules.clear()

    def rules_list(self) -> list[dict]:
        with self._lock:
            return [vars(r).copy() for r in self.rules.values()]

    # ---- the provider side -------------------------------------------------

    def receive(self, *, provider: str, idempotency_key: str | None, receipt: Receipt) -> Outcome:
        """Handle one provider call. Order matters: fault rules first, then idempotency."""
        with self._lock:
            self.calls_by_provider[provider] += 1

            rule = self.rules.get(receipt.event_id)
            if rule and (rule.times is None or rule.used < rule.times):
                rule.used += 1
                if rule.action == "reject":
                    self.calls["rejected_by_rule"] += 1
                    return Outcome(422, {"status": "rejected", "error": "oracle rule: reject"})
                self.calls["failed_by_rule"] += 1
                return Outcome(503, {"status": "unavailable", "error": "oracle rule: fail"})

            # A real provider remembers idempotency keys *per provider*. A retry to
            # the same provider is absorbed here; the same event sent through a
            # different provider (failover) is a second text on the customer's phone.
            key = idempotency_key or receipt.event_id
            if self.honor_idempotency and (provider, key) in self.idem:
                self.calls["idempotent_replay"] += 1
                return Outcome(200, {"status": "accepted", "provider_message_id": self.idem[(provider, key)],
                                     "idempotent_replay": True})

            pmid = f"{provider}-{uuid.uuid4().hex[:12]}"
            self.idem[(provider, key)] = pmid
            self.receipts.append(receipt)
            self.calls["delivered"] += 1
            return Outcome(200, {"status": "accepted", "provider_message_id": pmid})

    # ---- reporting ---------------------------------------------------------

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "run_id": self.run_id,
                "started_at": self.started_at,
                "expected": dict(self.expected),
                "receipts": list(self.receipts),
                "calls": dict(self.calls),
                "calls_by_provider": dict(self.calls_by_provider),
                "honor_idempotency": self.honor_idempotency,
            }
