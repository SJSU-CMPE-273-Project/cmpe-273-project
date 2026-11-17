"""Builds realistic NotificationRequested events (ProtoJSON, the contract's JSON mapping).

Each run gets its own accounts (lg-<run_tag>-0001 ...), so sequences can start at 1
without colliding with the real producers' accounts or an earlier run.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class Kind:
    type: str
    priority: str
    channels: tuple[str, ...]
    ttl: timedelta | None
    producer: str
    body: str


# Every kind starts with SMS, matching the proposal's failover chain
# (SMS primary -> SMS secondary -> push/email -> DLQ).
KINDS: dict[str, Kind] = {
    "deposit": Kind("NOTIFICATION_TYPE_DEPOSIT_CONFIRMED", "PRIORITY_NORMAL",
                    ("CHANNEL_SMS", "CHANNEL_PUSH"), None, "core-banking-java",
                    "Deposit of ${amt} posted to your account ending {last4}."),
    "fraud": Kind("NOTIFICATION_TYPE_FRAUD_ALERT", "PRIORITY_HIGH",
                  ("CHANNEL_SMS", "CHANNEL_PUSH", "CHANNEL_EMAIL"), timedelta(minutes=30),
                  "fraud-engine-python", "Did you spend ${amt} at {merchant}? Reply YES or NO."),
    "otp": Kind("NOTIFICATION_TYPE_OTP", "PRIORITY_HIGH", ("CHANNEL_SMS",), timedelta(minutes=5),
                "auth-otp-ruby", "Your RelayGuard code is {code}. It expires in 5 minutes."),
    "low_balance": Kind("NOTIFICATION_TYPE_LOW_BALANCE", "PRIORITY_LOW",
                        ("CHANNEL_SMS", "CHANNEL_EMAIL"), None, "core-banking-java",
                        "Your balance is below ${amt}."),
}

DEFAULT_MIX = {"deposit": 0.55, "otp": 0.25, "fraud": 0.15, "low_balance": 0.05}
MERCHANTS = ("ACME ELECTRONICS", "BLUE BOTTLE", "SHELL 4471", "TARGET T-1032", "AIRLINE*UA")


def parse_mix(text: str | None) -> dict[str, float]:
    """'deposit=0.6,otp=0.4' -> weights. Unknown kinds are an error, not ignored."""
    if not text:
        return dict(DEFAULT_MIX)
    mix: dict[str, float] = {}
    for part in text.split(","):
        name, _, weight = part.partition("=")
        name = name.strip()
        if name not in KINDS:
            raise ValueError(f"unknown kind {name!r}; choose from {', '.join(KINDS)}")
        mix[name] = float(weight)
    if sum(mix.values()) <= 0:
        raise ValueError("mix weights must add up to more than 0")
    return mix


def iso(dt: datetime) -> str:
    """UTC with milliseconds and a Z, the format the producers use."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class EventFactory:
    """Same seed + run_tag -> same accounts, kinds and event_ids (T4/T5 rely on this)."""

    def __init__(self, *, run_tag: str, accounts: int = 50, mix: dict[str, float] | None = None,
                 seed: int | None = None):
        if accounts < 1:
            raise ValueError("accounts must be at least 1")
        self.rng = random.Random(seed)
        self.accounts = [f"lg-{run_tag}-{i:04d}" for i in range(1, accounts + 1)]
        self.sequences = {a: 0 for a in self.accounts}
        weights = mix or DEFAULT_MIX
        self.kind_names = list(weights)
        self.kind_weights = [weights[k] for k in self.kind_names]

    def _uuid4(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def next(self, now: datetime | None = None, account: str | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        account = account or self.rng.choice(self.accounts)
        self.sequences[account] += 1
        name = self.rng.choices(self.kind_names, self.kind_weights)[0]
        kind = KINDS[name]
        body = kind.body.format(amt=f"{self.rng.uniform(5, 2500):.2f}",
                                last4=f"{self.rng.randint(0, 9999):04d}",
                                merchant=self.rng.choice(MERCHANTS),
                                code=f"{self.rng.randint(0, 999999):06d}")
        event = {
            "eventId": self._uuid4(),
            "accountId": account,
            "sequence": str(self.sequences[account]),  # proto3 JSON: uint64 is a string
            "type": kind.type,
            "priority": kind.priority,
            "channels": list(kind.channels),
            # 555-01xx numbers are reserved for fiction: never a real phone.
            "recipient": f"+1408555{self.rng.randint(100, 199):04d}",
            "body": body,
            "occurredAt": iso(now),
            "producer": kind.producer,
            "traceparent": f"00-{self.rng.getrandbits(128):032x}-{self.rng.getrandbits(64):016x}-01",
        }
        if kind.ttl:
            event["expiresAt"] = iso(now + kind.ttl)
        return event


def restamp(event: dict, now: datetime | None = None) -> dict:
    """Copy with occurredAt = now (and expiresAt moved by the same amount). Keeps event_id."""
    now = now or datetime.now(timezone.utc)
    out = dict(event)
    old = datetime.fromisoformat(event["occurredAt"].replace("Z", "+00:00"))
    out["occurredAt"] = iso(now)
    if "expiresAt" in event:
        exp = datetime.fromisoformat(event["expiresAt"].replace("Z", "+00:00"))
        out["expiresAt"] = iso(now + (exp - old))
    return out


def to_expected(event: dict) -> dict:
    """What the oracle needs to grade the run (no phone number, no text)."""
    keys = ("eventId", "accountId", "sequence", "occurredAt", "expiresAt")
    return {k: event[k] for k in keys if k in event}


def to_adapter_payload(event: dict) -> dict:
    """The adapters' /send body (snake_case, see services/sms-adapter-ruby/README.md)."""
    return {
        "event_id": event["eventId"],
        "account_id": event["accountId"],
        "sequence": int(event["sequence"]),
        "occurred_at": event["occurredAt"],
        "recipient": event["recipient"],
        "body": event["body"],
        "traceparent": event.get("traceparent"),
    }
