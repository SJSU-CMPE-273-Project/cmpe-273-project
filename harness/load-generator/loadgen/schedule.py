"""Traffic shape: a list of phases like 30s@20 (30 seconds at 20 events/s)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator

_PHASE = re.compile(r"^\s*(\d+(?:\.\d+)?)(s|m)?\s*@\s*(\d+(?:\.\d+)?)\s*$")


@dataclass(frozen=True)
class Phase:
    seconds: float
    rate: float  # events per second

    @property
    def count(self) -> int:
        return round(self.seconds * self.rate)


def parse_phase(text: str) -> Phase:
    """'30s@20' or '2m@200'. Rate is events per second."""
    m = _PHASE.match(text)
    if not m:
        raise ValueError(f"bad phase {text!r}: use <duration>[s|m]@<events/s>, e.g. 30s@20")
    seconds = float(m.group(1)) * (60 if m.group(2) == "m" else 1)
    rate = float(m.group(3))
    if seconds <= 0 or rate <= 0:
        raise ValueError(f"bad phase {text!r}: duration and rate must be above 0")
    return Phase(seconds, rate)


def offsets(phases: list[Phase]) -> Iterator[float]:
    """Seconds from the start at which each event is due, evenly spaced inside a phase."""
    start = 0.0
    for p in phases:
        for i in range(p.count):
            yield start + i / p.rate
        start += p.seconds


def total(phases: list[Phase]) -> tuple[int, float]:
    return sum(p.count for p in phases), sum(p.seconds for p in phases)
