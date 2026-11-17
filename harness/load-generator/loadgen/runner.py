"""The send loop: release events on schedule, register each batch with the oracle
first (so it knows about an event before it can arrive), then hand it to the sink."""
from __future__ import annotations

import asyncio
import json
import time
from typing import Callable, Iterable, TextIO

from .sinks import Oracle, Sink


async def run_schedule(due: Iterable[float], make: Callable[[], dict], sink: Sink,
                       oracle: Oracle | None = None, out: TextIO | None = None, *,
                       clock=time.monotonic, sleep=asyncio.sleep, tick: float = 0.05) -> dict:
    it = iter(due)
    nxt = next(it, None)
    t0 = clock()
    sent = 0
    max_late = 0.0
    while nxt is not None:
        now = clock() - t0
        if nxt > now:
            await sleep(min(nxt - now, tick))
            continue
        batch = []
        while nxt is not None and nxt <= now:
            max_late = max(max_late, now - nxt)
            batch.append(make())
            nxt = next(it, None)
        if oracle:
            await oracle.register(batch)
        for event in batch:
            if out:
                out.write(json.dumps(event, separators=(",", ":")) + "\n")
            sink.submit(event)
        sent += len(batch)
        await sleep(0)  # let the senders run between batches
    send_seconds = clock() - t0
    await sink.drain()
    total_seconds = clock() - t0
    return {
        "events": sent,
        "send_seconds": round(send_seconds, 2),
        "achieved_rate": round(sent / send_seconds, 1) if send_seconds > 0 else None,
        # How far behind schedule the loop fell. Large values mean the generator,
        # not the system under test, was the bottleneck.
        "max_lateness_s": round(max_late, 3),
        "drain_seconds": round(total_seconds - send_seconds, 2),
    }
