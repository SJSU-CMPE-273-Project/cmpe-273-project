"""Where events go.

kafka    the real entry point for the delivery worker: notifications.requested.
adapter  straight to an adapter's /send. A stand-in client so the adapters, Toxiproxy
         and the oracle can be load-tested before the worker exists. It keeps
         per-account order and retries with the same event_id, but it is NOT the
         worker: no failover, no DLQ, no TTL check.
none     count only (dry runs, tests).
"""
from __future__ import annotations

import asyncio
import logging
from collections import Counter
from typing import Protocol

import httpx

from .events import to_adapter_payload, to_expected

log = logging.getLogger("loadgen")
RETRYABLE = {429, 503, 504}


class Sink(Protocol):
    def submit(self, event: dict) -> None: ...
    async def drain(self) -> None: ...
    def stats(self) -> dict: ...


class NullSink:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def submit(self, event: dict) -> None:
        self.events.append(event)

    async def drain(self) -> None:
        return None

    def stats(self) -> dict:
        return {"target": "none", "accepted": len(self.events)}


class AdapterSink:
    """One queue + one sender per account: account A's message N+1 waits for N,
    while other accounts go in parallel. Same idea as the worker's per-account workflow."""

    def __init__(self, url: str, *, client: httpx.AsyncClient | None = None, max_attempts: int = 8,
                 timeout: float = 10.0, max_backoff: float = 5.0, max_busy: int = 600,
                 sleep=asyncio.sleep):
        self.url = url.rstrip("/") + "/send"
        # Up to 64 connections: more than the adapters' bulkheads (8), so overload shows up as 429s.
        self.client = client or httpx.AsyncClient(
            timeout=timeout, limits=httpx.Limits(max_connections=64, max_keepalive_connections=64))
        self.max_attempts = max_attempts
        self.max_busy = max_busy  # safety stop: ~10 min of 429s
        self.max_backoff = max_backoff
        self.sleep = sleep
        self.queues: dict[str, asyncio.Queue] = {}
        self.tasks: list[asyncio.Task] = []
        self.counts: Counter = Counter()
        self.queued = 0
        self.peak_backlog = 0

    def submit(self, event: dict) -> None:
        q = self.queues.get(event["accountId"])
        if q is None:
            q = self.queues[event["accountId"]] = asyncio.Queue()
            self.tasks.append(asyncio.create_task(self._sender(q)))
        q.put_nowait(event)
        self.queued += 1
        self.peak_backlog = max(self.peak_backlog, self.queued)

    async def _sender(self, q: asyncio.Queue) -> None:
        while True:
            event = await q.get()
            try:
                await self._send(event)
            finally:
                self.queued -= 1
                q.task_done()

    async def _send(self, event: dict) -> None:
        payload = to_adapter_payload(event)
        headers = {"traceparent": event["traceparent"]} if event.get("traceparent") else {}
        attempt = 0   # failures only; 429 doesn't count (see below)
        busy = 0
        first = True
        while attempt < self.max_attempts and busy < self.max_busy:
            if not first:
                self.counts["retries"] += 1
            first = False
            try:
                r = await self.client.post(self.url, json=payload, headers=headers)
                code = r.status_code
            except httpx.HTTPError as e:  # refused / reset / timed out: adapter down or cut
                code, r = f"conn:{type(e).__name__}", None
            if code == 200:
                self.counts["accepted"] += 1
                return
            self.counts[code if isinstance(code, str) else f"http_{code}"] += 1
            if code == 429:
                # Back pressure, not a failure: the adapter is healthy but full. Wait as
                # long as it asked and try again with the same event_id (no duplicate).
                # The worker should do the same, so this never burns a retry attempt.
                busy += 1
                await self.sleep(min(float(r.headers.get("Retry-After", "1")), self.max_backoff))
            elif code in RETRYABLE or isinstance(code, str):
                attempt += 1
                await self.sleep(min(0.2 * 2 ** (attempt - 1), self.max_backoff))
            else:
                break  # 400/422: retrying won't help
        self.counts["gave_up"] += 1
        log.warning('{"event":"send.gave_up","event_id":"%s","last":"%s"}', event["eventId"], code)

    async def drain(self) -> None:
        for q in list(self.queues.values()):
            await q.join()
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.client.aclose()

    def stats(self) -> dict:
        return {"target": "adapter", "url": self.url, "peak_backlog": self.peak_backlog,
                **dict(sorted(self.counts.items()))}


class KafkaSink:
    """Idempotent producer, acks=all, key=account_id: same settings as the ingest bridge."""

    def __init__(self, bootstrap: str, topic: str, producer=None):
        if producer is None:
            from confluent_kafka import Producer  # imported here so tests don't need librdkafka
            producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                 "acks": "all", "linger.ms": 5})
        from . import wire
        self.wire = wire
        self.producer = producer
        self.topic = topic
        self.counts: Counter = Counter()

    def _on_delivery(self, err, _msg) -> None:
        self.counts["failed" if err else "acked"] += 1
        if err:
            log.warning('{"event":"kafka.failed","error":"%s"}', err)

    def submit(self, event: dict) -> None:
        while True:
            try:
                self.producer.produce(self.topic, key=self.wire.key(event), value=self.wire.encode(event),
                                      on_delivery=self._on_delivery)
                break
            except BufferError:  # local queue full: let librdkafka send some first
                self.producer.poll(0.1)
        self.counts["produced"] += 1
        self.producer.poll(0)

    async def drain(self) -> None:
        left = await asyncio.to_thread(self.producer.flush, 30)
        if left:
            self.counts["unflushed"] += left

    def stats(self) -> dict:
        return {"target": "kafka", "topic": self.topic, **dict(sorted(self.counts.items()))}


class Oracle:
    """Tells the oracle what was sent, so it can tell lost from never-sent."""

    def __init__(self, url: str, token: str = "", client: httpx.AsyncClient | None = None):
        self.url = url.rstrip("/")
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.client = client or httpx.AsyncClient(timeout=10.0, headers=headers)

    async def register(self, events: list[dict]) -> None:
        if not events:
            return
        r = await self.client.post(f"{self.url}/expected", json=[to_expected(e) for e in events])
        r.raise_for_status()

    async def reset(self) -> str:
        r = await self.client.post(f"{self.url}/admin/reset")
        r.raise_for_status()
        return r.json()["run_id"]

    async def close(self) -> None:
        await self.client.aclose()
