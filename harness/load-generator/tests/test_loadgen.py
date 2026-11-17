import asyncio
import json
import uuid
from datetime import datetime, timezone

import httpx
import pytest

from loadgen import cli
from loadgen.events import EventFactory, parse_mix, restamp, to_adapter_payload, to_expected
from loadgen.runner import run_schedule
from loadgen.schedule import Phase, offsets, parse_phase, total
from loadgen.sinks import AdapterSink, KafkaSink, NullSink, Oracle

SCHEMA_REQUIRED = {"eventId", "accountId", "sequence", "type", "priority", "channels",
                   "recipient", "body", "occurredAt", "producer"}


# ---- events -----------------------------------------------------------------

def test_events_match_contract_shape():
    f = EventFactory(run_tag="t", accounts=3, seed=1)
    for _ in range(50):
        e = f.next()
        assert SCHEMA_REQUIRED <= e.keys()
        assert uuid.UUID(e["eventId"]).version == 4
        assert e["sequence"].isdigit()                 # proto3 JSON uint64 is a string
        assert e["occurredAt"].endswith("Z")
        assert e["recipient"].startswith("+1408555")   # fictional numbers only
        assert len(e["accountId"]) <= 64


def test_sequences_strictly_increase_per_account():
    f = EventFactory(run_tag="t", accounts=4, seed=2)
    last: dict[str, int] = {}
    for _ in range(200):
        e = f.next()
        seq = int(e["sequence"])
        assert seq == last.get(e["accountId"], 0) + 1
        last[e["accountId"]] = seq


def test_same_seed_same_events():
    now = datetime(2026, 11, 16, tzinfo=timezone.utc)
    a = [EventFactory(run_tag="x", seed=7).next(now) for _ in range(1)]
    b = [EventFactory(run_tag="x", seed=7).next(now) for _ in range(1)]
    assert a == b


def test_otp_has_five_minute_ttl_deposit_has_none():
    f = EventFactory(run_tag="t", seed=3, mix=parse_mix("otp=1"))
    e = f.next(datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc))
    assert e["expiresAt"] == "2026-11-16T12:05:00.000Z"
    d = EventFactory(run_tag="t", seed=3, mix=parse_mix("deposit=1")).next()
    assert "expiresAt" not in d


def test_parse_mix_rejects_unknown_kind():
    with pytest.raises(ValueError):
        parse_mix("deposit=1,marketing=2")


def test_restamp_keeps_id_and_ttl_length():
    e = EventFactory(run_tag="t", seed=4, mix=parse_mix("otp=1")).next(
        datetime(2026, 1, 1, tzinfo=timezone.utc))
    r = restamp(e, datetime(2026, 2, 1, tzinfo=timezone.utc))
    assert r["eventId"] == e["eventId"]
    assert r["occurredAt"] == "2026-02-01T00:00:00.000Z"
    assert r["expiresAt"] == "2026-02-01T00:05:00.000Z"


def test_expected_has_no_pii_and_adapter_payload_is_snake_case():
    e = EventFactory(run_tag="t", seed=5).next()
    exp = to_expected(e)
    assert "recipient" not in exp and "body" not in exp
    p = to_adapter_payload(e)
    assert p["event_id"] == e["eventId"] and p["sequence"] == int(e["sequence"])


# ---- schedule ---------------------------------------------------------------

def test_parse_phase():
    assert parse_phase("30s@20") == Phase(30, 20)
    assert parse_phase("2m@200") == Phase(120, 200)
    for bad in ("30@", "x@1", "0s@5", "5s@0"):
        with pytest.raises(ValueError):
            parse_phase(bad)


def test_offsets_are_even_and_phases_chain():
    phases = [Phase(1, 4), Phase(1, 2)]
    assert list(offsets(phases)) == [0, 0.25, 0.5, 0.75, 1.0, 1.5]
    assert total(phases) == (6, 2)


# ---- wire (Kafka) -------------------------------------------------------------

def test_wire_round_trip():
    from loadgen import wire
    e = EventFactory(run_tag="t", seed=6, mix=parse_mix("fraud=1")).next()
    raw = wire.encode(e)
    assert raw[:1] != b"\x00"  # no Confluent magic byte
    back = wire.decode(raw)
    assert back["eventId"] == e["eventId"]
    assert back["sequence"] == e["sequence"]
    assert back["channels"] == e["channels"]
    assert wire.key(e) == e["accountId"].encode()


def test_wire_rejects_fields_not_in_contract():
    from google.protobuf.json_format import ParseError
    from loadgen import wire
    e = EventFactory(run_tag="t", seed=6).next()
    with pytest.raises(ParseError):
        wire.encode({**e, "notAField": 1})


# ---- runner -----------------------------------------------------------------

class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    async def sleep(self, s):
        self.t += s
        await asyncio.sleep(0)


class FakeOracle:
    def __init__(self, sink):
        self.sink, self.batches = sink, []

    async def register(self, events):
        # The oracle must hear about an event before the sink sends it.
        assert not any(e in self.sink.events for e in events)
        self.batches.append([e["eventId"] for e in events])


def test_run_schedule_registers_before_sending_and_writes_out(tmp_path):
    clock, sink = FakeClock(), NullSink()
    oracle = FakeOracle(sink)
    f = EventFactory(run_tag="t", seed=8)
    path = tmp_path / "out.jsonl"
    with open(path, "w") as out:
        res = asyncio.run(run_schedule(offsets([Phase(2, 10)]), f.next, sink, oracle, out,
                                       clock=clock, sleep=clock.sleep))
    assert res["events"] == 20 == len(sink.events)
    assert sum(len(b) for b in oracle.batches) == 20
    lines = path.read_text().splitlines()
    assert [json.loads(x)["eventId"] for x in lines] == [e["eventId"] for e in sink.events]


# ---- adapter sink -------------------------------------------------------------

def adapter_sink(handler):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def no_sleep(_):
        await asyncio.sleep(0)
    return AdapterSink("http://adapter", client=client, sleep=no_sleep, max_attempts=4)


def test_adapter_sink_honours_429_then_succeeds_with_same_event_id():
    seen = []

    def handler(req):
        body = json.loads(req.content)
        seen.append(body["event_id"])
        if len(seen) == 1:
            return httpx.Response(429, headers={"Retry-After": "1"})
        return httpx.Response(200, json={"status": "accepted"})

    async def go():
        sink = adapter_sink(handler)
        sink.submit(EventFactory(run_tag="t", seed=9).next())
        await sink.drain()
        return sink.stats()

    stats = asyncio.run(go())
    assert stats["accepted"] == 1 and stats["http_429"] == 1 and stats["retries"] == 1
    assert len(set(seen)) == 1  # retried with the same idempotency key


def test_adapter_sink_gives_up_on_422_without_retry():
    async def go():
        sink = adapter_sink(lambda req: httpx.Response(422))
        sink.submit(EventFactory(run_tag="t", seed=9).next())
        await sink.drain()
        return sink.stats()

    stats = asyncio.run(go())
    assert stats["gave_up"] == 1 and "retries" not in stats


def test_adapter_sink_keeps_per_account_order_under_concurrency():
    order: list[tuple[str, int]] = []
    in_flight: dict[str, int] = {}

    async def handler(req):
        b = json.loads(req.content)
        acct = b["account_id"]
        in_flight[acct] = in_flight.get(acct, 0) + 1
        assert in_flight[acct] == 1  # never two sends for one account at once
        await asyncio.sleep(0.001)
        order.append((acct, b["sequence"]))
        in_flight[acct] -= 1
        return httpx.Response(200)

    async def go():
        sink = AdapterSink("http://a", client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        f = EventFactory(run_tag="t", accounts=3, seed=10)
        for _ in range(30):
            sink.submit(f.next())
        await sink.drain()

    asyncio.run(go())
    for acct in {a for a, _ in order}:
        seqs = [s for a, s in order if a == acct]
        assert seqs == sorted(seqs)


def test_adapter_sink_retries_connection_errors():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] < 3:
            raise httpx.ConnectError("refused")
        return httpx.Response(200)

    async def go():
        sink = adapter_sink(handler)
        sink.submit(EventFactory(run_tag="t", seed=11).next())
        await sink.drain()
        return sink.stats()

    stats = asyncio.run(go())
    assert stats["accepted"] == 1 and stats["conn:ConnectError"] == 2


# ---- kafka sink ---------------------------------------------------------------

class FakeProducer:
    def __init__(self):
        self.records = []

    def produce(self, topic, key, value, on_delivery):
        self.records.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, _):
        return 0

    def flush(self, _):
        return 0


def test_kafka_sink_keys_by_account_and_sends_raw_proto():
    from loadgen import wire
    prod = FakeProducer()
    sink = KafkaSink("x", "notifications.requested", producer=prod)
    e = EventFactory(run_tag="t", seed=12).next()
    sink.submit(e)
    asyncio.run(sink.drain())
    topic, key, value = prod.records[0]
    assert topic == "notifications.requested" and key == e["accountId"].encode()
    assert wire.decode(value)["eventId"] == e["eventId"]
    assert sink.stats()["acked"] == 1


# ---- oracle client + CLI --------------------------------------------------------

def test_oracle_register_posts_expected_without_pii():
    got = {}

    def handler(req):
        got["path"], got["body"] = req.url.path, json.loads(req.content)
        return httpx.Response(200, json={"registered": 1})

    async def go():
        o = Oracle("http://oracle", client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        await o.register([EventFactory(run_tag="t", seed=13).next()])
        await o.close()

    asyncio.run(go())
    assert got["path"] == "/expected" and "recipient" not in got["body"][0]


def test_cli_plan_then_replay_dry_run(tmp_path, capsys):
    plan = tmp_path / "plan.jsonl"
    assert cli.main(["plan", "--count", "5", "--accounts", "1", "--seed", "1",
                     "--run-tag", "p", "--out", str(plan)]) == 0
    ids = [line.split()[0] for line in capsys.readouterr().out.splitlines()[:5]]
    assert len(set(ids)) == 5
    assert cli.main(["replay", "--from", str(plan), "--target", "none", "--oracle", "",
                     "--only", ids[2], "--fresh-timestamps"]) == 0
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])["summary"]
    assert summary["events"] == 1 and summary["sink"]["accepted"] == 1


def test_cli_run_dry():
    assert cli.main(["run", "--target", "none", "--oracle", "", "--phase", "1s@20"]) == 0


def test_adapter_sink_429s_do_not_use_up_retry_attempts():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(429, headers={"Retry-After": "1"}) if calls["n"] <= 10 else httpx.Response(200)

    async def go():
        sink = adapter_sink(handler)  # max_attempts=4, but 10 x 429 is still fine
        sink.submit(EventFactory(run_tag="t", seed=14).next())
        await sink.drain()
        return sink.stats()

    stats = asyncio.run(go())
    assert stats["accepted"] == 1 and stats["http_429"] == 10 and "gave_up" not in stats
