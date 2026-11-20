#!/usr/bin/env python3
"""Exercise crash/retry behavior at the actual runner's durable boundary."""
import importlib.util
import datetime as dt
import fcntl
import json
import re
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import uuid
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('demo', Path(__file__).with_name('demo-producer.py'))
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)

# Use one instant across languages/accounts: IDs must not depend on the clock,
# and formatting must preserve milliseconds without rounding into the next one.
instant = dt.datetime(2026, 11, 10, 18, 5, 0, 123456, tzinfo=dt.timezone.utc)


class FixedClock(dt.datetime):
    @classmethod
    def now(cls, tz=None):
        assert tz == dt.timezone.utc
        return instant


identifiers = set()
traces = set()
with patch.object(demo.dt, 'datetime', FixedClock):
    for language in demo.POOLS:
        for seq in range(1, 4):
            doc = demo.event(language, f'{language}-{seq}', seq)
            assert doc['occurredAt'] == '2026-11-10T18:05:00.123Z'
            expiry = dt.datetime.fromisoformat(doc['expiresAt'].replace('Z', '+00:00'))
            occurred = dt.datetime.fromisoformat(doc['occurredAt'].replace('Z', '+00:00'))
            assert (expiry - occurred).total_seconds() == (300 if language == 'ruby' else 1800)
            identifier = uuid.UUID(doc['eventId'])
            assert identifier.version == 4 and str(identifier) == doc['eventId']
            assert identifier not in identifiers
            identifiers.add(identifier)
            traceparent = doc['traceparent']
            assert re.fullmatch(r'00-[0-9a-f]{32}-[0-9a-f]{16}-01', traceparent)
            _, trace_id, parent_id, _ = traceparent.split('-')
            assert int(trace_id, 16) != 0 and int(parent_id, 16) != 0
            assert trace_id not in traces
            traces.add(trace_id)

# Even an all-zero random draw must not create invalid W3C identifiers.
with patch.object(demo.secrets, 'randbits', side_effect=[0, 1, 0, 2]):
    assert demo.new_traceparent() == '00-' + '0' * 31 + '1-' + '0' * 15 + '2-01'

# The normal path must use the current wall clock, not a fixture timestamp.
before = dt.datetime.now(dt.timezone.utc)
doc = demo.event('python', 'fraud-demo-0001', 1)
after = dt.datetime.now(dt.timezone.utc)
occurred = dt.datetime.fromisoformat(doc['occurredAt'].replace('Z', '+00:00'))
assert before - dt.timedelta(milliseconds=1) <= occurred <= after

with tempfile.TemporaryDirectory() as temp:
    for language in demo.POOLS:
        sent = []
        raw = []
        def send(command, **kwargs):
            raw.append(kwargs['input'])
            sent.append(json.loads(kwargs['input']))
            if len(sent) == 1:
                # Models acknowledged publish followed by process failure before ack state commit.
                raise subprocess.CalledProcessError(1, command)
        args = ['demo-producer.py', language, '--accounts', '1', '--count', '1', '--state-dir', temp]
        with patch.object(sys, 'argv', args), patch.object(demo.subprocess, 'run', send):
            try:
                demo.main()
                raise AssertionError('expected interruption')
            except subprocess.CalledProcessError:
                pass
            demo.main()
        assert raw[0] == raw[1]
        assert sent[0] == sent[1], 'pending retry must keep exact event and payload'
        assert sent[0]['eventId'] == sent[1]['eventId']
        assert sent[0]['occurredAt'] == sent[1]['occurredAt']
        assert sent[0]['traceparent'] == sent[1]['traceparent']
        assert sent[2]['sequence'] == '2'
        assert sent[2]['eventId'] != sent[1]['eventId']
        assert sent[2]['traceparent'] != sent[1]['traceparent']
        assert demo.partition(sent[0]['accountId']) == demo.partition(sent[2]['accountId'])
        assert sent[0]['accountId'].startswith(demo.POOLS[language][0] + '-')
        db = sqlite3.connect(Path(temp) / (language + '.sqlite3'))
        assert db.execute('SELECT count(*) FROM events WHERE acked=0').fetchone()[0] == 0
        # Pre-upgrade pending events get one persisted context before retrying.
        legacy = dict(sent[0])
        del legacy['traceparent']
        db.execute('UPDATE events SET payload=?,acked=0 WHERE id=1', (json.dumps(legacy),))
        db.commit()
        legacy_sent = []

        def send_legacy(command, **kwargs):
            legacy_sent.append(kwargs['input'])
            if len(legacy_sent) == 1:
                raise subprocess.CalledProcessError(1, command)

        retry_args = ['demo-producer.py', language, '--count', '0', '--state-dir', temp]
        with patch.object(sys, 'argv', retry_args), patch.object(demo.subprocess, 'run', send_legacy):
            try:
                demo.main()
                raise AssertionError('expected interrupted legacy retry')
            except subprocess.CalledProcessError:
                pass
            demo.main()
        assert legacy_sent[0] == legacy_sent[1]
        upgraded = json.loads(legacy_sent[0])
        assert upgraded['traceparent']
        del upgraded['traceparent']
        assert upgraded == legacy
        db.execute('UPDATE sequences SET seq=?', (demo.MAX_SEQUENCE,))
        db.commit()
        db.close()
        with patch.object(sys, 'argv', args):
            try:
                demo.main()
                raise AssertionError('expected sequence exhaustion')
            except OverflowError:
                pass
        with (Path(temp) / (language + '.lock')).open('a') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(sys, 'argv', args):
                try:
                    demo.main()
                    raise AssertionError('second owner acquired lock')
                except BlockingIOError:
                    pass
print('PASS: all three producers use millisecond UTC event times, unique UUIDv4 IDs and W3C traceparents; retries preserve event metadata')
