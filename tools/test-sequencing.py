#!/usr/bin/env python3
"""Exercise crash/retry behavior at the actual runner's durable boundary."""
import importlib.util
import fcntl
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('demo', Path(__file__).with_name('demo-producer.py'))
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)

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
        assert sent[2]['sequence'] == '2'
        assert sent[2]['eventId'] != sent[1]['eventId']
        assert demo.partition(sent[0]['accountId']) == demo.partition(sent[2]['accountId'])
        assert sent[0]['accountId'].startswith(demo.POOLS[language][0] + '-')
        db = sqlite3.connect(Path(temp) / (language + '.sqlite3'))
        assert db.execute('SELECT count(*) FROM events WHERE acked=0').fetchone()[0] == 0
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
print('PASS: all three sequence owners persist sequence and replay pending bytes before new events')
