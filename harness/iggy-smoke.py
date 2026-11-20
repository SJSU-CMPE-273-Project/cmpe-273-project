#!/usr/bin/env python3
"""Live, repeatable transport + SIGKILL recovery checks; retain all volumes.
Run after make contracts-build with Python 3.12 and fraud requirements installed.
Uses its own consumer group; never alters the bridge's offsets.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
BIN = str(ROOT / '.state/iggy-smoke')
GROUP = 'contract-smoke-' + uuid.uuid4().hex[:12]


def run(*command, **kwargs):
    return subprocess.run(command, check=True, text=True, **kwargs)


def init():
    run('docker', 'compose', 'run', '--rm', 'iggy-init')


def poll(commit=True, minimum=0):
    result = run(BIN, '-group', GROUP, '-commit=' + str(commit).lower(),
                 '-min', str(minimum), capture_output=True)
    return [json.loads(line) for line in result.stdout.splitlines()]


def restart():
    run('docker', 'compose', 'kill', '-s', 'SIGKILL', 'iggy')
    run('docker', 'compose', 'up', '-d', 'iggy')
    init()


run('docker', 'compose', 'up', '-d', 'iggy')
init()
init()
run(BIN, '-mode', 'baseline', '-group', GROUP)
# Use the same durable producer state across harness runs (do not reset sequences).
state = ROOT / '.state/smoke-producers'
for language in ('java', 'python', 'ruby'):
    run(sys.executable, 'tools/demo-producer.py', language, '--namespace', 'smoke', '--accounts', '1', '--count', '2', '--state-dir', str(state))
first = poll(False, 6)
restart()
redelivered = poll(True, 6)
assert sorted(first, key=lambda x:x['eventId']) == sorted(redelivered, key=lambda x:x['eventId'])
restart()
assert poll() == [], 'committed records reappeared after restart'
for language in ('java', 'python', 'ruby'):
    run(sys.executable, 'tools/demo-producer.py', language, '--namespace', 'smoke', '--accounts', '1', '--count', '1', '--state-dir', str(state))
resumed = poll(True, 3)
assert len(resumed) == 3
for doc in resumed:
    previous = [d for d in first if d['accountId'] == doc['accountId']]
    assert int(doc['sequence']) == max(int(d['sequence']) for d in previous) + 1
    assert all(doc['partition'] == d['partition'] for d in previous)
print('PASS: all transports, idempotent init, uncommitted redelivery, committed resume, producer sequences and stable partitions')
