#!/usr/bin/env python3
"""Single sequence owner + durable outbox shared by the three demo publishers.

Lock is held through acknowledgements. A pending event is replayed with identical
ID, sequence and bytes before any new event is allocated. Gaps are allowed.
"""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
POOLS = {'java': ('bank', 'core-banking-java'), 'python': ('fraud', 'fraud-engine-python'),
         'ruby': ('otp', 'auth-otp-ruby')}
MAX_SEQUENCE = 9223372036854775807


def partition(account):
    return int.from_bytes(hashlib.sha256(account.encode()).digest()[:4], 'big') % 6


def publisher(language):
    if language == 'java':
        return ['java', '-jar', str(ROOT / 'services/core-banking-java/target/deposit-producer-1.0.0.jar')]
    if language == 'python':
        return [sys.executable, str(ROOT / 'services/fraud-engine-python/producer.py')]
    return ['docker', 'run', '--rm', '-i', '--network', 'relayguard_default',
            '-v', str(ROOT) + ':/workspace:ro', '-e', 'IGGY_PARTITION',
            '-e', 'IGGY_ROOT_USERNAME', '-e', 'IGGY_ROOT_PASSWORD',
            '-e', 'IGGY_HTTP_URL=http://iggy:3000', 'relayguard-ruby-contract']


def event(language, account, seq):
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    stamp = lambda value: value.isoformat().replace('+00:00', 'Z')
    kind = {'java': 'DEPOSIT_PENDING' if seq % 2 else 'DEPOSIT_CONFIRMED',
            'python': 'FRAUD_ALERT', 'ruby': 'OTP'}[language]
    return {'eventId': str(uuid.uuid4()), 'accountId': account, 'sequence': str(seq),
            'type': 'NOTIFICATION_TYPE_' + kind,
            'priority': 'PRIORITY_NORMAL' if language == 'java' else 'PRIORITY_HIGH',
            'channels': ['CHANNEL_SMS'], 'recipient': '+14085550123',
            'body': 'Synthetic demo ' + kind, 'occurredAt': stamp(now),
            'expiresAt': stamp(now + dt.timedelta(seconds=300 if language == 'ruby' else 1800)),
            'producer': POOLS[language][1]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('language', choices=POOLS)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--accounts', type=int, default=3)
    parser.add_argument('--state-dir', type=Path, default=ROOT / '.state')
    parser.add_argument('--namespace', default='demo', help='disjoint demo or smoke account pool')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if args.count < 0 or not 1 <= args.accounts <= 9999:
        parser.error('count >= 0 and accounts in 1..9999 required')
    if not re.fullmatch(r'[a-z0-9]{1,16}', args.namespace):
        parser.error('namespace must be 1..16 lowercase letters or digits')
    args.state_dir.mkdir(parents=True, exist_ok=True)
    # Every instance of one producer MUST mount the same durable state directory.
    with (args.state_dir / (args.language + '.lock')).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = sqlite3.connect(args.state_dir / (args.language + '.sqlite3'))
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        profile = args.namespace + (':dry' if args.dry_run else ':live')
        existing = db.execute("SELECT value FROM config WHERE key='profile'").fetchone()
        if existing and existing[0] != profile:
            raise RuntimeError('state belongs to a different account namespace or dry-run mode')
        with db:
            db.execute("INSERT OR IGNORE INTO config VALUES ('profile',?)", (profile,))
        db.execute('CREATE TABLE IF NOT EXISTS sequences(account TEXT PRIMARY KEY, seq INTEGER NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, payload TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0)')

        def drain():
            for row, payload in db.execute('SELECT id,payload FROM events WHERE acked=0 ORDER BY id').fetchall():
                doc = json.loads(payload)
                if args.dry_run:
                    print(payload)
                else:
                    env = dict(os.environ, IGGY_PARTITION=str(partition(doc['accountId'])))
                    env['PYTHONPATH'] = str(ROOT / 'gen/python')
                    subprocess.run(publisher(args.language) + ['--publish'], input=payload,
                                   text=True, env=env, check=True, cwd=ROOT)
                # Failure/crash before this commit replays this event on restart.
                with db:
                    db.execute('UPDATE events SET acked=1 WHERE id=?', (row,))
        drain()
        for i in range(args.count):
            account = f'{POOLS[args.language][0]}-{args.namespace}-{i % args.accounts + 1:04d}'
            with db:
                found = db.execute('SELECT seq FROM sequences WHERE account=?', (account,)).fetchone()
                seq = (found[0] if found else 0) + 1
                if seq > MAX_SEQUENCE:
                    raise OverflowError('account sequence exhausted')
                payload = json.dumps(event(args.language, account, seq), separators=(',', ':'))
                db.execute('INSERT OR REPLACE INTO sequences VALUES (?,?)', (account, seq))
                db.execute('INSERT INTO events(payload) VALUES (?)', (payload,))
            drain()
        db.close()


if __name__ == '__main__':
    main()
