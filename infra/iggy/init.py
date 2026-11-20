"""Iggy server 0.9.0 HTTP API. Create-only init; never reset offsets."""
import json
import os
import time
import urllib.error
import urllib.request

BASE = os.getenv('IGGY_HTTP_URL', 'http://iggy:3000')
PARTITIONS = int(os.getenv('IGGY_PARTITIONS', '6'))
TOKEN = None


def request(method, path, data=None):
    headers = {'Content-Type': 'application/json'}
    if TOKEN:
        headers['Authorization'] = 'Bearer ' + TOKEN
    req = urllib.request.Request(BASE + path, method=method, headers=headers,
                                 data=None if data is None else json.dumps(data).encode())
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as error:
        error.add_note(error.read().decode()[:1000])
        raise


def ensure(path, parent, data):
    try:
        return request('GET', path)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
    try:
        request('POST', parent, data)
    except urllib.error.HTTPError as error:
        # Concurrent initializers are OK only if the resource can be read below.
        if error.code not in (400, 409):
            raise
    return request('GET', path)


for attempt in range(60):
    try:
        TOKEN = request('POST', '/users/login', {
            'username': os.getenv('IGGY_ROOT_USERNAME', 'iggy'),
            'password': os.getenv('IGGY_ROOT_PASSWORD', 'Secret123'),
        })['access_token']['token']
        break
    except (OSError, KeyError):
        if attempt == 59:
            raise
        time.sleep(1)

ensure('/streams/relayguard', '/streams', {'name': 'relayguard'})
path = '/streams/relayguard/topics/requests'
topic = ensure(path, '/streams/relayguard/topics', {
    'name': 'requests', 'partitions_count': PARTITIONS,
    'compression_algorithm': 'none', 'message_expiry': 18446744073709551615,
    'max_topic_size': 18446744073709551615,
    'options': {'durability': 'persisted', 'consumer_offset_durability': 'persisted'},
})
if topic['partitions_count'] != PARTITIONS:
    raise RuntimeError('Partition count mismatch: account routing must not change')
# Existing topics must have the requested durability too; do not silently accept drift.
for key in ('durability', 'consumer_offset_durability'):
    if topic['options'][key]['value'] != 'persisted':
        raise RuntimeError('Existing topic must use persisted ' + key)
ensure(path + '/consumer-groups/ingest-bridge', path + '/consumer-groups', {'name': 'ingest-bridge'})
print('Iggy 0.9.0 ready: relayguard/requests, 6 partitions, ingest-bridge (offsets preserved)')
