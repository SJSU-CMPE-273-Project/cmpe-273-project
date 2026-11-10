#!/usr/bin/env python3
"""Exercise each real publisher's generated codec against identical fixtures."""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import jsonschema
from google.protobuf import json_format
from relayguard.v1.notification_pb2 import NotificationRequested

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
schema = json.loads((ROOT / 'contracts/jsonschema/notification_requested.schema.json').read_text())
validator = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
commands = {
    'java': ['java', '-jar', 'services/core-banking-java/target/deposit-producer-1.0.0.jar'],
    'python': [sys.executable, 'services/fraud-engine-python/producer.py'],
    'ruby': ['docker', 'run', '--rm', '-i', '-v', str(ROOT) + ':/workspace:ro', 'relayguard-ruby-contract'],
    'go': [os.getenv('IGGY_SMOKE_BIN', str(ROOT / '.state/iggy-smoke')), '-mode', 'codec'],
}
count = 0
for fixture in sorted((ROOT / 'contracts/fixtures').glob('*.json')):
    expected = json.loads(fixture.read_text())
    validator.validate(expected)
    expected_message = json_format.ParseDict(expected, NotificationRequested())
    for language, command in commands.items():
        run = subprocess.run(command, input=fixture.read_text(), text=True, capture_output=True)
        if run.returncode:
            raise RuntimeError(language + ': ' + run.stderr)
        result = json.loads(run.stdout)
        validator.validate(result['json'])
        assert result['json'] == expected, (language, fixture.name)
        message = NotificationRequested.FromString(base64.b64decode(result['binary']))
        assert message == expected_message, (language, fixture.name, 'binary')
        count += 1
base = json.loads((ROOT / 'contracts/fixtures/ordinary.json').read_text())
for invalid in ['0', '-1', '01', '9223372036854775808', '18446744073709551615', '18446744073709551616', 42, 9007199254740993]:
    doc = dict(base, sequence=invalid)
    assert list(validator.iter_errors(doc)), invalid
for field, invalid in [('type', 1), ('priority', 'HIGH'), ('channels', ['SMS']),
                       ('occurredAt', '2026-11-09T18:05:00+00:00'),
                       ('occurredAt', '2026-11-09T18:05:00.12Z'),
                       ('occurredAt', '2026-02-30T00:00:00Z')]:
    doc = dict(base, **{field: invalid})
    assert list(validator.iter_errors(doc)), (field, invalid)
for language, command in commands.items():
    for invalid in ['0', '9223372036854775808']:
        run = subprocess.run(command, input=json.dumps(dict(base, sequence=invalid)), text=True, capture_output=True)
        assert run.returncode != 0, (language, invalid)
print(f'PASS: {count} cross-language fixture roundtrips; invalid enum, timestamp and sequence profiles rejected')
