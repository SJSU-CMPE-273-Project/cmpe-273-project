"""Wire formats from contracts/README.md.

Kafka: raw binary NotificationRequested, key = UTF-8 account_id, no Confluent framing.
Iggy:  ProtoJSON text on relayguard/requests, partition chosen from the account.
The Python classes are generated
from contracts/proto at image build time, so they always match the contract.
"""
from __future__ import annotations

import hashlib

from google.protobuf import json_format

from relayguard.v1 import notification_pb2  # generated in the Dockerfile


def encode(event: dict) -> bytes:
    """ProtoJSON dict -> raw Protobuf bytes. Rejects fields the contract doesn't have."""
    msg = notification_pb2.NotificationRequested()
    json_format.ParseDict(event, msg)  # camelCase, enum names, string uint64, RFC 3339 times
    return msg.SerializeToString(deterministic=True)


def decode(raw: bytes) -> dict:
    msg = notification_pb2.NotificationRequested()
    msg.ParseFromString(raw)
    return json_format.MessageToDict(msg)


def key(event: dict) -> bytes:
    return event["accountId"].encode("utf-8")


# ---- Iggy (relayguard/requests): what the real producers publish -----------------
IGGY_PARTITIONS = 6  # must match the ingest bridge (services/ingest-bridge-go)


def to_json(event: dict) -> str:
    """ProtoJSON dict -> compact ProtoJSON text, after a round trip through the
    contract so unknown fields fail here instead of at the bridge."""
    msg = notification_pb2.NotificationRequested()
    json_format.ParseDict(event, msg)
    return json_format.MessageToJson(msg, indent=None)


def iggy_partition(account_id: str) -> int:
    """First 4 bytes of SHA-256(account), big-endian, mod 6. The bridge recomputes
    this and rejects a mismatch as wrong_partition, so it has to be bit-for-bit
    the same as tools/demo-producer.py and bridge.go."""
    digest = hashlib.sha256(account_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % IGGY_PARTITIONS
