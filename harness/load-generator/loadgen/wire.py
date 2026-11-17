"""Kafka wire format from contracts/README.md: raw binary NotificationRequested,
key = UTF-8 account_id, no Confluent framing. The Python classes are generated
from contracts/proto at image build time, so they always match the contract.
"""
from __future__ import annotations

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
