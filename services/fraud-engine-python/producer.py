"""One-shot codec/publisher; tools/demo-producer.py owns sequencing and retries."""
import asyncio
import base64
import json
import os
import sys
from google.protobuf import json_format
from relayguard.v1.notification_pb2 import NotificationRequested


def encode(doc):
    message = json_format.Parse(json.dumps(doc), NotificationRequested())
    if not 1 <= message.sequence <= 9223372036854775807:
        raise ValueError('sequence outside BIGINT range')
    return message, json_format.MessageToJson(message, indent=None, ensure_ascii=False)


async def publish(payload):
    from apache_iggy import IggyClient, SendMessage
    client = IggyClient.from_connection_string(os.getenv(
        'IGGY_CONNECTION_STRING', 'iggy://iggy:Secret123@localhost:8090'))
    await client.connect()
    await client.send_messages(stream='relayguard', topic='requests',
                               partitioning=int(os.environ['IGGY_PARTITION']),
                               messages=[SendMessage(payload)])


if __name__ == '__main__':
    message, payload = encode(json.load(sys.stdin))
    if '--publish' in sys.argv:
        asyncio.run(publish(payload))
    else:
        print(json.dumps({'json': json.loads(payload),
                          'binary': base64.b64encode(message.SerializeToString()).decode()}))
