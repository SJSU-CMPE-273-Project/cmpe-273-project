#!/usr/bin/env bash
# Creates RelayGuard's Kafka topics. Safe to re-run (--if-not-exists).
#
# notifications.requested  validated, deduped events from the ingest bridge
#                          raw NotificationRequested Protobuf, key = UTF-8 account_id
#                          preserves arrival order; see contracts/README.md
# notifications.status     delivery outcomes (delivered / failed / dead-lettered)
# notifications.dlq        messages that exhausted retries, with attempt history
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
PARTITIONS="${PARTITIONS:-6}"
KT=/opt/kafka/bin/kafka-topics.sh

for topic in notifications.requested notifications.status notifications.dlq; do
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1
done

"$KT" --bootstrap-server "$BOOTSTRAP" --list
echo "kafka-init: topics ready"
