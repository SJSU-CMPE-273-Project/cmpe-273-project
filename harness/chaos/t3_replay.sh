#!/usr/bin/env bash
# T3: replay the last 1,000 events (same event_ids). Pass: 0 duplicate sends, 0 lost.
# Latency is reported, not graded.
#
# This is what the ingest bridge's at-least-once boundary can do in real life:
# the same record lands in Kafka twice. The worker must skip what it already delivered.
#
# TARGET=kafka (default) needs the worker.
# TARGET=adapter replays straight into the SMS adapter (through Toxiproxy). That only
# proves provider-level idempotency (the oracle absorbs a repeated Idempotency-Key),
# not the worker's own dedup. The output says which one ran.
#
# env: TARGET=kafka COUNT=1000 RATE=100
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
TARGET=${TARGET:-$E2E_TARGET}; COUNT=${COUNT:-1000}; RATE=${RATE:-100}
level=$([[ $TARGET != adapter ]] && echo "end to end (worker dedup)" || echo "adapter level (provider idempotency only)")
header T3 "replay last $COUNT events, $level" "target=$TARGET rate=${RATE}/s"
require_up oracle toxiproxy sms-adapter-primary
[[ $TARGET != adapter ]] && require_e2e

oracle_reset
secs=$(python3 -c "import math;print(math.ceil($COUNT/$RATE))")
say "sending $COUNT events"
loadgen run --target "$TARGET" --phase "${secs}s@$RATE" --out /runs/t3.jsonl >/dev/null
fails=0
wait_accounted || fails=1
say "replaying the last $COUNT, same event_ids"
loadgen replay --target "$TARGET" --from /runs/t3.jsonl --last "$COUNT" >/dev/null || fails=1
sleep "${SETTLE_SECONDS:-$([[ $TARGET != adapter ]] && echo 30 || echo 2)}"
echo "provider calls: $(oq calls)"
# Graded on dedup, not speed: a 1,000-event burst can't meet the 2 s normal-load SLO (T0's job).
FINISH_IGNORE=latency finish $fails
