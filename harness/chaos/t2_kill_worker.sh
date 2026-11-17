#!/usr/bin/env bash
# T2: kill -9 the delivery worker mid-send. Pass: 0 duplicates, 0 lost.
#
# SIGKILL gives the worker no chance to clean up, like a crashed pod. Temporal
# replays the workflow from history on restart; the event_id idempotency key
# must stop the in-flight send from going out twice.
#
# env: RATE=20 DOWN_SECONDS=10
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
RATE=${RATE:-20}; DOWN=${DOWN_SECONDS:-10}
header T2 "SIGKILL $WORKER_SERVICE mid-send" "rate=${RATE}/s down=${DOWN}s"
require_up oracle kafka
require_worker
trap 'dc start "$WORKER_SERVICE" >/dev/null 2>&1 || true' EXIT

oracle_reset
loadgen run --target kafka --phase "60s@$RATE" >/dev/null &
lg=$!
sleep 15
say "kill -9 $WORKER_SERVICE"; dc kill -s SIGKILL "$WORKER_SERVICE" >/dev/null
sleep "$DOWN"
dc start "$WORKER_SERVICE" >/dev/null; say "$WORKER_SERVICE restarted"
wait $lg || say "load generator reported send errors"
fails=0
wait_accounted || fails=1
finish $fails
