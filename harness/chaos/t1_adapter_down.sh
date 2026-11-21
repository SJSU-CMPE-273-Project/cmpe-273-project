#!/usr/bin/env bash
# T1: SMS primary adapter down for 5 minutes. Pass: 0 lost after recovery.
#
# Traffic keeps flowing the whole time. The worker has to retry or fail over
# while the adapter is gone; the backlog then drains once it's back.
# Reports: drain time = restart -> last delivery (SLO draft: <= 2 min).
#
# env: OUTAGE_SECONDS=300 RATE=5 (events/s)
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
OUTAGE=${OUTAGE_SECONDS:-300}; RATE=${RATE:-5}
header T1 "SMS primary adapter down ${OUTAGE}s" "rate=${RATE}/s target=$E2E_TARGET"
require_e2e
require_up oracle sms-adapter-primary
trap 'dc start sms-adapter-primary >/dev/null 2>&1 || true' EXIT

oracle_reset
LG=$(mktemp)
loadgen run --target "$E2E_TARGET" --phase "$((OUTAGE + 60))s@$RATE" >"$LG" &
lg=$!
sleep 20
say "stopping sms-adapter-primary"; dc stop sms-adapter-primary >/dev/null
sleep "$OUTAGE"
dc start sms-adapter-primary >/dev/null; restarted=$(now_iso); say "sms-adapter-primary back"
wait $lg || say "load generator reported send errors"
fails=0
wait_accounted || fails=1
last=$(oq last-delivery)
drain=$(python3 -c "from datetime import datetime as d;a=d.fromisoformat('$restarted');b=d.fromisoformat('$last');print(f'{max(0,(b-a).total_seconds()):.1f}')")
echo "drain time after restart: ${drain}s (SLO <= ${DRAIN_SLO:=120}s)"
python3 -c "import sys; sys.exit(0 if $drain <= $DRAIN_SLO else 1)" || fails=$((fails + 1))
FINISH_IGNORE=latency finish $fails
