#!/usr/bin/env bash
# T6: primary SMS provider outage (Toxiproxy cuts sms_primary). Pass: failover, 0 lost,
# 0 duplicates. Reports failover time = outage start -> first delivery by a fallback.
#
# env: RATE=10 OUTAGE_SECONDS=45
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
RATE=${RATE:-10}; OUTAGE=${OUTAGE_SECONDS:-45}
header T6 "primary SMS provider outage ${OUTAGE}s" "rate=${RATE}/s target=kafka"
require_up oracle kafka toxiproxy
require_worker
trap 'toxi_enable sms_primary || true' EXIT

oracle_reset
loadgen run --target kafka --phase "$((OUTAGE + 45))s@$RATE" >/dev/null &
lg=$!
sleep 15
cut=$(now_iso); toxi_disable sms_primary; say "sms_primary cut"
sleep "$OUTAGE"
toxi_enable sms_primary; say "sms_primary restored"
wait $lg || say "load generator reported send errors"
fails=0
wait_accounted || fails=1
ft=$(oq first-after sms_secondary,push,email "$cut")
if [[ -n $ft ]]; then echo "failover time: ${ft}s (outage start -> first fallback delivery)"
else echo "failover time: none (no fallback delivery seen)"; fails=$((fails + 1)); fi
finish $fails
