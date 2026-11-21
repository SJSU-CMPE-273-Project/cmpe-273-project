#!/usr/bin/env bash
# T7: 10x burst for 2 minutes. Pass: 0 lost, 0 duplicates, order kept. Reports peak
# backlog and latency: back pressure means the backlog (and so latency) grows during the
# burst, then drains, instead of dropping messages. Latency is reported, not graded:
# the p99 <= 2 s SLO is for normal load (T0).
#
# TARGET=kafka (default) needs the worker; backlog = the worker's consumer-group lag.
# TARGET=adapter sends straight to the SMS adapter through Toxiproxy (works today);
# backlog = messages waiting in the load generator, and you see the adapter's 429s.
#
# env: TARGET=kafka RATE=20 (base events/s) BURST_SECONDS=120 WORKER_GROUP=delivery-worker
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
TARGET=${TARGET:-$E2E_TARGET}; RATE=${RATE:-20}; BURST=${BURST_SECONDS:-120}; GROUP=${WORKER_GROUP:-delivery-worker}
peak=$(python3 -c "print(int($RATE*10))")
header T7 "10x burst for ${BURST}s" "target=$TARGET base=${RATE}/s peak=${peak}/s"
require_up oracle toxiproxy sms-adapter-primary
[[ $TARGET != adapter ]] && require_e2e

oracle_reset
LG=$(mktemp); LAG=$(mktemp)
if [[ $TARGET != adapter ]]; then
  ( while :; do
      dc exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
        --describe --group "$GROUP" 2>/dev/null | awk '$2=="notifications.requested"{s+=$6} END{print s+0}'
      sleep 5
    done >"$LAG" ) &
  sampler=$!
  trap 'kill $sampler 2>/dev/null || true' EXIT
fi
say "30s at ${RATE}/s, ${BURST}s at ${peak}/s, 30s at ${RATE}/s"
lg_ok=0
loadgen run --target "$TARGET" --phase "30s@$RATE" --phase "${BURST}s@$peak" --phase "30s@$RATE" >"$LG" || lg_ok=1
fails=$lg_ok
wait_accounted || fails=$((fails + 1))
echo "load generator: achieved $(summary_field "$LG" 's["achieved_rate"]')/s overall, max lateness $(summary_field "$LG" 's["max_lateness_s"]')s"
if [[ $TARGET != adapter ]]; then
  kill $sampler 2>/dev/null || true
  echo "peak consumer lag ($GROUP): $(sort -n "$LAG" | tail -n 1) messages"
else
  echo "adapter back pressure: $(summary_field "$LG" 's["sink"].get("http_429",0)') x 429, peak backlog $(summary_field "$LG" 's["sink"]["peak_backlog"]') messages, gave up $(summary_field "$LG" 's["sink"].get("gave_up",0)')"
fi
FINISH_IGNORE=latency finish $fails
