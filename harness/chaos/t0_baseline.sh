#!/usr/bin/env bash
# T0: baseline, no faults. Normal traffic for 60 s. Pass: every invariant holds and
# p99 latency is under the oracle's target (SLO draft: p99 <= 2 s).
# Every other test is compared with this one.
#
# TARGET=iggy (default) is the full producer path and needs the worker. TARGET=adapter measures adapter + Toxiproxy
# + oracle only, which is useful today and as a floor for the end-to-end number.
#
# env: TARGET=iggy|adapter RATE=20 SECONDS_=60
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
TARGET=${TARGET:-$E2E_TARGET}; RATE=${RATE:-20}; DUR=${SECONDS_:-60}
header T0 "baseline, no faults" "target=$TARGET rate=${RATE}/s duration=${DUR}s"
require_up oracle toxiproxy sms-adapter-primary
[[ $TARGET != adapter ]] && require_e2e

oracle_reset
LG=$(mktemp)
fails=0
loadgen run --target "$TARGET" --phase "${DUR}s@$RATE" >"$LG" || fails=1
wait_accounted 60 || fails=$((fails + 1))
echo "load generator: $(summary_field "$LG" 's["events"]') events at $(summary_field "$LG" 's["achieved_rate"]')/s, sink $(summary_field "$LG" 'json.dumps(s["sink"])')"
finish $fails
