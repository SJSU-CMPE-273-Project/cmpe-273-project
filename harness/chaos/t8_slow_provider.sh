#!/usr/bin/env bash
# T8: a slow provider is isolated (bulkhead + back pressure).
#
# Makes the primary SMS provider hang, floods the primary adapter, and checks:
#   1. overflow is refused at once with 429 + Retry-After (back pressure),
#      not queued until it times out
#   2. the primary adapter's /health still answers (the bulkhead kept threads free)
#   3. the secondary adapter still delivers fast (one slow provider can't sink the other)
#
# Adapter-level for now: it calls the adapters directly (host ports 18081/18082).
# Once the delivery worker is in, run it with the worker in the loop and
# check the oracle report for 0 lost.
#
# Usage: harness/chaos/t8_slow_provider.sh
#   env: REQUESTS=20 HANG_SECONDS=5 PRIMARY=http://localhost:18081
#        SECONDARY=http://localhost:18082 ADMIN_TOKEN=<adapter admin token, if set>
set -euo pipefail

PRIMARY=${PRIMARY:-http://localhost:18081}
SECONDARY=${SECONDARY:-http://localhost:18082}
REQUESTS=${REQUESTS:-20}
HANG_SECONDS=${HANG_SECONDS:-5}
FAST=1.0 # seconds: what counts as "answered at once"
AUTH=()
[[ -n "${ADMIN_TOKEN:-}" ]] && AUTH=(-H "Authorization: Bearer $ADMIN_TOKEN")
OUT=$(mktemp -d)
trap 'curl -s -o /dev/null -X PUT ${AUTH[@]+"${AUTH[@]}"} -H "Content-Type: application/json" \
        -d "{\"mode\":\"ok\"}" "$PRIMARY/admin/fault"; rm -rf "$OUT"' EXIT

send() { # url event_id -> "code seconds"
  curl -s -o /dev/null -w "%{http_code} %{time_total}\n" -X POST "$1/send" \
    -H "Content-Type: application/json" \
    -d "{\"event_id\":\"$2\",\"account_id\":\"t8\",\"sequence\":1,\"recipient\":\"+14085550100\",\"body\":\"t8\"}"
}
is_fast() { awk -v t="$1" -v f="$FAST" 'BEGIN { exit !(t < f) }'; }

max=$(curl -s "$PRIMARY/health" | python3 -c 'import json,sys;print(json.load(sys.stdin)["bulkhead"]["max_in_flight"])')
echo "T8 slow provider | primary bulkhead max_in_flight=$max | $REQUESTS requests | provider hangs ${HANG_SECONDS}s"

curl -sf -o /dev/null -X PUT ${AUTH[@]+"${AUTH[@]}"} -H "Content-Type: application/json" \
  -d "{\"mode\":\"timeout\",\"timeout_seconds\":$HANG_SECONDS}" "$PRIMARY/admin/fault"

for i in $(seq 1 "$REQUESTS"); do send "$PRIMARY" "t8-$RANDOM-$i" > "$OUT/p$i" & done
sleep 0.5 # let the flood land, then probe while the slow calls are still hanging
health=$(curl -s -o /dev/null -w "%{http_code} %{time_total}" "$PRIMARY/health")
secondary=$(send "$SECONDARY" "t8-secondary-$RANDOM")
wait

cat "$OUT"/p* > "$OUT/all"
n429=$(awk '$1==429' "$OUT/all" | wc -l | tr -d ' ')
n504=$(awk '$1==504' "$OUT/all" | wc -l | tr -d ' ')
other=$(awk '$1!=429 && $1!=504' "$OUT/all" | wc -l | tr -d ' ')
slow429=$(awk -v f="$FAST" '$1==429 && $2>=f' "$OUT/all" | wc -l | tr -d ' ')
max429=$(awk '$1==429 {if ($2>m) m=$2} END {printf "%.3f", m+0}' "$OUT/all")

fails=0
check() { # name ok? detail
  if [[ "$2" == 1 ]]; then printf "%-34s PASS  %s\n" "$1" "$3"; else printf "%-34s FAIL  %s\n" "$1" "$3"; fails=$((fails+1)); fi
}
check "overflow refused with 429" "$([[ $n429 -gt 0 && $other -eq 0 ]] && echo 1)" \
  "$n429 x 429, $n504 x 504 (slots used by the slow provider), $other other"
check "429s answered at once" "$([[ $slow429 -eq 0 ]] && echo 1)" "slowest 429 took ${max429}s (limit ${FAST}s)"
read -r hcode htime <<<"$health"
check "primary /health stays up" "$([[ $hcode == 200 ]] && is_fast "$htime" && echo 1)" "$hcode in ${htime}s"
read -r scode stime <<<"$secondary"
check "secondary unaffected" "$([[ $scode == 200 ]] && is_fast "$stime" && echo 1)" "$scode in ${stime}s"

[[ $fails -eq 0 ]] && echo "T8: PASS" || { echo "T8: FAIL ($fails)"; exit 1; }
