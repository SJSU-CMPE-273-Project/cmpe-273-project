#!/usr/bin/env bash
# T4: message N for one account fails twice, then succeeds. Pass: N, N+1, N+2 arrive in order.
#
# Per-account ordering: N+1 must wait for N's retries, not overtake it.
# The oracle plays a provider that returns 503 for N twice (FAIL_TIMES).
#
# env: FAIL_TIMES=2
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
TIMES=${FAIL_TIMES:-2}
header T4 "message N fails ${TIMES}x, then N+1 and N+2 must wait" "target=$E2E_TARGET"
require_e2e
require_up oracle
trap 'oracle_clear_rules || true' EXIT

oracle_reset
plan=(); while IFS= read -r l; do plan+=("$l"); done < <(loadgen plan --count 3 --accounts 1 --run-tag "t4$RANDOM" --out /runs/t4.jsonl | head -n 3)
read -r n_id account _ <<<"${plan[0]}"
oracle_rule "{\"event_id\":\"$n_id\",\"action\":\"fail\",\"times\":$TIMES}"
say "account $account: rule set on N=$n_id"
loadgen replay --target "$E2E_TARGET" --from /runs/t4.jsonl --fresh-timestamps >/dev/null
fails=0
wait_accounted 120 || fails=1
seqs=$(oq sequences "$account")
echo "delivered order for $account: $seqs (want 1 2 3)"
[[ $seqs == "1 2 3" ]] || fails=$((fails + 1))
FINISH_IGNORE=latency finish $fails
