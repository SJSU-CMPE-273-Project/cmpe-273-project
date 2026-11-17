#!/usr/bin/env bash
# T5: a poison message (provider always says no). Pass: it lands in the DLQ, the rest
# of the account still delivers, and a replay delivers it exactly once.
#
# The replay re-publishes the original event with the SAME event_id, which is what
# the Ops Console's DLQ replay does (see DeadLetter in the contract).
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
header T5 "poison message -> DLQ -> replay once" "target=kafka"
require_worker
require_up oracle kafka postgres
trap 'oracle_clear_rules || true' EXIT

oracle_reset
plan=(); while IFS= read -r l; do plan+=("$l"); done < <(loadgen plan --count 5 --accounts 1 --run-tag "t5$RANDOM" --out /runs/t5.jsonl | head -n 5)
read -r poison account _ <<<"${plan[2]}"
oracle_rule "{\"event_id\":\"$poison\",\"action\":\"reject\"}"
say "account $account: poison = 3rd event $poison"
loadgen replay --target kafka --from /runs/t5.jsonl --fresh-timestamps >/dev/null
fails=0
# "no loss" passes only once the worker has written the poison as dead_lettered in Postgres.
wait_accounted 180 || fails=1
echo "before replay: poison delivered $(oq count "$poison")x (want 0); account order: $(oq sequences "$account")"
[[ $(oq count "$poison") == 0 ]] || fails=$((fails + 1))

say "provider fixed; replaying the poison from the DLQ"
oracle_clear_rules
loadgen replay --target kafka --from /runs/t5.jsonl --only "$poison" >/dev/null
for _ in $(seq 1 30); do [[ $(oq count "$poison") -ge 1 ]] && break; sleep 2; done
sleep 5 # give a second copy time to show up, if there is one
n=$(oq count "$poison")
echo "after replay: poison delivered ${n}x (want 1)"
[[ $n == 1 ]] || fails=$((fails + 1))
finish $fails
