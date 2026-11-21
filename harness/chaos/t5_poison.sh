#!/usr/bin/env bash
# T5: a poison message (provider always says no). Pass: it lands in the DLQ, the rest
# of the account still delivers, and a replay delivers it exactly once.
#
# The replay is the worker's audited operator replay (delivery-worker replay EVENT OPERATOR),
# the same path the Ops Console's Replay button uses. Re-publishing the event would not
# work: ingest dedupes by event_id, so a copy never reaches the worker. A second replay
# of the same event must be refused (exactly one replay per event).
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
header T5 "poison message -> DLQ -> replay once" "target=$E2E_TARGET"
require_e2e
require_up oracle postgres
trap 'oracle_clear_rules || true' EXIT

oracle_reset
plan=(); while IFS= read -r l; do plan+=("$l"); done < <(loadgen plan --count 5 --accounts 1 --run-tag "t5$RANDOM" --out /runs/t5.jsonl 2>/dev/null | head -n 5)
read -r poison account _ <<<"${plan[2]}"
oracle_rule "{\"event_id\":\"$poison\",\"action\":\"reject\"}"
say "account $account: poison = 3rd event $poison"
loadgen replay --target "$E2E_TARGET" --from /runs/t5.jsonl --fresh-timestamps >/dev/null
fails=0
# "no loss" passes only once the worker has written the poison as dead_lettered in Postgres.
wait_accounted 180 || fails=1
echo "before replay: poison delivered $(oq count "$poison")x (want 0); account order: $(oq sequences "$account")"
[[ $(oq count "$poison") == 0 ]] || fails=$((fails + 1))

say "provider fixed; replaying the poison from the DLQ"
oracle_clear_rules
dc exec -T "$WORKER_SERVICE" /delivery-worker replay "$poison" chaos-t5 || { say "replay command failed"; fails=$((fails + 1)); }
for _ in $(seq 1 30); do [[ $(oq count "$poison") -ge 1 ]] && break; sleep 2; done
sleep 5 # give a second copy time to show up, if there is one
n=$(oq count "$poison")
echo "after replay: poison delivered ${n}x (want 1)"
[[ $n == 1 ]] || fails=$((fails + 1))
if dc exec -T "$WORKER_SERVICE" /delivery-worker replay "$poison" chaos-t5-again >/dev/null 2>&1; then
  sleep 5; echo "second replay was accepted (want refused); poison now delivered $(oq count "$poison")x"
  fails=$((fails + 1))
else
  echo "second replay refused (want refused)"
fi
FINISH_IGNORE=latency finish $fails
