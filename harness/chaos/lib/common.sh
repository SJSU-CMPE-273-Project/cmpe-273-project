# Shared helpers for harness/chaos/t*.sh. Source it; don't run it.
#
# Exit codes every test uses: 0 PASS, 1 FAIL, 2 SKIP (something it needs isn't running).
# env: ORACLE (http://localhost:18080)  TOXI (http://localhost:8474)
#      ORACLE_ADMIN_TOKEN  WORKER_SERVICE (delivery-worker)  DRAIN_TIMEOUT (180 s)

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
CHAOS=$ROOT/harness/chaos
export ORACLE=${ORACLE:-http://localhost:18080}
TOXI=${TOXI:-http://localhost:8474}
WORKER_SERVICE=${WORKER_SERVICE:-delivery-worker}
# How end-to-end tests send traffic. iggy = producer path (Iggy -> ingest bridge -> Kafka
# -> worker). The worker only delivers events the bridge admitted, so kafka (straight onto
# notifications.requested) no longer reaches the adapters; keep it for Kafka-only checks.
E2E_TARGET=${E2E_TARGET:-iggy}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-180}
OAUTH=()
[[ -n "${ORACLE_ADMIN_TOKEN:-}" ]] && OAUTH=(-H "Authorization: Bearer $ORACLE_ADMIN_TOKEN")

dc() { docker compose --project-directory "$ROOT" "$@"; }
oq() { python3 "$CHAOS/lib/oracle_query.py" "$@"; }
now_iso() { python3 -c 'from datetime import datetime,timezone;print(datetime.now(timezone.utc).isoformat())'; }
say() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# Load generator in the compose network (oracle, toxiproxy, kafka by service name).
loadgen() { dc --profile harness run --rm -T load-generator "$@"; }

header() { # TEST "description" key=value...
  TEST=$1
  echo "== $1: $2"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)   commit: $(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null)"
  shift 2
  [[ $# -gt 0 ]] && echo "params: $*"
  echo
}

skip() { echo "$TEST: SKIP ($1)"; exit 2; }

worker_running() { dc ps --status running --services 2>/dev/null | grep -qx "$WORKER_SERVICE"; }
require_worker() {
  worker_running || skip "needs $WORKER_SERVICE (Lane 2) running in compose; nothing to retry, fail over or dead-letter without it"
}
# Everything on the producer -> adapter path, plus the worker.
require_e2e() {
  require_worker
  if [[ $E2E_TARGET == iggy ]]; then require_up iggy ingest-bridge kafka; else require_up kafka; fi
}
require_up() { # service...
  local s
  for s in "$@"; do
    dc ps --status running --services 2>/dev/null | grep -qx "$s" || skip "needs '$s' running (make up)"
  done
}

oracle_reset() { curl -sf -X POST ${OAUTH[@]+"${OAUTH[@]}"} "$ORACLE/admin/reset" >/dev/null; }
oracle_rule() { curl -sf -X POST ${OAUTH[@]+"${OAUTH[@]}"} -H "Content-Type: application/json" -d "$1" "$ORACLE/admin/rules" >/dev/null; }
oracle_clear_rules() { curl -sf -X DELETE ${OAUTH[@]+"${OAUTH[@]}"} "$ORACLE/admin/rules" >/dev/null; }
oracle_text() { curl -sf "$ORACLE/report?format=text"; }

toxi_enable() { curl -sf -o /dev/null -X POST "$TOXI/proxies/$1" -d '{"enabled": true}'; }
toxi_disable() { curl -sf -o /dev/null -X POST "$TOXI/proxies/$1" -d '{"enabled": false}'; }

# Wait until the oracle says every expected event is delivered, expired or dead-lettered.
wait_accounted() { # [timeout seconds]
  local limit=${1:-$DRAIN_TIMEOUT} start=$SECONDS r
  while :; do
    r=$(oq check "no loss" 2>/dev/null || echo ERROR)
    [[ $r == PASS ]] && { say "all events accounted for after $((SECONDS - start))s"; return 0; }
    (( SECONDS - start >= limit )) && { say "still '$r' after ${limit}s"; return 1; }
    sleep 2
  done
}

# Print the oracle's table, then the verdict. extra_fails counts test-specific failures.
# FINISH_IGNORE=latency: show that check but don't grade on it. The 2 s p99 SLO is for
# normal load (T0); fault tests grade on their own criteria (harness/README.md test matrix).
finish() { # extra_fails
  local extra=${1:-0}
  echo
  oracle_text
  echo
  [[ -n ${FINISH_IGNORE:-} ]] && echo "graded on every check except: $FINISH_IGNORE (reported above, not graded)"
  if oq passed ${FINISH_IGNORE:+--ignore "$FINISH_IGNORE"} && [[ $extra -eq 0 ]]; then echo "$TEST: PASS"; exit 0; fi
  echo "$TEST: FAIL"; exit 1
}

# Pull the summary JSON (last stdout line) out of a load generator run.
summary_field() { # file python-expr-on-s   e.g. summary_field out.txt 's["sink"].get("http_429",0)'
  tail -n 1 "$1" | python3 -c "import json,sys; s=json.load(sys.stdin)['summary']; print($2)"
}
