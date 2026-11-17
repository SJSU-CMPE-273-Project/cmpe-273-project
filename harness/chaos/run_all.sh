#!/usr/bin/env bash
# Runs T0-T8 and saves each output plus a summary table to docs/results/<timestamp>/.
#
#   harness/chaos/run_all.sh            # all
#   harness/chaos/run_all.sh t3 t7 t8   # some
# Tests that need a service that isn't running report SKIP (exit 2), not FAIL.
# Any env var the single tests read (TARGET, RATE, OUTAGE_SECONDS, ...) passes through.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
STAMP=$(date +%Y-%m-%d-%H%M)
OUT=${RESULTS_DIR:-$ROOT/docs/results/$STAMP}
mkdir -p "$OUT"
tests=("$@"); [[ ${#tests[@]} -eq 0 ]] && tests=(t0 t1 t2 t3 t4 t5 t6 t7 t8)

rows=()
for t in "${tests[@]}"; do
  script=$(ls "$HERE/${t}_"*.sh 2>/dev/null | head -n 1)
  [[ -z $script ]] && { echo "no script for $t"; continue; }
  name=$(echo "$t" | tr a-z A-Z)
  echo "=================== $name ($(basename "$script"))"
  start=$SECONDS
  "$script" 2>&1 | tee "$OUT/$name.txt"
  code=${PIPESTATUS[0]}
  case $code in 0) r=PASS ;; 2) r=SKIP ;; *) r=FAIL ;; esac
  why=$(grep -E "^$name: " "$OUT/$name.txt" | tail -n 1 | sed "s/^$name: //")
  rows+=("| $name | $r | $((SECONDS - start))s | ${why:-$r} | [$name.txt]($name.txt) |")
done

{
  echo "# Failure-test run $STAMP"
  echo
  echo "Commit \`$(git -C "$ROOT" rev-parse --short HEAD)\`. Target and rate per test are in each file's header."
  echo
  echo "| Test | Result | Took | Verdict line | Output |"
  echo "|---|---|---|---|---|"
  printf '%s\n' "${rows[@]}"
} >"$OUT/summary.md"
echo; cat "$OUT/summary.md"
