#!/usr/bin/env bash
# stress_10_5gb.sh — create 10 collections, insert ~5 GB of vector data.
#
# Wraps scripts/stress_50_2gb.py (the actual engine) with the right knobs for
# this scenario. Adds a separate log file and progress watcher.
#
# Defaults
#   - 10 collections, prefix=big-
#   - 5 GB raw f32 (dim 768)  → ~1.75M vectors total, ~175K per collection
#   - concurrency 6 (enough to keep server busy without 502s)
#   - resumes automatically on interrupt; retries 502s with backoff
#
# Usage
#   bash scripts/stress_10_5gb.sh                 # foreground
#   bash scripts/stress_10_5gb.sh --background    # nohup, tail the log
#   bash scripts/stress_10_5gb.sh --teardown      # delete big-* and exit
#   bash scripts/stress_10_5gb.sh --status        # show last log line + count
#
# Override anything via env:
#   COLLECTIONS=20 GB=10 DIM=512 PREFIX=mass- bash scripts/stress_10_5gb.sh

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/scripts/stress_50_2gb.py"
LOG="${LOG:-/tmp/stress_10_5gb.log}"

COLLECTIONS="${COLLECTIONS:-10}"
GB="${GB:-5}"
DIM="${DIM:-768}"
PREFIX="${PREFIX:-big-}"
CONCURRENCY="${CONCURRENCY:-6}"
GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
export GATEWAY

bold()   { printf '\033[1m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
red()    { printf '\033[31m%s\033[0m\n' "$*"; }
gray()   { printf '\033[90m%s\033[0m\n' "$*"; }

show_status() {
  echo "log: $LOG"
  if [[ -f "$LOG" ]]; then
    grep -E '^(Inserted|Raw f32|Elapsed|Throughput|Collections|Failures)' "$LOG" | tail -10 || true
    echo
    gray "last progress line:"
    tr '\r' '\n' < "$LOG" | tail -n 1
  else
    yellow "no log yet"
  fi
  echo
  local n
  n=$(curl -s "$GATEWAY/v1/collections" | python3 -c "
import sys,json
try:
    cs=json.load(sys.stdin)
    print(len([c for c in cs if c.startswith('$PREFIX')]))
except Exception:
    print('?')
")
  echo "$PREFIX* collections currently in DB: $n"
  if pgrep -f stress_50_2gb.py >/dev/null; then
    green "writer process: running"
  else
    yellow "writer process: not running"
  fi
}

case "${1:-}" in
  --teardown)
    exec python3 "$PY" --teardown --prefix "$PREFIX"
    ;;
  --status)
    show_status
    exit 0
    ;;
esac

bold "Stress test — 10 collections × 5 GB"
gray "  collections:  $COLLECTIONS  (prefix=$PREFIX)"
gray "  target:       ${GB} GB raw f32"
gray "  dimension:    $DIM"
gray "  concurrency:  $CONCURRENCY"
gray "  gateway:      $GATEWAY"
gray "  log:          $LOG"
echo

# Sanity: gateway reachable
code=$(curl -s -o /dev/null -w '%{http_code}' "$GATEWAY/v1/collections")
[[ "$code" == "200" ]] || { red "gateway not reachable ($GATEWAY) — got $code"; exit 1; }

CMD=(python3 "$PY"
  --collections "$COLLECTIONS"
  --target-gb "$GB"
  --dim "$DIM"
  --prefix "$PREFIX"
  --concurrency "$CONCURRENCY"
)

if [[ "${1:-}" == "--background" ]]; then
  shift || true
  : > "$LOG"
  nohup "${CMD[@]}" "$@" > "$LOG" 2>&1 &
  pid=$!
  green "launched in background  pid=$pid"
  gray "  follow:  tail -f $LOG"
  gray "  status:  bash scripts/stress_10_5gb.sh --status"
  gray "  abort:   kill $pid"
  gray "  cleanup: bash scripts/stress_10_5gb.sh --teardown"
  exit 0
fi

# Foreground: stream output AND tee to log
"${CMD[@]}" "$@" 2>&1 | tee "$LOG"
