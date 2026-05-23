#!/usr/bin/env bash
# Run a representative "large data" test against an in-process engine.
# For a live cluster test, use scripts/load_test.py against a running gateway.
#
# Examples:
#   ./scripts/large_data_test.sh                # 500k vectors, dim 768
#   ./scripts/large_data_test.sh xlarge         # 2M vectors (requires several GB RAM)
#   ./scripts/large_data_test.sh medium recall  # 100k + recall measurement
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCENARIO="${1:-large}"
RECALL_FLAG=""
if [[ "${2:-}" == "recall" ]]; then
  RECALL_FLAG="--measure-recall"
fi

echo "=== VectorDB large-data test ==="
echo "Scenario: $SCENARIO"
echo "Host: $(uname -srm 2>/dev/null || true)"
echo "Cores: $(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo '?')"
echo "Free RAM: $(free -h 2>/dev/null | awk '/^Mem:/{print $7}' || vm_stat 2>/dev/null | awk '/free/{print $3}' | head -1 || echo '?')"
echo

cargo run -p vectordb-bench --release -- \
  --scenario "$SCENARIO" \
  --workers "$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)" \
  $RECALL_FLAG

echo
echo "Done. For an end-to-end test against a running gateway, see:"
echo "  python scripts/load_test.py --help"
