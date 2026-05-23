#!/usr/bin/env bash
# Run VectorDB micro-benchmarks and print a summary report.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "=== VectorDB benchmarks ==="
echo "Host: $(uname -srm 2>/dev/null || true)"
echo "Rust: $(rustc --version 2>/dev/null || echo 'n/a')"
echo

echo "--- Quick harness (vectordb-bench) ---"
cargo run -p vectordb-bench --release -- "$@"
echo

if [[ "${CRITERION:-0}" == "1" ]]; then
  echo "--- Criterion: vectordb-core ---"
  cargo bench -p vectordb-core -- --sample-size 10
  echo "--- Criterion: vectordb-storage ---"
  cargo bench -p vectordb-storage -- --sample-size 10
  echo
  echo "HTML reports: target/criterion/*/report/index.html"
fi

echo "Done. Set CRITERION=1 for full Criterion suites (slower)."
