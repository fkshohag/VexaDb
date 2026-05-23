#!/usr/bin/env bash
# Remove macOS AppleDouble (._*) and .DS_Store files that break Docker on external volumes.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

count=0
while IFS= read -r -d '' f; do
  rm -f "$f"
  count=$((count + 1))
done < <(find . -name '._*' -print0 2>/dev/null)

while IFS= read -r -d '' f; do
  rm -f "$f"
  count=$((count + 1))
done < <(find . -name '.DS_Store' -print0 2>/dev/null)

if command -v dot_clean >/dev/null 2>&1; then
  dot_clean -m .
fi

echo "Removed $count macOS artifact file(s) under $ROOT"
echo "Re-run: docker compose build --no-cache && docker compose up"
