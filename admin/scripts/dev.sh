#!/usr/bin/env bash
# Run the admin panel in dev mode:
#   - Go backend on :8090 (proxies /api/* to the VectorDB gateway)
#   - Vite dev server on :5173 (HMR; proxies /api to the Go backend)
#
# Usage:
#   ./scripts/dev.sh                     # uses default upstream http://127.0.0.1:8080
#   VECTORDB_URL=http://host:8080 ./scripts/dev.sh
#   VECTORDB_API_KEY=secret ./scripts/dev.sh
#
# Open http://127.0.0.1:5173

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

UPSTREAM="${VECTORDB_URL:-http://127.0.0.1:8080}"

cleanup() {
  echo "stopping…"
  if [[ -n "${BACKEND_PID:-}" ]]; then kill "$BACKEND_PID" 2>/dev/null || true; fi
  if [[ -n "${FRONTEND_PID:-}" ]]; then kill "$FRONTEND_PID" 2>/dev/null || true; fi
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "▶ backend: go run ./backend  (upstream=$UPSTREAM)"
( cd backend && VECTORDB_URL="$UPSTREAM" go run . ) &
BACKEND_PID=$!

# Wait briefly for the backend to come up before starting Vite.
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -sSf http://127.0.0.1:8090/config.json >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

if [[ ! -d frontend/node_modules ]]; then
  echo "▶ installing frontend deps (first run)"
  ( cd frontend && npm install )
fi

echo "▶ frontend: vite (http://127.0.0.1:5173)"
( cd frontend && npm run dev ) &
FRONTEND_PID=$!

wait "$BACKEND_PID" "$FRONTEND_PID"
