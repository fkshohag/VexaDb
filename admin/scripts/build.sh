#!/usr/bin/env bash
# Build the React frontend and a single Go binary that serves it.
#
# Output:
#   admin/frontend/dist/...            (React build)
#   admin/backend/vectordb-admin       (Go binary; serves dist/ and proxies /api)
#
# Usage:
#   ./scripts/build.sh
#   VECTORDB_URL=http://10.0.0.5:8080 ./scripts/build.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "▶ frontend: npm install && npm run build"
( cd frontend && npm install && npm run build )

echo "▶ backend: go build"
( cd backend && go build -o vectordb-admin . )

cat <<EOF

Done.

Run the admin panel with:
  cd admin/backend
  ./vectordb-admin --listen :8090 --upstream "\${VECTORDB_URL:-http://127.0.0.1:8080}"

Then open http://127.0.0.1:8090
EOF
