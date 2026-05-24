#!/usr/bin/env bash
# End-to-end VectorDB demo using only curl + jq + python3.
#
#   sdks/curl/quickstart.sh
#
# Honors VECTORDB_URL (default http://127.0.0.1:8080) and VECTORDB_API_KEY.

set -euo pipefail

URL="${VECTORDB_URL:-http://127.0.0.1:8080}"
KEY="${VECTORDB_API_KEY:-}"
COLLECTION="${COLLECTION:-curl-quickstart}"
DIM="${DIM:-8}"
N="${N:-100}"

AUTH=()
if [[ -n "$KEY" ]]; then
  AUTH+=(-H "x-api-key: $KEY" -H "Authorization: Bearer $KEY")
fi

# Wrappers so commands stay short. http_code captures HTTP status.
# `${AUTH[@]+"${AUTH[@]}"}` safely expands an empty array under `set -u`.
api() {
  local method="$1" path="$2"; shift 2
  curl -sS ${AUTH[@]+"${AUTH[@]}"} -X "$method" "$URL$path" "$@"
}
api_code() {
  local method="$1" path="$2"; shift 2
  curl -sS -o /dev/null -w '%{http_code}' ${AUTH[@]+"${AUTH[@]}"} -X "$method" "$URL$path" "$@"
}

step() { printf '\n=== %s ===\n' "$*"; }

for tool in curl jq python3; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 1; }
done

step "health"
api GET /health | jq .

step "cleanup any prior collection"
code=$(api_code DELETE "/v1/collections/$COLLECTION" || echo 502)
echo "delete -> HTTP $code"

step "create collection ($COLLECTION, dim=$DIM, cosine)"
api POST /v1/collections \
  -H 'Content-Type: application/json' \
  -d "$(cat <<JSON
{
  "name": "$COLLECTION",
  "dimension": $DIM,
  "metric": "cosine",
  "payload_indexes": [
    {"field": "category", "kind": "keyword"},
    {"field": "score", "kind": "numeric"}
  ],
  "bm25_text_field": "text"
}
JSON
)"
echo "ok"

step "bulk upsert $N points"
python3 - "$DIM" "$N" <<'PY' > /tmp/vectordb_points.json
import json, random, sys
dim, n = int(sys.argv[1]), int(sys.argv[2])
random.seed(42)
points = [
    {
        "id": f"doc-{i:03d}",
        "values": [random.gauss(0.0, 1.0) for _ in range(dim)],
        "payload": {
            "category": ["news", "blog", "paper"][i % 3],
            "score": i,
            "text": f"document {i} about quickstart",
        },
    }
    for i in range(n)
]
print(json.dumps({"points": points, "chunk_size": 32}))
PY
api POST "/v1/collections/$COLLECTION/bulk" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/vectordb_points.json | jq .

step "dense search top-5"
python3 - "$DIM" <<'PY' > /tmp/vectordb_query.json
import json, random, sys
dim = int(sys.argv[1])
random.seed(7)
q = [random.gauss(0.0, 1.0) for _ in range(dim)]
print(json.dumps({"vector": q, "top_k": 5}))
PY
api POST "/v1/collections/$COLLECTION/search" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/vectordb_query.json | jq .

step "filtered search (category=news)"
python3 - "$DIM" <<'PY' > /tmp/vectordb_query_filtered.json
import json, random, sys
dim = int(sys.argv[1])
random.seed(7)
q = [random.gauss(0.0, 1.0) for _ in range(dim)]
print(json.dumps({
    "vector": q,
    "top_k": 5,
    "filter": {"must": [{"key": "category", "match": {"value": "news"}}]},
}))
PY
api POST "/v1/collections/$COLLECTION/search" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/vectordb_query_filtered.json | jq .

step "hybrid (RRF) search"
python3 - "$DIM" <<'PY' > /tmp/vectordb_query_hybrid.json
import json, random, sys
dim = int(sys.argv[1])
random.seed(7)
q = [random.gauss(0.0, 1.0) for _ in range(dim)]
print(json.dumps({
    "vector": q,
    "top_k": 5,
    "text_query": "quickstart",
    "search_mode": "hybrid_rrf",
}))
PY
api POST "/v1/collections/$COLLECTION/search" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/vectordb_query_hybrid.json | jq .

step "get one point"
api GET "/v1/collections/$COLLECTION/points/doc-000" | jq .

step "cluster status"
api GET /v1/admin/cluster \
  | jq '{shard_count, replication_factor, nodes: (.nodes | length)}'

step "delete two points"
api DELETE "/v1/collections/$COLLECTION/points" \
  -H 'Content-Type: application/json' \
  -d '{"ids":["doc-000","doc-001"]}' | jq .

step "drop collection"
code=$(api_code DELETE "/v1/collections/$COLLECTION")
echo "delete -> HTTP $code"

echo
echo "done."
