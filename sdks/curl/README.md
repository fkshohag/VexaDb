# VectorDB — curl cookbook

Every endpoint exposed by `vectordb-gateway` (HTTP/JSON) callable from a plain
shell. Set these once per session:

```bash
export VECTORDB_URL=http://127.0.0.1:8080
export VECTORDB_API_KEY=    # set if the gateway enforces auth
AUTH=(-H "x-api-key: $VECTORDB_API_KEY" -H "Authorization: Bearer $VECTORDB_API_KEY")
```

Throughout this doc the placeholder `$AUTH` expands to those two headers.
Drop them entirely when auth is disabled (`VECTORDB_API_KEYS` unset on the
gateway).

## Health & readiness

```bash
curl -s "$VECTORDB_URL/health"      # returns {"status":"ok"} when reachable
curl -s "$VECTORDB_URL/live"        # 200 if process is alive
curl -s "$VECTORDB_URL/ready"       # 200 only when storage is ready
curl -s "$VECTORDB_URL/metrics"     # Prometheus exposition
```

## Collections

```bash
# list
curl -s "${AUTH[@]}" "$VECTORDB_URL/v1/collections"

# create (cosine, 768-dim, with a keyword payload index and BM25 text field)
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "demo",
    "dimension": 768,
    "metric": "cosine",
    "payload_indexes": [
      {"field": "category", "kind": "keyword"},
      {"field": "score",    "kind": "numeric"}
    ],
    "bm25_text_field": "text",
    "sparse_enabled": false,
    "scalar_quantization": false
  }'

# describe
curl -s "${AUTH[@]}" "$VECTORDB_URL/v1/collections/demo"

# delete
curl -s "${AUTH[@]}" -X DELETE "$VECTORDB_URL/v1/collections/demo"
```

`metric` is one of `cosine`, `euclidean`, `dot`. `payload_indexes[].kind` is
`keyword`, `numeric`, or `bool`.

## Points (vectors)

```bash
# upsert one or many
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/upsert" \
  -H 'Content-Type: application/json' \
  -d '{
    "points": [
      {"id":"doc-1","values":[0.1,0.2,0.3,0.4],"payload":{"category":"news","text":"hello world"}},
      {"id":"doc-2","values":[0.5,0.1,0.0,0.9],"payload":{"category":"blog"}}
    ]
  }'

# bulk upsert (server chunks at chunk_size, default 500)
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/bulk" \
  -H 'Content-Type: application/json' \
  -d @points.json

# get one point
curl -s "${AUTH[@]}" "$VECTORDB_URL/v1/collections/demo/points/doc-1"

# delete by ids
curl -s "${AUTH[@]}" -X DELETE "$VECTORDB_URL/v1/collections/demo/points" \
  -H 'Content-Type: application/json' \
  -d '{"ids":["doc-1","doc-2"]}'
```

## Search

```bash
# dense
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/search" \
  -H 'Content-Type: application/json' \
  -d '{"vector":[0.1,0.2,0.3,0.4],"top_k":5}'

# filtered
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/search" \
  -H 'Content-Type: application/json' \
  -d '{
    "vector":[0.1,0.2,0.3,0.4],
    "top_k":5,
    "filter":{"must":[{"key":"category","match":{"value":"news"}}]}
  }'

# range filter on numeric payload index
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/search" \
  -H 'Content-Type: application/json' \
  -d '{
    "vector":[0.1,0.2,0.3,0.4],
    "top_k":5,
    "filter":{"must":[{"key":"score","range":{"gte":10,"lt":50}}]}
  }'

# any-of (set membership) and must_not (negation)
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/search" \
  -H 'Content-Type: application/json' \
  -d '{
    "vector":[0.1,0.2,0.3,0.4],
    "top_k":5,
    "filter":{
      "must":[{"key":"category","any_of":{"any":["news","blog"]}}],
      "must_not":[{"key":"score","range":{"lt":10}}]
    }
  }'

# hybrid (dense + BM25 text), reciprocal-rank fusion
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/search" \
  -H 'Content-Type: application/json' \
  -d '{
    "vector":[0.1,0.2,0.3,0.4],
    "top_k":5,
    "text_query":"hello",
    "search_mode":"hybrid_rrf"
  }'

# hybrid weighted (alpha = dense weight, 1-alpha = lexical/sparse)
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/search" \
  -H 'Content-Type: application/json' \
  -d '{
    "vector":[0.1,0.2,0.3,0.4],
    "top_k":5,
    "text_query":"hello",
    "search_mode":"hybrid_weighted",
    "hybrid_alpha":0.7
  }'
```

`search_mode` accepts: `dense` (default), `sparse`, `bm25`, `hybrid_rrf`, `hybrid_weighted`.

### Filter DSL

```text
{
  "must":     [ <Condition>, ... ],   // AND
  "must_not": [ <Condition>, ... ],   // NOR
  "should":   [ <Condition>, ... ]    // OR (at least one)
}
```

Each `<Condition>` is one of:

```jsonc
{ "key": "category", "match":  {"value": "news"} }      // equals
{ "key": "category", "any_of": {"any": ["news","blog"]} } // set membership
{ "key": "score",    "range":  {"gte": 10, "lt": 50} }  // numeric range
{ "key": "author",   "exists": {"exists": true} }       // field present
{ "filter": { ... } }                                    // nested boolean tree
```

For best performance, register the field in `payload_indexes` at collection
creation (`keyword` for strings/booleans, `numeric` for numbers). Unindexed
fields still match — they're scanned in memory.

## Admin

```bash
# trigger a rebalance sweep (dry-run reports what would move)
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/admin/rebalance" \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":true}'

# rebalance status
curl -s "${AUTH[@]}" "$VECTORDB_URL/v1/admin/rebalance"

# cluster topology
curl -s "${AUTH[@]}" "$VECTORDB_URL/v1/admin/cluster"

# WAL compaction (optionally take a fresh snapshot first)
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/admin/compact-wal" \
  -H 'Content-Type: application/json' \
  -d '{"snapshot_first":true}'

# rebuild ANN index for a collection
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/collections/demo/reindex"
```

## Snapshots

```bash
curl -s "${AUTH[@]}" -X POST "$VECTORDB_URL/v1/snapshots"          # create
curl -s "${AUTH[@]}" "$VECTORDB_URL/v1/snapshots"                  # list
curl -s "${AUTH[@]}" -X DELETE "$VECTORDB_URL/v1/snapshots/snap-...."  # delete
```

## End-to-end demo

[`quickstart.sh`](./quickstart.sh) runs through the full lifecycle (create →
bulk upsert → dense / filtered / hybrid search → cluster status → cleanup).
Requires `bash`, `curl`, `jq`, and `python3` (only for generating random
vectors).

```bash
./sdks/curl/quickstart.sh
```
