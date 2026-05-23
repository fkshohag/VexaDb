# REST API

The HTTP gateway (`vectordb-gateway`) exposes all functionality over JSON. By
default it listens on `:8080` and proxies to a gRPC server or router.

- **Auth** (when enabled): `x-api-key: <key>` *or* `Authorization: Bearer <key>`
- **Content-Type**: `application/json`
- **Probes** (no auth required): `/health`, `/live`, `/ready`, `/metrics`

> All payloads are pretty-printed in this doc; production calls don’t need
> whitespace.

---

## Health & probes

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Returns `{ "status": "ok" }` (server status). |
| `GET` | `/live` | `200` if process up. |
| `GET` | `/ready` | `200` only if `HealthResponse.ready` (leader, if Raft). |
| `GET` | `/metrics` | Prometheus text format (gateway counters). |

Server-side Prometheus is at `[metrics].listen` (default `:9090`).

---

## Collections

### `POST /v1/collections`

Create a collection.

```json
{
  "name": "embeddings",
  "dimension": 1536,
  "metric": "cosine",
  "payload_indexes": [
    {"field": "category", "kind": "keyword"},
    {"field": "price", "kind": "numeric"},
    {"field": "in_stock", "kind": "bool"}
  ],
  "sparse_enabled": false,
  "bm25_text_field": "text",
  "scalar_quantization": false
}
```

Response: `201 Created`.

| Field | Type | Notes |
|-------|------|-------|
| `metric` | `cosine` \| `euclidean` \| `dot` | Default `cosine`. |
| `payload_indexes[].kind` | `keyword` \| `numeric` \| `bool` | Filter pushdown. |
| `bm25_text_field` | string | Path in JSON payload to index for BM25. |
| `sparse_enabled` | bool | Enable sparse inverted index. |
| `scalar_quantization` | bool | Store 8-bit quantized codes alongside f32. |

### `GET /v1/collections`

```json
["embeddings", "kb"]
```

### `GET /v1/collections/:name`

```json
{
  "spec": "...debug-formatted CollectionSpec...",
  "vector_count": 12345
}
```

### `DELETE /v1/collections/:name`

`204 No Content`.

---

## Vectors

### `POST /v1/collections/:name/upsert`

Insert or replace points.

```json
{
  "points": [
    {
      "id": "doc-1",
      "values": [0.1, 0.2, 0.3],
      "payload": {"category": "books", "price": 19.99, "text": "..."},
      "sparse": {"indices": [10, 25], "values": [0.7, 0.4]}
    }
  ]
}
```

Response:

```json
{ "upserted": 1 }
```

### `POST /v1/collections/:name/bulk`

Batched upsert (durable WAL chunking).

```json
{ "points": [ ... ], "chunk_size": 500 }
```

Response: `{ "upserted": N }`.

### `POST /v1/collections/:name/search`

Dense / hybrid / lexical search.

```json
{
  "vector": [0.1, 0.2, 0.3],
  "top_k": 10,
  "filter": {
    "must": [{"key": "category", "match": {"value": "books"}}],
    "must_not": [],
    "should": [{"key": "price", "range": {"lte": 50}}]
  },
  "sparse_query": {"indices": [10], "values": [1.0]},
  "text_query": "vector database",
  "search_mode": "hybrid_rrf",
  "hybrid_alpha": 0.5
}
```

| `search_mode` | Behaviour |
|---------------|-----------|
| `dense` (default) | HNSW only |
| `sparse` | Sparse inverted index dot product |
| `bm25` | BM25 over `bm25_text_field` |
| `hybrid_rrf` | RRF fusion of all available signals |
| `hybrid_weighted` | `α·dense + (1-α)·lexical` (uses `hybrid_alpha`) |

Response:

```json
[
  {"id": "doc-1", "score": 0.97},
  {"id": "doc-2", "score": 0.85}
]
```

See [`filter-dsl.md`](filter-dsl.md) for filter syntax.

### `DELETE /v1/collections/:name/points`

```json
{ "ids": ["a", "b"] }
```

Response: `{ "deleted": 2 }`.

### `GET /v1/collections/:name/points/:id`

```json
{
  "id": "doc-1",
  "values": [0.1, 0.2, 0.3],
  "payload": {"category": "books"}
}
```

`404` if not found.

---

## Snapshots & operations

### Snapshots

| Method | Path | Notes |
|--------|------|-------|
| `POST` | `/v1/snapshots` | Create snapshot, returns `{id, created_at_ms, path}`. |
| `GET` | `/v1/snapshots` | List snapshots. |
| `DELETE` | `/v1/snapshots/:id` | Delete one snapshot. |

### Admin

| Method | Path | Body | Notes |
|--------|------|------|-------|
| `POST` | `/v1/admin/compact-wal` | `{"snapshot_first": true}` | Rewrite WAL from in-memory state. |
| `POST` | `/v1/collections/:name/reindex` | — | Rebuild HNSW from stored vectors. |

`compact-wal` returns:

```json
{
  "entries_before": 8123,
  "entries_after": 124,
  "snapshot": {"id": "snap-1716000000000", "path": "..."}
}
```

`reindex` returns `{ "vectors_reindexed": N }`.

---

## Errors

The gateway returns standard HTTP status codes:

| Status | Meaning |
|--------|---------|
| `400` | Invalid request body or filter |
| `401` | Auth missing or wrong API key |
| `404` | Collection or point not found |
| `409` | Collection already exists |
| `412` | Wrong shard (router or leader required) |
| `502` | Upstream gRPC error |
| `503` | Not ready (e.g., follower in Raft mode) |

Error responses are plain text bodies describing the cause.

---

## Examples

```bash
# Create + populate
curl -s -X POST :8080/v1/collections \
  -d '{"name":"books","dimension":3,"payload_indexes":[{"field":"category","kind":"keyword"}]}' \
  -H 'Content-Type: application/json'

curl -s -X POST :8080/v1/collections/books/upsert \
  -H 'Content-Type: application/json' \
  -d '{"points":[
    {"id":"a","values":[1,0,0],"payload":{"category":"books"}},
    {"id":"b","values":[0.9,0.1,0],"payload":{"category":"movies"}}
  ]}'

# Filtered search
curl -s -X POST :8080/v1/collections/books/search \
  -H 'Content-Type: application/json' \
  -d '{"vector":[1,0,0],"top_k":3,"filter":{"must":[{"key":"category","match":{"value":"books"}}]}}'

# Snapshot
curl -s -X POST :8080/v1/snapshots
```
