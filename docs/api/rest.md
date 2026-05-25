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

### `GET /v1/collections/:name/stats`

Structured collection statistics (preferred over parsing `describe`).

```json
{
  "name": "embeddings",
  "vector_count": 12345,
  "dimension": 1536,
  "metric": "cosine",
  "sparse_enabled": false,
  "bm25_text_field": "text",
  "payload_index_count": 2,
  "scalar_quantization": false
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
  "filter": "category == 'books' && price <= 50",
  "sparse_query": {"indices": [10], "values": [1.0]},
  "text_query": "vector database",
  "search_mode": "hybrid_rrf",
  "hybrid_alpha": 0.5,
  "output_fields": ["category", "text"],
  "with_payload": true,
  "with_vector": false
}
```

`filter` accepts **either** a string expression (Milvus-style) **or** the JSON Filter DSL object.

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
  {"id": "doc-1", "score": 0.97, "payload": {"category": "books", "text": "..."}},
  {"id": "doc-2", "score": 0.85, "payload": {"category": "books", "text": "..."}}
]
```

When `with_payload` is false, `payload` is omitted. When `with_vector` is true, a `vector` field is included.

### `POST /v1/collections/:name/query`

Filter-only retrieval (no query vector). Milvus `Query()` equivalent.

```json
{
  "filter": "category == 'books'",
  "ids": [],
  "limit": 100,
  "offset": 0,
  "output_fields": ["category", "text"],
  "with_payload": true,
  "with_vector": false
}
```

Response:

```json
{
  "points": [
    {"id": "doc-1", "payload": {"category": "books", "text": "..."}, "values": null}
  ]
}
```

See [`filter-dsl.md`](filter-dsl.md) for the JSON filter shape; string expressions use `==`, `!=`, `&&`, `||`, `in [...]`, `not in [...]`, and `exists(field)`.

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

## Authentication & RBAC

VexaDb supports three credential types:

| Header | Example | Use |
|--------|---------|-----|
| `Authorization: Basic <b64>` | `Basic cm9vdDpodW50ZXIy` | Username + password |
| `Authorization: Bearer <token>` | `Bearer 7c61...:secret` | API token (`tokenid:secret`) or legacy key |
| `x-api-key: <token>` | `x-api-key: 7c61...:secret` | Same as Bearer |

### Bootstrap

On first start, set `VECTORDB_ROOT_PASSWORD` to create a `root` superuser
(role: `admin`). Subsequent restarts skip bootstrap if any user exists.

For backward compatibility, `VECTORDB_API_KEYS` (comma-separated) continues to
grant superuser access without an associated user account.

### Built-in roles

| Role | Grants |
|------|--------|
| `admin` | `*` on `Global` and `Collection` (everything) |
| `read_write` | List/Describe/Stats + Search/Query/Get/Insert/Upsert/Delete |
| `read_only` | List/Describe/Stats + Search/Query/Get |

### Login & tokens

```
POST /v1/auth/login          {"username":"root","password":"..."} -> {token_id, token, user}
POST /v1/auth/tokens         {"description":"..."}               -> {token_id, token, user}
DELETE /v1/auth/tokens/:id
```

The plaintext `token` is shown **once**; pass it as `Bearer <token>` or
`x-api-key: <token>` afterward.

### Users

```
POST   /v1/users               {"name","password","roles":[]}
GET    /v1/users               -> ["alice", ...]
GET    /v1/users/:name         -> {name, disabled, created_at_ms, roles}
DELETE /v1/users/:name
PATCH  /v1/users/:name/password  {"old","new"}
POST   /v1/users/:name/roles/:role     (grant)
DELETE /v1/users/:name/roles/:role     (revoke)
```

### Roles

```
POST   /v1/roles               {"name","description"}
GET    /v1/roles               -> ["admin", ...]
GET    /v1/roles/:name         -> {name, description, grants: [...]}
DELETE /v1/roles/:name
POST   /v1/roles/:role/grants   {"object_type":"Collection","object_name":"books","privilege":"Search"}
DELETE /v1/roles/:role/grants   (same body)
```

Privileges: `CreateCollection`, `DropCollection`, `ListCollections`,
`DescribeCollection`, `CollectionStats`, `Search`, `Query`, `Insert`, `Upsert`,
`Delete`, `Get`, `Reindex`, `Snapshot`, `Rebalance`, `CompactWal`,
`ClusterStatus`, `ManageRbac`. `"*"` matches all.

### Privilege groups

```
POST   /v1/privilege-groups    {"name","privileges":["Search","Query"]}
GET    /v1/privilege-groups
DELETE /v1/privilege-groups/:name
PATCH  /v1/privilege-groups/:name   {"add":[],"remove":[]}
```

### Backup / restore

```
POST /v1/admin/rbac/backup     -> RBACMeta JSON snapshot
POST /v1/admin/rbac/restore    RBACMeta JSON
```

---

## Errors

The gateway returns standard HTTP status codes:

| Status | Meaning |
|--------|---------|
| `400` | Invalid request body or filter |
| `401` | Auth missing or wrong credentials |
| `403` | Authenticated but not authorized for this object/privilege |
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
