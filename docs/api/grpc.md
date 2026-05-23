# gRPC API

`vectordb-server` exposes the full surface as a tonic gRPC service. The
canonical schema lives in
[`crates/vectordb-proto/proto/vectordb.proto`](../../crates/vectordb-proto/proto/vectordb.proto).

- **Default port**: `:6334` for data nodes, `:6333` for routers (`all_in_one` uses `:6334`).
- **Auth**: same as REST — `x-api-key` or `authorization: Bearer ...` metadata.
- **Health**: `Health` RPC is exempt from auth so probes work.
- **Leader discovery**: writes from a follower fail with status
  `failed_precondition` and metadata `x-vectordb-leader: http://leader:port`.

---

## Service: `vectordb.v1.VectorService`

| RPC | Type | Purpose |
|-----|------|---------|
| `Health` | unary | Status, role, leader endpoint, ready bit |
| `CreateCollection` | unary | Create new collection from `CollectionSpec` |
| `DeleteCollection` | unary | Drop a collection |
| `ListCollections` | unary | List names |
| `DescribeCollection` | unary | Spec + vector_count |
| `Upsert` | unary | Insert/update points |
| `Search` | unary | Dense/sparse/BM25/hybrid search |
| `Delete` | unary | Delete by ids |
| `Get` | unary | Fetch one point with payload |
| `CreateSnapshot` / `ListSnapshots` / `DeleteSnapshot` | unary | Snapshot management |
| `BulkUpsert` | unary | Chunked bulk import |
| `ImportStream` | client streaming | High-throughput streaming ingest |
| `CompactWal` | unary | Rewrite WAL from current state (optionally snapshot first) |
| `ReindexCollection` | unary | Rebuild HNSW from stored vectors |

---

## Key messages

### `CollectionSpec`

```proto
message CollectionSpec {
  string name = 1;
  uint32 dimension = 2;
  DistanceMetric metric = 3;       // COSINE | EUCLIDEAN | DOT_PRODUCT
  uint32 m = 4;                    // HNSW parameter (default 16)
  uint32 ef_construction = 5;      // default 200
  uint32 ef_search = 6;            // default 64
  repeated PayloadFieldIndex payload_indexes = 7;
  bool sparse_enabled = 8;
  string bm25_text_field = 9;
  bool scalar_quantization = 10;
}
```

### `VectorPoint`

```proto
message VectorPoint {
  string id = 1;
  repeated float values = 2;
  bytes payload = 3;          // arbitrary JSON encoded as bytes
  SparseVector sparse = 4;
}
```

### `SearchRequest`

```proto
message SearchRequest {
  string collection = 1;
  repeated float query = 2;
  uint32 top_k = 3;
  repeated string filter_ids = 4;  // hard restrict to these ids
  string filter_json = 5;          // Filter DSL JSON
  SparseVector sparse_query = 6;
  string text_query = 7;
  string search_mode = 8;          // dense | sparse | bm25 | hybrid_rrf | hybrid_weighted
  float hybrid_alpha = 9;
}
```

### `BulkUpsertRequest`

```proto
message BulkUpsertRequest {
  string collection = 1;
  repeated VectorPoint points = 2;
  uint32 chunk_size = 3;     // 0 → default 500
}
```

### `ImportChunk` (client-streaming)

```proto
message ImportChunk {
  string collection = 1;
  repeated VectorPoint points = 2;
  bool finalize = 3;
}
```

Stream rules:
- The first chunk should set `collection`; later chunks may omit it.
- The server flushes when buffered ≥ 500 points or `finalize == true`.
- The response is a single `ImportStreamResponse{upserted}` after the stream ends.

---

## Examples

### Rust SDK

```rust
use vectordb_client::VectorDbClient;
use vectordb_proto::vectordb::v1::{VectorPoint, CollectionSpec, DistanceMetric};

let mut c = VectorDbClient::connect_with(
    "http://127.0.0.1:6334".into(),
    Some("dev-secret-key-change-me".into()),
).await?;

c.create_collection(CollectionSpec {
    name: "docs".into(),
    dimension: 3,
    metric: DistanceMetric::Cosine as i32,
    m: 16, ef_construction: 200, ef_search: 64,
    ..Default::default()
}).await?;

c.upsert("docs", vec![VectorPoint{
    id: "a".into(),
    values: vec![1.0, 0.0, 0.0],
    payload: Vec::new(),
    sparse: Default::default(),
}]).await?;

let hits = c.search("docs", vec![1.0, 0.0, 0.0], 5).await?;
```

### `grpcurl`

```bash
grpcurl -plaintext -d '{"collection":"docs","query":[1,0,0],"top_k":5}' \
  127.0.0.1:6334 vectordb.v1.VectorService/Search
```

If the schema isn’t in reflection, point grpcurl at the proto file:

```bash
grpcurl -plaintext \
  -import-path crates/vectordb-proto/proto \
  -proto vectordb.proto \
  -d '{}' 127.0.0.1:6334 vectordb.v1.VectorService/ListCollections
```

---

## Generating bindings in other languages

The proto files are stable. To generate bindings yourself:

```bash
# Python (grpcio-tools)
python -m grpc_tools.protoc -I crates/vectordb-proto/proto \
  --python_out=. --grpc_python_out=. \
  crates/vectordb-proto/proto/vectordb.proto

# Go
protoc -I crates/vectordb-proto/proto \
  --go_out=. --go-grpc_out=. \
  crates/vectordb-proto/proto/vectordb.proto
```

Most users should prefer the official REST SDKs (Python, Node, Go, Java) which
target the gateway and don’t require codegen — see
[`guides/sdks.md`](../guides/sdks.md).

---

## Status mapping

| gRPC status | Cause | REST equivalent |
|-------------|-------|-----------------|
| `OK` | success | `200 / 204` |
| `INVALID_ARGUMENT` | bad request, dimension mismatch | `400` |
| `UNAUTHENTICATED` | missing / wrong API key | `401` |
| `NOT_FOUND` | unknown collection / id | `404` |
| `ALREADY_EXISTS` | duplicate collection | `409` |
| `FAILED_PRECONDITION` | wrong shard or follower for write | `412` |
| `UNAVAILABLE` | upstream connect failure | `502 / 503` |
| `INTERNAL` | engine / storage error | `500` |
