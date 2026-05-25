# SDKs

| Language | Path | Transport | Highlights |
|----------|------|-----------|------------|
| **Rust** | [`crates/vectordb-client`](../../crates/vectordb-client/) | gRPC | leader-redirect, full proto surface |
| **Python** | [`sdks/python`](../../sdks/python/) | REST | `RagPipeline`, sync `httpx` |
| **Node.js / TS** | [`sdks/nodejs`](../../sdks/nodejs/) | REST | Native `fetch`, Node 18+ |
| **Go** | [`sdks/go`](../../sdks/go/) | REST | `vexaclient` + `entity` (Milvus-style options) |
| **Java** | [`sdks/java`](../../sdks/java/) | REST | Maven, Java 17+, Gson |

All REST SDKs accept the same options:

- `base_url` (default `http://127.0.0.1:8080`)
- `api_key` (sent as `x-api-key` and `Authorization: Bearer …`)

The Rust SDK uses gRPC directly and supports automatic leader redirect via
the `x-vectordb-leader` metadata.

---

## Surface parity

Every REST SDK has methods for:

- `health`, `live`, `ready`
- `list_collections`, `create_collection`, `describe_collection`, `delete_collection`
- `upsert`, `bulk_upsert`, `search`, `query`, `delete_points`, `get_point`, `stats`
- `compact_wal`, `reindex_collection`
- `create_snapshot`, `list_snapshots`, `delete_snapshot`

And RAG helpers:

- `chunk_text` / `chunkText` / `ChunkText` / `RagUtil.chunkText`
- `expand_query` / `expandQuery` / `ExpandQuery` / `RagUtil.expandQuery`
- `rerank_by_overlap` / `rerankByOverlap` / `RerankByOverlap` / `RagUtil.rerankByOverlap`
- `RagPipeline` / `rag.Pipeline` with `ingest()` and `query()`

---

## Python

```bash
pip install -e sdks/python
```

```python
from vectordb import VectorDbClient

with VectorDbClient("http://127.0.0.1:8080", api_key="dev") as c:
    c.create_collection("docs", dimension=1536, metric="cosine")
    c.upsert("docs", [{"id":"a","values":[...]}])
    print(c.search("docs", [...], top_k=5))
```

RAG: see [`rag.md`](rag.md). Tests: `pytest sdks/python/tests`.

---

## Node.js / TypeScript

```bash
cd sdks/nodejs && npm install && npm run build
```

```ts
import { VectorDbClient } from "@vectordb/client";

const c = new VectorDbClient("http://127.0.0.1:8080", { apiKey: "dev" });
await c.createCollection("docs", 1536);
await c.upsert("docs", [{ id: "a", values: [/*...*/] }]);
const hits = await c.search("docs", [/*...*/], { topK: 5 });
```

ESM only. Requires native `fetch` (Node 18+).

---

## Go

```bash
cd sdks/go && go build ./...
```

```go
import (
    "github.com/vectordb/vectordb/sdks/go/entity"
    "github.com/vectordb/vectordb/sdks/go/vexaclient"
)

cli, _ := vexaclient.New(ctx, &vexaclient.ClientConfig{Address: "http://127.0.0.1:8080"})
_ = cli.CreateCollection(ctx, vexaclient.NewSimpleCreateCollectionOption("docs", 1536))
_, _ = cli.Insert(ctx, vexaclient.NewColumnBasedInsertOption("docs").
    WithIDs([]string{"a"}).WithFloatVectorColumn("vector", 1536, [][]float32{vec}))
hits, _ := cli.Search(ctx, vexaclient.NewSearchOption("docs", 5, []entity.Vector{entity.FloatVector(query)}))
```

Tests: `go test ./...`. Demo: `go run ./examples/rag_demo`.

---

## Java

```bash
cd sdks/java && mvn -q package
```

```java
VectorDbClient c = new VectorDbClient("http://127.0.0.1:8080", System.getenv("VECTORDB_API_KEY"));
c.createCollection("docs", 1536, "cosine", null, false, "text", false);
c.upsert("docs", List.of(Map.of("id","a","values", List.of(0.1, 0.2, 0.3))));
var hits = c.search("docs", List.of(0.1, 0.2, 0.3), 5, null, null, "dense", 0.5);
```

Tests: `mvn test`. Demo: `mvn -q exec:java -Dexec.mainClass=dev.vectordb.RagDemo`.

---

## Rust (gRPC)

```toml
[dependencies]
vectordb-client = { path = "crates/vectordb-client" }
vectordb-proto  = { path = "crates/vectordb-proto" }
tokio           = { version = "1", features = ["full"] }
```

```rust
use vectordb_client::VectorDbClient;
use vectordb_proto::vectordb::v1::{VectorPoint, CollectionSpec, DistanceMetric};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut c = VectorDbClient::connect_with(
        "http://127.0.0.1:6334".into(),
        Some("dev".into()),
    ).await?;

    c.create_collection(CollectionSpec {
        name: "docs".into(), dimension: 3,
        metric: DistanceMetric::Cosine as i32,
        m: 16, ef_construction: 200, ef_search: 64,
        ..Default::default()
    }).await?;

    c.upsert("docs", vec![VectorPoint {
        id: "a".into(), values: vec![1.0, 0.0, 0.0],
        ..Default::default()
    }]).await?;

    let hits = c.search("docs", vec![1.0, 0.0, 0.0], 5).await?;
    println!("{:?}", hits);
    Ok(())
}
```

The Rust SDK auto-redirects writes to the Raft leader.

---

## Picking an SDK

- **App in production?** Use the language SDK that matches your service.
- **Embedded / co-located?** Use Rust gRPC for the lowest latency.
- **Notebook / data pipeline?** Python is the most ergonomic.
- **Cross-cutting CLI tooling?** Rust CLI (`vectordb`) ships in this repo.

For the full feature checklist see [`../features.md`](../features.md).
