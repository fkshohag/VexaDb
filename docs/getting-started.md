# Getting started

Run a single-node VectorDB in under a minute, then upsert your first vectors
and run a search. After this, see [`architecture.md`](architecture.md) for the
internals or [`guides/clustering.md`](guides/clustering.md) for sharded /
replicated deployments.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Rust | 1.87+ | Build the workspace |
| `cmake` + C++ toolchain | any recent | RocksDB native build |
| `protoc` | bundled via `protoc-bin-vendored` | gRPC codegen (no install needed) |
| Docker (optional) | 24+ | Run via `docker compose` |

## 1. Build & run

```bash
git clone <your-fork> vectordb
cd vectordb
cargo build --release

# Single all-in-one node on :6334, REST gateway on :8080
cargo run -p vectordb-server --release &
VECTORDB_GRPC=http://127.0.0.1:6334 \
  cargo run -p vectordb-gateway --release &
```

Health checks:

```bash
curl -s http://127.0.0.1:8080/health
curl -s http://127.0.0.1:8080/live
curl -s http://127.0.0.1:8080/ready
```

## 2. First collection + first vector

```bash
# Create a 3-dim cosine collection
curl -s -X POST http://127.0.0.1:8080/v1/collections \
  -H 'Content-Type: application/json' \
  -d '{"name":"hello","dimension":3,"metric":"cosine"}'

# Upsert a vector
curl -s -X POST http://127.0.0.1:8080/v1/collections/hello/upsert \
  -H 'Content-Type: application/json' \
  -d '{"points":[{"id":"a","values":[1,0,0],"payload":{"text":"hi"}}]}'

# Search
curl -s -X POST http://127.0.0.1:8080/v1/collections/hello/search \
  -H 'Content-Type: application/json' \
  -d '{"vector":[1,0,0],"top_k":5}'
```

## 3. Try the CLI

```bash
cargo run -p vectordb-cli -- collections create embeddings --dim 3
cargo run -p vectordb-cli -- upsert embeddings doc-1 "0.1,0.2,0.3"
cargo run -p vectordb-cli -- search embeddings "0.1,0.2,0.3" --top-k 5
```

## 4. Try a SDK (Python)

```bash
pip install -e sdks/python
```

```python
from vectordb import VectorDbClient

with VectorDbClient("http://127.0.0.1:8080") as c:
    c.create_collection("docs", dimension=3)
    c.upsert("docs", [{"id":"a","values":[1,0,0]}])
    print(c.search("docs", [1,0,0], top_k=5))
```

Other languages: see [`guides/sdks.md`](guides/sdks.md).

## 5. Try Docker

```bash
# macOS external volume? clean AppleDouble files first
./scripts/clean-macos-artifacts.sh

docker compose up --build
# REST gateway: http://localhost:8080
# gRPC router:  localhost:6333
```

## 6. Run benchmarks

```bash
./scripts/benchmark.sh
# or
cargo run -p vectordb-bench --release -- --count 5000 --queries 500
```

See [`benchmarks.md`](benchmarks.md).

## What's next?

- [Architecture & design](architecture.md)
- [REST API reference](api/rest.md)
- [Configuration](guides/configuration.md)
- [Clustering & replication](guides/clustering.md)
- [Hybrid search (BM25 + sparse + dense)](guides/hybrid-search.md)
- [RAG pipeline](guides/rag.md)
