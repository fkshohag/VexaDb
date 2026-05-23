# VectorDB SDKs

Official language clients for the VectorDB **REST gateway** (`vectordb-gateway`, default `:8080`).

| SDK | Path | Install |
|-----|------|---------|
| **Python** | [`python/`](python/) | `pip install -e sdks/python` |
| **Node.js** | [`nodejs/`](nodejs/) | `npm install` + `npm run build` in `sdks/nodejs` |
| **Go** | [`go/`](go/) | `go get github.com/vectordb/vectordb/sdks/go` |
| **Java** | [`java/`](java/) | `mvn package` in `sdks/java` |
| **Rust** | [`../crates/vectordb-client`](../crates/vectordb-client/) | `vectordb-client` crate (gRPC) |

Python, Node, Go, and Java SDKs include **RAG helpers**: text chunking, document ingest with pluggable embeddings, multi-query fusion, and lightweight overlap reranking.

Set `VECTORDB_API_KEY` when the gateway enforces auth (`VECTORDB_API_KEYS`).
