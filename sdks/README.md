# VectorDB SDKs

Official language clients for the VectorDB **REST gateway** (`vectordb-gateway`, default `:8080`).

| Flavor | Path | Install / use | Quickstart |
|--------|------|---------------|------------|
| **curl** (cookbook) | [`curl/`](curl/) | bash + `curl` + `jq` | `./sdks/curl/quickstart.sh` |
| **Python** | [`python/`](python/) | `pip install -e sdks/python` | `python sdks/python/examples/quickstart.py` |
| **Node.js** | [`nodejs/`](nodejs/) | `npm install` + `npm run build` in `sdks/nodejs` | — |
| **Go** | [`go/`](go/) | `go get github.com/vectordb/vectordb/sdks/go` | `go run ./sdks/go/examples/quickstart` |
| **Java** | [`java/`](java/) | `mvn package` in `sdks/java` | — |
| **Rust** (gRPC) | [`../crates/vectordb-client`](../crates/vectordb-client/) | `vectordb-client` crate | — |

Python, Node, Go, and Java SDKs include **RAG helpers**: text chunking, document ingest with pluggable embeddings, multi-query fusion, and lightweight overlap reranking.

Common environment:

```bash
export VECTORDB_URL=http://127.0.0.1:8080
export VECTORDB_API_KEY=    # only when the gateway enforces auth
```
