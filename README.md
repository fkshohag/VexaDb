# VectorDB

Production-oriented, horizontally scalable vector database written in Rust — inspired by [Pinecone](https://www.pinecone.io/), designed for real workloads: embeddings search, RAG, recommendations, and similarity APIs.

## Features

- **Approximate nearest neighbor (ANN)** via HNSW with configurable `M`, `ef_construction`, `ef_search`
- **Distance metrics**: cosine, Euclidean (L2²), dot product
- **Collections** (namespaces) with dimension + metric validation
- **Durability**: write-ahead log (WAL) + RocksDB collection metadata
- **gRPC API** (Tonic) — language-agnostic clients
- **Horizontal scaling hooks**: consistent-hash sharding, per-node shard ownership, multi-node Docker layout
- **Rust SDK** (`vectordb-client`) and **CLI** (`vectordb`)

## Architecture

```
┌─────────────┐     gRPC      ┌──────────────────┐
│   Client    │ ────────────► │  vectordb-server │
│  (any lang) │               │  ┌────────────┐  │
└─────────────┘               │  │ HNSW index │  │
                              │  │ WAL + meta │  │
                              │  │ Shard gate │  │
                              └──────────────────┘
```

| Crate | Role |
|-------|------|
| `vectordb-core` | Vectors, metrics, HNSW |
| `vectordb-storage` | WAL, mmap segments, collection engine |
| `vectordb-cluster` | Consistent hash ring, shard router, membership |
| `vectordb-proto` | Protobuf / gRPC definitions |
| `vectordb-router` | Query router (fan-out + merge) |
| `vectordb-server` | Data or router node binary |
| `vectordb-gateway` | HTTP/JSON REST gateway |
| `vectordb-client` | Rust SDK |
| `vectordb-cli` | Admin CLI |

## Quick start

```bash
# Build
cargo build --release

# Run server (default :6334, data in ./data)
cargo run -p vectordb-server --release

# Create collection + upsert + search
cargo run -p vectordb-cli -- collections create embeddings --dim 3
cargo run -p vectordb-cli -- upsert embeddings doc-1 "0.1,0.2,0.3"
cargo run -p vectordb-cli -- search embeddings "0.1,0.2,0.3" --top-k 5
```

### Docker (2 shards + router + REST)

```bash
docker compose up --build
# gRPC router: localhost:6333
# REST gateway: http://localhost:8080
```

### Cluster (router + 2 data nodes)

```bash
# Terminal 1–2: data shards
cargo run -p vectordb-server -- --config config/node-0.toml
cargo run -p vectordb-server -- --config config/node-1.toml

# Terminal 3: router (single client endpoint)
cargo run -p vectordb-server -- --config config/router.toml

# Terminal 4: REST gateway → router
VECTORDB_GRPC=http://127.0.0.1:6333 cargo run -p vectordb-gateway
```

```bash
# REST example
curl -s http://127.0.0.1:8080/health
curl -s -X POST http://127.0.0.1:8080/v1/collections \
  -H 'Content-Type: application/json' \
  -d '{"name":"docs","dimension":3}'
curl -s -X POST http://127.0.0.1:8080/v1/collections/docs/upsert \
  -H 'Content-Type: application/json' \
  -d '{"points":[{"id":"a","values":[1,0,0]}]}'
curl -s -X POST http://127.0.0.1:8080/v1/collections/docs/search \
  -H 'Content-Type: application/json' \
  -d '{"vector":[1,0,0],"top_k":5}'
```

## Configuration

See [`config/example.toml`](config/example.toml). Or use flags / env:

| Env | Description |
|-----|-------------|
| `VECTORDB_CONFIG` | Path to TOML config |
| `VECTORDB_LISTEN` | gRPC bind address |
| `VECTORDB_DATA_DIR` | Persistent data directory |
| `VECTORDB_NODE_ID` | Unique node identifier |

## Horizontal scaling

1. Run data nodes with distinct `shard_id` and shared `shard_count` (`config/node-0.toml`, `node-1.toml`, …).
2. Run a **router** (`role = "router"`) with `[[cluster.nodes]]` listing each shard's gRPC address (`config/router.toml`).
3. Point clients at the router (`:6333`) or **gateway** (`:8080`).

**Roadmap**:

- [x] Query router with fan-out merge
- [x] HTTP/JSON REST gateway
- [ ] Raft replication (`openraft`) per shard
- [ ] Metadata filtering (JSON payload indexes)
- [ ] Quantization (PQ / scalar)

## Development

```bash
cargo test --workspace
cargo bench -p vectordb-core
```

## License

Apache-2.0
