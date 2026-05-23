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
| `vectordb-server` | Node binary |
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

### Docker

```bash
docker compose up --build
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

1. Run multiple `vectordb-server` instances with distinct `shard_id` and shared `shard_count`.
2. Point clients at the node that owns a point's shard (consistent hash on point ID), or add a router layer (roadmap).
3. Increase `virtual_nodes_per_shard` in cluster config for even distribution.

**Roadmap** (contributions welcome):

- [ ] Query router / proxy with fan-out merge
- [ ] Raft replication (`openraft`) per shard
- [ ] HTTP/JSON REST gateway
- [ ] Metadata filtering (JSON payload indexes)
- [ ] Quantization (PQ / scalar)

## Development

```bash
cargo test --workspace
cargo bench -p vectordb-core
```

## License

Apache-2.0
