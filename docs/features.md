# VectorDB feature roadmap

Status legend: **[x]** done · **[~]** partial · **[ ]** planned · **[L]** large / future track

This file is the canonical checklist. The user-supplied wish-list (Pinecone +
Qdrant + Weaviate combined) is preserved below, annotated with the current
implementation status. See [`README.md`](../README.md) for usage and the
roadmap section for the upcoming milestones (M2…M5).

---

## Core Vector Features

- [x] Dense vector storage
- [x] Sparse vector storage *(M3)*
- [x] Hybrid vector search (BM25 + dense fusion) *(M3)*
- [x] Similarity search
- [x] Cosine similarity
- [x] Dot product search
- [x] Euclidean distance search
- [x] Top-K nearest neighbor search
- [x] Approximate nearest neighbor (ANN) search

## Indexing Features

- [x] HNSW indexing
- [ ] IVF indexing *(M3)*
- [~] Product quantization (PQ) *(scalar 8-bit done; full PQ planned)*
- [x] Vector compression *(M3 — scalar quantization)*
- [x] Real-time indexing
- [x] Incremental indexing
- [x] Batch indexing *(via Upsert with N points)*
- [x] Dynamic index updates

## Data Operations

- [x] Insert vectors
- [x] Upsert vectors
- [x] Update vectors *(via upsert)*
- [x] Delete vectors
- [x] Fetch by ID
- [x] Batch operations
- [x] Bulk import *(M4)*
- [x] Streaming ingestion *(M4 — gRPC `ImportStream`)*

## Search Features

- [x] Semantic search
- [x] Hybrid BM25 + vector search *(M3)*
- [x] Metadata filtering *(M1: Filter DSL)*
- [x] Boolean filtering *(`must` / `must_not` / `should`)*
- [x] Range filtering *(numeric `gte`/`lte`/`gt`/`lt`)*
- [x] Namespace filtering *(collections + match-on-field)*
- [x] Multi-condition filtering
- [x] Similarity scoring
- [ ] Re-ranking support *(M3)*

## Metadata Features

- [x] JSON metadata storage
- [x] Structured metadata
- [x] Metadata indexing *(keyword / numeric / bool indexes)*
- [x] Filterable attributes
- [x] Tag-based filtering *(arrays + `any_of`)*

## Multi-Tenant Features

- [x] Namespaces *(collections)*
- [~] Tenant isolation *(at process level; M2 will add API-key scoping)*
- [x] Shared indexes
- [x] Dedicated indexes *(per collection)*
- [x] Logical data separation

## Scalability Features

- [x] Horizontal scaling
- [x] Automatic sharding *(consistent hashing)*
- [x] Distributed indexing
- [x] Distributed query execution *(router fan-out + merge top-k)*
- [ ] Auto scaling *(operator/control-plane track, [L])*
- [x] High QPS support *(benchmark harness + `docs/benchmarks.md`)*
- [~] Billion-scale vector support *(architecture supports it; needs M3 quant + segment paging)*

## Availability Features

- [x] Replication *(Raft per shard)*
- [x] Failover *(Raft leader election)*
- [x] High availability *(3-node quorum)*
- [~] Read replicas *(any follower can serve reads today; M2 will add explicit read-replica routing)*
- [ ] Multi-zone deployment *([L])*
- [ ] Disaster recovery *(M1 snapshots are the building block; M4 adds offsite backup)*

## Storage Features

- [x] In-memory indexes
- [~] SSD-backed storage *(WAL + RocksDB are SSD-friendly; mmap segments planned)*
- [ ] Tiered storage *([L])*
- [x] Persistent storage
- [x] Snapshotting *(M1)*
- [x] Backups *(M1: snapshot copy)*
- [x] Recovery support *(WAL replay + snapshot + WAL compaction M4)*
- [x] WAL (write-ahead logging)

## Performance Features

- [x] Low-latency search *(HNSW)*
- [~] Query optimization *(filter pushdown via payload indexes done in M1)*
- [x] Parallel query execution *(per shard)*
- [x] Query routing
- [x] Smart shard routing
- [ ] Cache optimization *(M3)*
- [x] SIMD acceleration *(M3 — f32x4 dot / L2)*
- [ ] GPU acceleration support *([L])*

## AI & RAG Features

- [x] RAG optimization *(M5 — `RagPipeline` in Python/Node SDKs)*
- [x] LLM retrieval support *(via REST/gRPC)*
- [x] AI memory storage
- [x] Conversational memory *(use a collection)*
- [x] Context retrieval
- [x] Chunk retrieval
- [x] Semantic chunk matching
- [ ] Multi-modal embeddings *(any-dimension vectors supported; tooling = M5)*

## Embedding Features

- [x] OpenAI embedding compatibility *(any 1536-d cosine collection)*
- [x] Cohere embedding compatibility
- [x] Sentence-transformer compatibility
- [x] Custom embedding support
- [x] Multi-model embedding support *(separate collections)*

## Security Features

- [x] API keys *(M2)*
- [x] Authentication *(M2 — Bearer / x-api-key)*
- [~] Authorization *(M2 — key gate only; RBAC later)*
- [ ] Encryption at rest *([L] — rely on disk encryption for now)*
- [x] Encryption in transit *(M2 — optional TLS on gRPC + HTTP)*
- [ ] VPC networking *([L])*
- [ ] IAM integration *([L])*
- [ ] Tenant security isolation *(M2)*

## Cloud Features

- [ ] Serverless indexes *([L] — managed-service track)*
- [ ] Managed infrastructure *([L])*
- [ ] AWS support *(works wherever Linux + Docker run; managed track [L])*
- [ ] GCP support *([L])*
- [ ] Azure support *([L])*
- [ ] Multi-region deployment *([L])*

## Developer Features

- [x] REST API
- [x] gRPC API
- [x] Python SDK *(M5)*
- [x] Node.js SDK *(M5)*
- [x] Java SDK *(M5+)*
- [x] Go SDK *(M5+)*
- [x] CLI tooling

## Monitoring & Observability

- [x] Query metrics *(M2 — Prometheus `/metrics`)*
- [x] Latency monitoring *(M2 — `vectordb_rpc_duration_seconds`)*
- [ ] Usage analytics *(M2)*
- [x] Health monitoring *(`/health`, `/live`, `/ready` on gateway)*
- [~] Index statistics *(`describe_collection` returns count; M2 expands)*
- [x] Logging *(JSON tracing + structured search/upsert query logs)*
- [ ] Alerting *([L])*

## Enterprise Features

- [ ] SLA support *([L])*
- [ ] Dedicated deployments *(supported by self-hosting; managed track [L])*
- [ ] Private networking *([L])*
- [ ] Compliance support *([L])*
- [ ] Enterprise scaling *(M3 + M4)*
- [ ] Custom resource allocation *([L])*

## Advanced Retrieval Features

- [x] Hybrid retrieval *(M3)*
- [x] Sparse + dense fusion *(M3)*
- [ ] Adaptive ANN tuning *(M3)*
- [x] Query expansion *(M5 — `expand_query` / multi-query retrieve)*
- [~] Semantic ranking *(server hybrid search; client overlap rerank in M5)*
- [x] Exact re-ranking *(M5 — `rerank_by_overlap` post-retrieval)*
- [~] Candidate pruning *(payload-index brute-force path landed in M1)*

## Operational Features

- [x] Index management *(CLI + REST)*
- [ ] Live scaling *(M4 — Raft membership change)*
- [ ] Replica management *(M4)*
- [x] Online reindexing *(M4)*
- [ ] Rolling upgrades *(M5)*
- [x] Background compaction *(M4 — WAL rewrite)*
- [ ] Automatic balancing *(M4)*

## Common Use Cases Supported

- [x] Semantic search
- [x] AI chatbots
- [x] RAG systems
- [x] Recommendation systems
- [x] Document search
- [x] Knowledge bases
- [x] AI agents
- [x] Personalization systems
- [x] Similarity matching
- [x] Fraud detection *(any-dim vectors; works today)*
- [x] Image retrieval *(supply CLIP/DINO embeddings)*
- [x] Audio retrieval *(supply audio embeddings)*
- [x] Video retrieval *(supply video frame embeddings)*

---

## Milestones

| Milestone | Theme | Status |
|-----------|-------|--------|
| **M1** | Metadata filtering · payload indexes · snapshots | **DONE** |
| **M2** | Auth (API keys + TLS) · Prometheus metrics · leader discovery | **DONE** |
| **M3** | Sparse vectors + hybrid search · quantization · SIMD | **DONE** |
| **M4** | Bulk import · WAL compaction · online reindexing · replica mgmt | **DONE** *(replica mgmt deferred)* |
| **M5** | Python / Node SDKs · RAG helpers | **DONE** |
| **L** | Cloud control plane · multi-region · GPU · compliance | future track |

## What landed in M5

- **`sdks/python`** — `vectordb` package: `VectorDbClient` (REST) + `RagPipeline`, `chunk_text`, `expand_query`, `rerank_by_overlap`
- **`sdks/nodejs`** — `@vectordb/client`: TypeScript client + matching RAG helpers (Node 18+ `fetch`)
- **`sdks/go`** — `vectordb` module: REST client + `RagPipeline` (Go 1.22+)
- **`sdks/java`** — `dev.vectordb:vectordb-client`: Maven library + RAG (Java 17+)
- Pluggable **`embed(text)`** hook for OpenAI, sentence-transformers, or any model
- Examples: `sdks/python/examples/rag_demo.py`, `sdks/nodejs/examples/rag-demo.mjs`, `sdks/go/examples/rag_demo`, `sdks/java` `RagDemo`

## What landed in M4

- `WalEntry::BulkUpsert` — batched durable import (default chunk 500)
- gRPC **`BulkUpsert`**, client-streaming **`ImportStream`**, **`CompactWal`**, **`ReindexCollection`**
- Engine: `bulk_upsert`, `compact_wal`, `snapshot_and_compact_wal`, `reindex_collection`
- WAL **rewrite** from in-memory state (`wal_compact::export_state_to_wal`)
- HNSW `iter_points` / `point_ids` for export and reindex
- REST: `POST /v1/collections/:name/bulk`, `POST .../reindex`, `POST /v1/admin/compact-wal`

## What landed in M3

- `SparseVector` + `SparseInvertedIndex` (dot-product retrieval)
- `Bm25Index` over a configurable JSON text field
- Hybrid fusion: **RRF** (`hybrid_rrf`) and **weighted** (`hybrid_weighted`)
- **Scalar quantization** (8-bit per dimension, online min/max)
- **SIMD** distance kernels (`wide` f32x4) in `Distance::dot` / `l2_squared`
- Search modes: `dense` | `sparse` | `bm25` | `hybrid_rrf` | `hybrid_weighted`
- Proto/API: `sparse_query`, `text_query`, `search_mode`, `hybrid_alpha` on search

## What landed in M2

- `vectordb-auth` — shared API-key validation (`Bearer` / `x-api-key`)
- gRPC interceptor on `vectordb-server` (health exempt) + Axum middleware on gateway
- Optional **TLS** for gRPC (`[tls]` in config) and HTTP (`VECTORDB_TLS_CERT` / `KEY`)
- **Prometheus** metrics: `vectordb_rpc_*` on `:9090`, `vectordb_http_requests_total` on gateway
- Structured **query logs** for search/upsert (`tracing` JSON)
- **Leader discovery**: `HealthResponse.leader_endpoint`, `ready`, `is_leader`; `x-vectordb-leader` on write errors
- Rust SDK **auto-redirect** writes to leader; gateway **`/live`** / **`/ready`** probes

## What landed in M1

- `vectordb-core::filter` — JSON filter DSL (`must` / `must_not` / `should`,
  `match`, `any_of`, `range`, `exists`, dotted JSON paths, nested filters)
- `CollectionConfig::payload_indexes` (keyword / numeric / bool) for accelerated
  filtered search
- Engine: JSON payload storage, payload-index maintenance, two-path filtered
  search (indexed brute-force when selective; HNSW oversearch + post-filter
  otherwise)
- WAL replay made idempotent for `CreateCollection`
- `vectordb-storage::snapshot::SnapshotManager` (create / list / delete)
- gRPC: new `filter_json` field, new `CreateSnapshot` / `ListSnapshots` /
  `DeleteSnapshot` RPCs, `payload_indexes` on `CollectionSpec`
- Router forwards filters and snapshots; Rust SDK + CLI + REST gateway
  expose them
- Tests: 5 unit tests for the filter DSL, integration tests for
  `filtered_search_with_payload_indexes` and `snapshot_create_and_list`
