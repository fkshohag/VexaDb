# VectorDB Documentation

Welcome to **VectorDB** — a production-oriented, horizontally scalable vector
database written in Rust, inspired by Pinecone, Qdrant, and Weaviate.

This index links the full documentation set. Start at
[`getting-started.md`](getting-started.md) if you’re new.

---

## Contents

### Overview
- [Getting started](getting-started.md) — install, first server, first query
- [Architecture & design](architecture.md) — components, write path, read path, sharding, Raft
- [Feature roadmap](features.md) — milestone status (M1–M5 + future)
- [Benchmarks](benchmarks.md) — `vectordb-bench` and Criterion suites

### API reference
- [REST API](api/rest.md) — HTTP/JSON gateway endpoints
- [gRPC API](api/grpc.md) — protobuf service surface
- [Filter DSL](api/filter-dsl.md) — JSON metadata filtering language

### Guides
- [Configuration](guides/configuration.md) — TOML files, env vars, CLI flags
- [Clustering & replication](guides/clustering.md) — sharding, Raft 3-node quorum
- [Security](guides/security.md) — API keys, TLS, hardening
- [Observability](guides/observability.md) — Prometheus, health probes, logs
- [Operations](guides/operations.md) — snapshots, WAL compaction, reindex, backup/restore
- [Load testing & large data](guides/load-testing.md) — `vectordb-bench`, live `load_test.py`, scaling tips
- [RAG demo: system-design corpus](guides/rag-system-design-demo.md) — runnable end-to-end RAG with real content
- [Hybrid search](guides/hybrid-search.md) — dense + sparse + BM25 fusion
- [RAG with VectorDB](guides/rag.md) — chunking, ingestion, retrieval recipe
- [SDKs](guides/sdks.md) — Python, Node, Go, Java, Rust

### Project
- [`../README.md`](../README.md) — high-level overview
- [`../sdks/README.md`](../sdks/README.md) — SDK index

---

## Quick links

| What | Where |
|------|-------|
| **HTTP gateway** | `http://127.0.0.1:8080` (default) |
| **gRPC server** | `127.0.0.1:6334` (default) |
| **Prometheus** | `http://127.0.0.1:9090/metrics` |
| **Health/live/ready** | `GET /health`, `/live`, `/ready` |
| **Example configs** | [`../config/`](../config/) |
| **Diagrams** | [`architecture.md`](architecture.md) |

## License

Apache-2.0
