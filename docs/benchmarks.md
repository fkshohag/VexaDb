# VectorDB benchmarks

How to measure insert throughput, search QPS, and distance-kernel cost on your machine.

> **For testing with millions of vectors, the live REST gateway, or recall@K
> measurement, see [`guides/load-testing.md`](guides/load-testing.md).** This
> page covers the micro-benchmarks; that page covers large-scale and
> end-to-end scenarios.

## Quick report (recommended)

```bash
./scripts/benchmark.sh

# Smaller / faster smoke run
cargo run -p vectordb-bench --release -- --count 2000 --queries 200

# Core HNSW only (skip WAL/RocksDB)
cargo run -p vectordb-bench --release -- --core-only
```

Example output:

```
## Core (in-memory HNSW, dim=128, n=10000)
  insert: 42000 vectors/s
  search k=10: 8500 QPS
  search latency: p50=0.11ms p95=0.18ms ...

## Engine (WAL + RocksDB + HNSW, dim=128, n=10000)
  bulk_upsert: 12000 vectors/s
  search k=10: 6200 QPS
  ...
```

### CLI flags (`vectordb-bench`)

| Flag | Default | Meaning |
|------|---------|---------|
| `--dim` | 128 | Embedding dimension |
| `--count` | 10000 | Vectors indexed |
| `--queries` | 1000 | Search iterations timed |
| `--top-k` | 10 | Neighbors per query |
| `--core-only` | off | Skip durable engine bench |

## Criterion (detailed, HTML)

```bash
cargo bench -p vectordb-core
cargo bench -p vectordb-storage
```

Open `target/criterion/<group>/report/index.html` in a browser.

### Core groups

| Group | What it measures |
|-------|------------------|
| `distance` | Cosine / L2² / dot at dims 128–1536 (SIMD) |
| `hnsw_insert` | Build index (1k / 10k vectors, dim 128) |
| `hnsw_search` | ANN search at n=1k/10k, k=1/10/100 |

### Storage groups

| Group | What it measures |
|-------|------------------|
| `engine_upsert` | Per-point upsert + WAL (500 / 2k points) |
| `engine_search` | Search on 5k-vector collection |
| `engine_bulk_upsert` | Bulk import 2k points (chunk 500) |

Full Criterion run via script:

```bash
CRITERION=1 ./scripts/benchmark.sh
```

## Interpreting results

- **Insert vectors/s** — HNSW graph construction + (for engine) WAL append and RocksDB metadata.
- **Search QPS** — Approximate nearest neighbor; scales with `n`, `ef_search`, and `k`. Lower p95 latency is better for interactive RAG.
- **Distance µs/op** — Pure SIMD kernel; should be sub-microsecond to low microseconds at dim 128.

Results vary strongly by CPU, disk (WAL fsync policy), and build profile. Always use **`--release`**.

## HTTP / gateway load (optional)

Start the stack, then use any HTTP load tool against the gateway:

```bash
cargo run -p vectordb-server --release &
VECTORDB_GRPC=http://127.0.0.1:6334 cargo run -p vectordb-gateway --release &

# Create collection + upsert once, then load-test search:
hey -n 5000 -c 32 -m POST -H 'Content-Type: application/json' \
  -d '{"vector":[1,0,0],"top_k":10}' \
  http://127.0.0.1:8080/v1/collections/bench/search
```

## Tips

- Compare before/after code changes with the same `--count` and `--queries`.
- For billion-scale claims, shard horizontally; single-node benches reflect one shard.
- Set `sync_wal = false` in engine config only for throughput experiments (less durability).
