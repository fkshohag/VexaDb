# Testing with large data

Three layers of testing, in order of size and realism:

| Layer | Tool | Scope | Typical scale |
|-------|------|-------|---------------|
| **L1 — micro** | `cargo bench` (Criterion) | Single function | 1k–10k |
| **L2 — in-process** | `vectordb-bench` | Whole engine, no network | 10k–10M |
| **L3 — end-to-end** | `scripts/load_test.py` | Live gateway over HTTP | 10k–100M+ |

Always build with `--release`. Debug builds are 10–50× slower.

---

## L2: in-process scenarios (`vectordb-bench`)

The Rust harness has presets for typical sizes:

```bash
./scripts/large_data_test.sh small            # 10k vectors,  dim 128
./scripts/large_data_test.sh medium           # 100k vectors, dim 384
./scripts/large_data_test.sh large            # 500k vectors, dim 768  (default)
./scripts/large_data_test.sh xlarge           # 2M vectors,   dim 768  (no_sync_wal)

# With recall@K vs brute-force ground truth
./scripts/large_data_test.sh medium recall
```

Custom run:

```bash
cargo run -p vectordb-bench --release -- \
  --dim 768 \
  --count 1000000 \
  --queries 500 \
  --workers 8 \
  --top-k 10 \
  --measure-recall \
  --no-sync-wal
```

Sample output:

```
## Engine (WAL + RocksDB + HNSW, dim=768, n=1000000, sync_wal=false)
    [engine] inserted 200000/1000000 (38400/s)
    [engine] inserted 400000/1000000 (39800/s)
    ...
  bulk_upsert: 41200 vectors/s (24.27s total)
  [engine] search k=10 workers=8: 7400 QPS (wall 0.07s)
  search latency: p50=0.85ms p95=1.20ms p99=1.65ms avg=0.93ms
  recall@10 (vs brute-force on 50000 truth points, 100 queries): 96.2% (4.81s)
  data dir on disk: 4.21 GiB
```

### Flags worth knowing

| Flag | Effect |
|------|--------|
| `--scenario small\|medium\|large\|xlarge` | Quick presets |
| `--workers N` | Concurrent search threads (uses Rayon) |
| `--no-sync-wal` | Faster ingest at the cost of fsync per write |
| `--measure-recall` | Compares ANN top-K against exact brute-force on a sample |
| `--recall-queries N` | How many queries to grade (default 100) |
| `--core-only` | In-memory HNSW only (skip WAL/RocksDB) |
| `--skip-core` | Skip in-memory; benchmark durable engine only |
| `--progress-every N` | Progress lines while inserting (default 10000) |

### Memory & disk

The harness prints an estimate before insert:

```
estimated index size: ~3.34 GiB
```

That’s `n × (dim · 4 + 16·2·8 + 64)` — close enough to plan RAM. After
ingest with `--count ≥ 50_000` it also reports the on-disk size of the data
directory (WAL + RocksDB).

### Tips

- For ≥1M vectors at dim ≥768 use `--no-sync-wal` and snapshot+compact later.
- Recall depends strongly on `ef_search`. The bench uses defaults
  (`m=16, ef_construction=200, ef_search=64`). Edit
  `crates/vectordb-bench/src/main.rs:bench_engine` to override.

---

## L3: live gateway end-to-end (`scripts/load_test.py`)

Tests the full stack — REST → gateway → router/server → engine. This is the
realistic number: it includes JSON serialisation, HTTP/1.1, auth, and any
cluster fan-out you’ve configured.

### 1. Stand up a server

Single all-in-one node:

```bash
cargo run -p vectordb-server --release &
VECTORDB_GRPC=http://127.0.0.1:6334 cargo run -p vectordb-gateway --release &
```

Or a sharded cluster (see [`clustering.md`](clustering.md)):

```bash
cargo run -p vectordb-server -- --config config/node-0.toml &
cargo run -p vectordb-server -- --config config/node-1.toml &
cargo run -p vectordb-server -- --config config/router.toml &
VECTORDB_GRPC=http://127.0.0.1:6333 cargo run -p vectordb-gateway --release &
```

### 2. Run the load test

```bash
pip install httpx   # or: pip install -e sdks/python

python scripts/load_test.py \
  --base http://127.0.0.1:8080 \
  --collection bench \
  --dim 128 \
  --count 100000 \
  --batch-size 2000 \
  --chunk-size 500 \
  --queries 5000 \
  --workers 16 \
  --top-k 10 \
  --reset
```

Sample output:

```
VectorDB load test — base=http://127.0.0.1:8080 collection=bench
config: dim=128 count=100000 batch=2000 chunk=500 queries=5000 workers=16 top_k=10

## Insert
    inserted    20,000/   100,000  (   8,200/s, eta  9.8s)
    inserted    50,000/   100,000  (   9,100/s, eta  5.5s)
    inserted   100,000/   100,000  (   9,200/s, eta  0.0s)
  insert: 9,200 vectors/s (10.87s total, ~48.83 MiB of f32)

## Search
  search k=10 workers=16: 18,400 QPS (wall 0.27s)
  latency: p50=0.78ms p95=1.42ms p99=2.10ms avg=0.84ms

  /health: {"status": "ok"}
  gateway counter: vectordb_http_requests_total 105003
```

### Flag reference

| Flag | Default | Notes |
|------|---------|-------|
| `--base` | `http://127.0.0.1:8080` | Gateway URL |
| `--api-key` | env `VECTORDB_API_KEY` | When auth is enabled |
| `--dim` | 128 | Embedding dimension |
| `--count` | 100,000 | Total vectors inserted |
| `--batch-size` | 2,000 | Vectors per HTTP request |
| `--chunk-size` | 500 | Server-side WAL record chunk |
| `--queries` | 2,000 | Concurrent queries issued |
| `--workers` | 16 | Threads for parallel search |
| `--top-k` | 10 | Neighbors per query |
| `--reset` | — | Drop the collection first |
| `--skip-insert` | — | Reuse data, only run search workload |
| `--with-payload` | — | Attach a small JSON payload per point |

### Iterating on parameters

To find your machine’s peak QPS:

```bash
for w in 1 4 8 16 32 64; do
  python scripts/load_test.py --skip-insert --queries 5000 --workers "$w" --reset=false
done
```

To find the insert ceiling, vary `--batch-size` (HTTP) and `--chunk-size` (WAL):

```bash
for b in 500 2000 5000 10000; do
  python scripts/load_test.py --reset --count 200000 --batch-size "$b" --chunk-size 500
done
```

---

## Hitting truly large scale

| Goal | Approach |
|------|----------|
| **10M+ on one node** | Use `xlarge` scenario, `--no-sync-wal`, snapshot + WAL compact periodically |
| **100M across nodes** | Shard horizontally (`shard_count = N`), run gateway/router and load test against the gateway. The router fans out automatically. |
| **Sustained QPS** | Run multiple gateways behind a load balancer; multiple routers in parallel |
| **Real embeddings** | Replace `random_unit_vector` with a real model (OpenAI, sentence-transformers) — see RAG guide |
| **Recall on real data** | Compute brute-force ground truth on a sample with the same model |

Sharded run example (2 shards, dim 384, 1M vectors total):

```bash
# 1. Start 2 data nodes + router + gateway (see config/node-0.toml etc.)
# 2. Load test
python scripts/load_test.py --base http://127.0.0.1:8080 \
  --collection big --dim 384 --count 1000000 \
  --batch-size 5000 --chunk-size 1000 --queries 5000 --workers 32 --reset
```

The router hashes `point.id` to a shard and parallelises both the upsert and
the search fan-out.

---

## Profiling beyond throughput

While a load test is running, watch:

- **Prometheus**: `http://127.0.0.1:9090/metrics`
  ```promql
  histogram_quantile(0.95, sum by (le) (rate(vectordb_rpc_duration_seconds_bucket{rpc="search"}[1m])))
  rate(vectordb_rpc_total[1m])
  ```
- **System**: `htop`, `iostat -x 1`, `du -sh data/` for disk growth
- **Server logs**: structured JSON; grep for `query completed` and slow paths

For CPU profiling:

```bash
cargo install flamegraph
sudo cargo flamegraph -p vectordb-server --release
```

(Then start the load test; stop the server to finalise the SVG.)

---

## Reproducible benchmarks

When sharing numbers, include:

- Hardware: CPU model, RAM, storage type
- VectorDB commit hash + build flags (`cargo build --release`)
- Scenario: dim, count, top-k, workers, sync_wal, payload size
- Whether you measured recall

Pin the seed (`--seed`) on the load test and pick a Criterion `--sample-size`
that matches.

---

## Troubleshooting

| Symptom | Likely cause |
|---------|-------------|
| Insert tops out around 5k/s on `--count > 100k` | `sync_wal = true` (default). Try `--no-sync-wal` for testing. |
| QPS plateaus at small `--workers` | Single HTTP connection bottleneck; try more workers or run multiple gateway processes |
| `412 Precondition Failed` | Wrong shard or follower. Use the gateway/router URL, not a follower directly. |
| `503 Service Unavailable` | Raft follower hasn’t caught up. Wait or hit the leader. |
| Recall < 90% | Increase `ef_search` per collection or run reindex with higher `ef_construction`. |
| Disk growing fast | Run WAL compaction: `POST /v1/admin/compact-wal {"snapshot_first":true}` |
