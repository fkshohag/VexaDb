# RAG demo: real system-design content

A complete, runnable example of retrieval-augmented retrieval over a curated
corpus of 12 system-design articles. Demonstrates ingestion, dense search,
hybrid (dense + BM25) search, multi-query expansion, and overlap reranking
end-to-end through the live REST gateway.

## What's in the corpus

`data/system-design/` ships 12 dense markdown docs covering the canonical
interview-and-architecture surface area:

| File | Topic |
|------|-------|
| `01-load-balancing.md` | L4/L7, algorithms, health checks, sticky sessions |
| `02-caching.md` | cache-aside, write-through/back/around, eviction, invalidation |
| `03-database-sharding.md` | range/hash/consistent-hash, shard keys, resharding |
| `04-replication-consistency.md` | leader/follower, CAP, PACELC, consistency models |
| `05-consensus-raft.md` | Paxos vs Raft, leader election, log replication, safety |
| `06-message-queues.md` | queue vs log, delivery semantics, partitions, DLQs |
| `07-microservices.md` | bounded contexts, sagas, outbox, service mesh |
| `08-rate-limiting.md` | token bucket, sliding window, distributed counters |
| `09-api-gateway.md` | gateway vs proxy vs mesh, BFF, anti-patterns |
| `10-observability.md` | three pillars, RED/USE, SLOs, error budgets, sampling |
| `11-database-indexes.md` | B-tree, LSM, hash, bitmap, covering indexes |
| `12-vector-databases.md` | HNSW, IVF, PQ, distance metrics, hybrid search |

Each document is 400–600 words of accurate technical prose, intended to be
genuinely useful as RAG context, not filler.

## Run it

### 1. Start the database

```bash
cargo run -p vectordb-server --release &
VECTORDB_GRPC=http://127.0.0.1:6334 cargo run -p vectordb-gateway --release &
```

### 2. Install the Python deps

On macOS, use `python3` and `pip3` (there is often no `python` / `pip` on PATH):

```bash
cd /path/to/vectordb

# Recommended: project virtualenv (avoids PEP 668 / system Python issues)
python3 -m venv .venv
source .venv/bin/activate
pip install httpx                          # minimum for the demo script
pip install -e sdks/python               # optional: in-repo client + RagPipeline
pip install sentence-transformers        # optional: real embeddings (~80MB)
```

If you can't or don't want to install `sentence-transformers`, the demo
falls back to a deterministic hashing-BoW embedder. Quality is much lower
but everything runs.

### 3. Run the demo

```bash
# With venv activated (see above):
python scripts/rag_system_design.py

# Or without activating — call the venv interpreter directly:
.venv/bin/python scripts/rag_system_design.py --embedder hash --reset --hybrid
```

More examples:

```bash
# Force the hash fallback (no ML deps)
python scripts/rag_system_design.py --embedder hash

# Hybrid (dense + BM25) — best quality on this corpus
python scripts/rag_system_design.py --hybrid --reset

# Ask a single custom question
python scripts/rag_system_design.py --query "How do I prevent thundering herds on cache miss?"

# Re-query an already-ingested collection (no re-embedding)
python scripts/rag_system_design.py --skip-ingest --hybrid

# Multi-query expansion with overlap rerank
python scripts/rag_system_design.py --multi-query --rerank
```

If `docker compose up` is already running, the gateway is usually at
`http://127.0.0.1:8080` — no need to start `cargo run` separately.

## Sample output

```
=== VectorDB system-design RAG demo ===
  gateway:    http://127.0.0.1:8080
  collection: sysdesign
  corpus:     /Volumes/.../data/system-design
  loading sentence-transformers model: sentence-transformers/all-MiniLM-L6-v2
  model loaded in 1.8s, dim=384
  loaded 12 documents from corpus

## Ingest
  upserted 67 chunks in 4.32s (15 chunks/s)

## Retrieval — mode=dense, top_k=4, multi_query=False, rerank=False

>>> How do I scale a relational database when a single server isn't enough?
  [1] score=0.7421  doc=03-database-sharding#0
      title: Database sharding
      text : Sharding (a.k.a. horizontal partitioning) splits a single logical dataset across many physical nodes…
  [2] score=0.6184  doc=03-database-sharding#3
      title: Database sharding
      text : Resharding is hard. Approaches: Double writes + backfill — write to old and new shards…
  ...

>>> What is the CAP theorem and how does it shape replication design?
  [1] score=0.8019  doc=04-replication-consistency#2
      title: Replication and consistency
      text : CAP says that under a Partition you must choose between Consistency and Availability. PACELC…
  ...

Average retrieval latency: 8.3 ms across 12 queries
```

## What the script demonstrates

The script exercises the full RAG path against the live gateway:

1. **Chunking** — markdown files are split with overlap (`chunk_size=600`,
   `overlap=120`) using the SDK's `RagPipeline.ingest`.
2. **Embedding** — `sentence-transformers` (default) or a hash fallback.
   The dimension is read from the model and used to create the collection.
3. **Bulk upsert** — chunks are pushed via `POST /v1/collections/.../bulk`
   with payload metadata (title, source path, topic).
4. **Dense search** — `POST /v1/collections/.../search` with the query
   embedding.
5. **Hybrid search** (`--hybrid`) — dense + BM25 fusion via the gateway's
   `text_query` + `search_mode=hybrid` + `hybrid_alpha`.
6. **Multi-query** (`--multi-query`) — generates variants of the query and
   merges results with reciprocal rank fusion.
7. **Rerank** (`--rerank`) — token-overlap rerank after retrieval.

## Tuning recall

Defaults aim for a good demo, not for peak quality. Things to tune:

| Knob | Effect |
|------|--------|
| `--chunk-size` (default 600) | Smaller = more precise; larger = more context |
| `--overlap` (default 120) | Larger = better cross-chunk recall; more storage |
| `--top-k` (default 4) | More context per answer; higher token cost |
| `--hybrid` | Adds keyword recall; useful for proper-noun queries |
| `--hybrid-alpha` (default 0.6) | 0 = pure BM25, 1 = pure dense |
| `--multi-query` | More queries per question; better tail recall |
| `--rerank` | Promotes chunks that share keywords with the query |
| `--model` | Try `BAAI/bge-small-en-v1.5` or `intfloat/e5-small-v2` |

## Adding your own content

Drop more `.md` files into `data/system-design/` (or pass `--corpus`
pointing at any directory of markdown files), then re-run with `--reset`:

```bash
cp my-notes/*.md data/system-design/
python scripts/rag_system_design.py --reset --hybrid
```

Each file becomes one document; the first `# heading` is captured as the
title in the payload.

## What this *doesn't* do

The demo stops at retrieval — it prints the top-K chunks but does not call
an LLM to generate an answer. Plug retrieval into your favourite LLM:

```python
from openai import OpenAI

llm = OpenAI()
hits = pipeline.query("What is CAP?", top_k=4)
context = "\n\n---\n\n".join(h.text for h in hits)
resp = llm.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "Answer using the provided context only."},
        {"role": "user", "content": f"Context:\n{context}\n\nQuestion: What is CAP?"},
    ],
)
print(resp.choices[0].message.content)
```

That's it — VectorDB is the retrieval substrate; the LLM and orchestration
layer are yours.
