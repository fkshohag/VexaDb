# Retrieval-augmented generation (RAG)

VectorDB SDKs ship a small, opinionated **`RagPipeline`** for the common
recipe: chunk → embed → upsert → search → (optional) rerank. You bring the
embedding model.

The exact same surface is available in **Python**, **Node.js**, **Go**, and
**Java**. Examples below use Python; see
[`sdks/`](../../sdks/) for the others.

---

## What the pipeline does

```mermaid
flowchart LR
    D[Document<br/>id + text + payload] --> C[chunk_text]
    C --> E[embed each chunk]
    E --> U[bulk_upsert]
    Q[Query] --> EQ[embed query]
    EQ --> S[search top-K]
    S --> M[multi-query RRF<br/>(optional)]
    M --> R[rerank by overlap<br/>(optional)]
    R --> H[RagHit list]
```

- **Chunking** — overlapping windows, whitespace-aware boundaries
- **Embedding** — your function (OpenAI, sentence-transformers, etc.)
- **Storage** — bulk upsert with payload `{text, document_id, chunk_index}`
- **Retrieval** — single or multi-query (variant fusion)
- **Rerank** — token-overlap boost (cheap), or skip for raw vector ranking

---

## Python quick recipe

```bash
pip install -e sdks/python
```

```python
from vectordb import VectorDbClient, RagPipeline

# 1. Plug in any embedding model
def embed(text: str) -> list[float]:
    # Example: OpenAI text-embedding-3-small (replace with your client)
    import openai
    return openai.embeddings.create(model="text-embedding-3-small", input=text).data[0].embedding

# 2. Wire the pipeline
client = VectorDbClient("http://127.0.0.1:8080", api_key="...")
rag = RagPipeline(
    client,
    collection="kb",
    dimension=1536,
    embed=embed,
    chunk_size=512,
    overlap=64,
    bm25_text_field="text",  # enables hybrid retrieval if you want it
)

# 3. Ingest
rag.ingest([
    {"id": "doc-1", "text": "Long article text...", "payload": {"author": "alice"}},
    {"id": "doc-2", "text": "Another doc..."},
])

# 4. Retrieve
hits = rag.query(
    "what is a vector database?",
    top_k=5,
    multi_query=True,   # query expansion + RRF fusion
    rerank=True,        # token-overlap rerank
)
for h in hits:
    print(f"{h.score:.3f} [{h.document_id}#{h.chunk_index}] {h.text[:80]}")
```

---

## Hybrid RAG (BM25 + dense)

Enable BM25 + sparse on the collection, then ask for hybrid search at query
time:

```python
rag = RagPipeline(client, "kb", 1536, embed,
                  bm25_text_field="text",
                  sparse_enabled=False)  # set True if you also have SPLADE

hits = rag.query(
    "vector index for embeddings",
    top_k=8,
    search_mode="hybrid_rrf",  # mix dense + BM25 via RRF
)
```

---

## Filtered RAG

Pass a Filter DSL JSON dict via `filter`:

```python
hits = rag.query(
    "production-ready vector store",
    top_k=5,
    filter={"must": [{"key": "author", "match": {"value": "alice"}}]},
)
```

See [`../api/filter-dsl.md`](../api/filter-dsl.md).

---

## Streaming or large ingest

For tens of thousands of chunks, prefer the bulk path (the SDK uses it by
default through `bulk_upsert`). The gRPC layer also exposes
client-streaming `ImportStream`; the Python SDK currently uses bulk REST,
which is enough for most workloads. For maximum throughput in Rust, use
`vectordb-client::import_stream` directly.

---

## Multi-query expansion

`multi_query=True` produces ~3 query variants (raw, lowercased, keyword-only)
and fuses results with **RRF**. This typically improves recall on noisy
queries without changing precision much.

```python
hits = rag.query("compare HNSW and IVF for embeddings",
                 top_k=10, multi_query=True)
```

---

## Rerank

`rerank=True` re-orders candidates by query-token overlap on top of the
retrieval score. Cheap and often helpful for RAG. For a stronger reranker,
score the top-N with a cross-encoder model client-side and resort.

---

## Deleting & updating a document

The SDK names chunk ids `<doc_id>#<chunk_index>`. To replace a document:

```python
client.delete_points("kb", [f"doc-1#{i}" for i in range(50)])
rag.ingest([{"id": "doc-1", "text": new_text}])
```

A future `update_document` helper is on the roadmap.

---

## Sizing

| Scale | Settings |
|-------|----------|
| ≤100k chunks | dim 768/1536, 1 shard, default HNSW |
| ≤1M | bump `ef_construction=400`, snapshot+compact daily |
| ≤10M | shard 2–4 nodes; consider scalar quantization |
| ≥100M | multi-shard + Raft replication; benchmark with your real query latency |

---

## Reference: `RagPipeline` parameters

| Param | Default | Meaning |
|-------|---------|---------|
| `collection` | — | Collection name |
| `dimension` | — | Must match `embed(text)` output |
| `embed` | — | `Callable[[str], list[float]]` |
| `text_field` | `text` | Payload key where chunk text is stored |
| `chunk_size` | 512 | Char window |
| `overlap` | 64 | Char overlap between chunks |
| `metric` | `cosine` | Distance metric |
| `bm25_text_field` | `text_field` | Enables BM25 path |
| `sparse_enabled` | `false` | Enable sparse inverted index |

`query()` accepts: `top_k`, `filter`, `search_mode`, `hybrid_alpha`,
`multi_query`, `rerank`.

The Node, Go, and Java versions mirror these names — see
[`guides/sdks.md`](sdks.md).
