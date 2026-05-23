# Hybrid search

VectorDB supports four signal sources and two fusion strategies, all driven by
a single `SearchRequest`.

| Signal | Storage | When to use |
|--------|---------|-------------|
| **Dense vector** | HNSW (`f32`) | Semantic similarity from any embedding model |
| **Sparse vector** | Inverted index (`u32 → f32`) | SPLADE, BM25-style sparse embeddings |
| **BM25** | Lexical index over a payload field | Keyword recall, exact-term match |
| **Filter** | Payload indexes | Hard constraints (`category`, `price`, `tags`) |

Fusion modes:

- `hybrid_rrf` — Reciprocal Rank Fusion across the available signals
- `hybrid_weighted` — `α · dense + (1 − α) · lexical`

---

## Configure a hybrid collection

```bash
curl -s -X POST :8080/v1/collections \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "hybrid",
    "dimension": 384,
    "metric": "cosine",
    "bm25_text_field": "text",
    "sparse_enabled": true,
    "payload_indexes": [
      {"field":"category","kind":"keyword"}
    ]
  }'
```

| Field | Effect |
|-------|--------|
| `bm25_text_field` | Path in the JSON payload to lex-index (e.g. `"text"` or `"content.body"`) |
| `sparse_enabled` | Build sparse inverted index for points that include `sparse` |
| `payload_indexes` | Filter pushdown |

---

## Upsert with all signals

```bash
curl -s -X POST :8080/v1/collections/hybrid/upsert \
  -H 'Content-Type: application/json' \
  -d '{
    "points": [
      {
        "id": "doc-1",
        "values": [0.1, 0.2, ... ],
        "payload": {"category": "books", "text": "Vector databases store embeddings."},
        "sparse": {"indices": [10, 25, 88], "values": [0.7, 0.3, 0.5]}
      }
    ]
  }'
```

---

## Query

### Dense (default)
```json
{ "vector": [...], "top_k": 10 }
```

### Sparse only
```json
{
  "vector": [],
  "top_k": 10,
  "sparse_query": {"indices":[10,25],"values":[0.6,0.4]},
  "search_mode": "sparse"
}
```

### BM25 only
```json
{ "vector": [], "top_k": 10, "text_query": "vector database", "search_mode": "bm25" }
```

### Hybrid RRF
```json
{
  "vector": [0.1, 0.2, ...],
  "top_k": 10,
  "text_query": "vector database",
  "sparse_query": {"indices":[10],"values":[1.0]},
  "search_mode": "hybrid_rrf"
}
```

Each signal contributes ranks; the engine fuses with `1 / (60 + rank)` per
signal and returns the top-K.

### Hybrid weighted
```json
{
  "vector": [0.1, 0.2, ...],
  "top_k": 10,
  "text_query": "vector database",
  "search_mode": "hybrid_weighted",
  "hybrid_alpha": 0.7
}
```

`α = 0.7` → 70% weight on dense, 30% on the best lexical signal available
(BM25 if `text_query` set, else sparse).

---

## With a filter

Filters apply **after** retrieval at each path; combine freely with hybrid:

```json
{
  "vector": [...],
  "top_k": 5,
  "text_query": "vector database",
  "search_mode": "hybrid_rrf",
  "filter": {
    "must": [{"key":"category","match":{"value":"books"}}]
  }
}
```

See [`../api/filter-dsl.md`](../api/filter-dsl.md).

---

## Choosing a mode

| Workload | Recommended mode |
|----------|------------------|
| Pure semantic similarity | `dense` |
| Q&A, lexical-heavy queries | `bm25` or `hybrid_rrf` |
| Recall-critical retrieval (RAG) | `hybrid_rrf` |
| Tunable lexical-vs-semantic | `hybrid_weighted` with α grid-searched |
| Term-level overlap matters | include `sparse_query` |

In practice: start with `hybrid_rrf` whenever you have payload text. It’s
robust, parameter-free, and usually beats either signal alone.

---

## Performance notes

- The engine queries each signal for `top_k * 2` candidates internally before
  fusing.
- Sparse postings are scanned exactly; BM25 uses tokenised payload text with
  document frequency stats updated on every upsert.
- Quantization (`scalar_quantization = true`) reduces memory but the HNSW
  graph itself stays `f32` — quantized codes are kept alongside for future
  reranking workflows.

For an end-to-end RAG pipeline that uses hybrid search, see
[`rag.md`](rag.md).
