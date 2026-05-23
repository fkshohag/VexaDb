# VectorDB Python SDK

REST client and RAG helpers for [VectorDB](../../README.md) (via `vectordb-gateway`).

## Install

```bash
cd sdks/python
pip install -e .
```

## Quick start

```python
from vectordb import VectorDbClient

client = VectorDbClient("http://127.0.0.1:8080", api_key="your-key")
client.create_collection("docs", dimension=3)
client.upsert("docs", [{"id": "a", "values": [1.0, 0.0, 0.0], "payload": {"text": "hello"}}])
hits = client.search("docs", [1.0, 0.0, 0.0], top_k=5)
print(hits)
client.close()
```

## RAG pipeline

```python
from vectordb import VectorDbClient, RagPipeline

def embed(text: str) -> list[float]:
    # plug in OpenAI, sentence-transformers, etc.
    ...

client = VectorDbClient("http://127.0.0.1:8080")
rag = RagPipeline(client, "kb", dimension=1536, embed=embed)
rag.ingest([{"id": "doc1", "text": "Vector databases store embeddings..."}])
for hit in rag.query("what is a vector database?", top_k=3, rerank=True):
    print(hit.score, hit.text)
```

See [`examples/rag_demo.py`](examples/rag_demo.py).
