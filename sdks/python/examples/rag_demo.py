#!/usr/bin/env python3
"""Minimal RAG demo using a trivial hash embedding (for local testing only)."""

from __future__ import annotations

import hashlib
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from vectordb import RagPipeline, VectorDbClient  # noqa: E402

DIM = 32


def fake_embed(text: str) -> list[float]:
    """Deterministic pseudo-embedding — replace with a real model in production."""
    digest = hashlib.sha256(text.encode()).digest()
    vec = [((digest[i % len(digest)] / 255.0) * 2 - 1) for i in range(DIM)]
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


def main() -> None:
    base = os.environ.get("VECTORDB_URL", "http://127.0.0.1:8080")
    api_key = os.environ.get("VECTORDB_API_KEY")
    client = VectorDbClient(base, api_key=api_key)
    rag = RagPipeline(client, "rag_demo", dimension=DIM, embed=fake_embed)

    rag.ingest(
        [
            {
                "id": "intro",
                "text": (
                    "Vector databases store high-dimensional embeddings for "
                    "similarity search. They power RAG and recommendation systems."
                ),
            },
            {
                "id": "ops",
                "text": (
                    "VectorDB supports HNSW indexing, metadata filters, hybrid "
                    "BM25+dense search, and Raft replication."
                ),
            },
        ]
    )

    for hit in rag.query("hybrid search and replication", top_k=3, rerank=True):
        print(f"{hit.score:.4f} [{hit.document_id}] {hit.text[:80]}...")

    client.close()


if __name__ == "__main__":
    main()
