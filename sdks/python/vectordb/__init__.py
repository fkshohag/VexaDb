"""VectorDB Python SDK — REST client and RAG helpers."""

from vectordb.client import VectorDbClient, VectorDbError
from vectordb.rag import RagHit, RagPipeline, chunk_text, expand_query, rerank_by_overlap

__all__ = [
    "VectorDbClient",
    "VectorDbError",
    "RagPipeline",
    "RagHit",
    "chunk_text",
    "expand_query",
    "rerank_by_overlap",
]

__version__ = "0.1.0"
