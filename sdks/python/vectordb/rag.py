"""RAG helpers: chunking, ingest, retrieve, query expansion, lightweight rerank."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from vectordb.client import VectorDbClient

EmbedFn = Callable[[str], list[float]]


@dataclass
class RagHit:
    id: str
    score: float
    text: str
    payload: dict[str, Any]
    chunk_index: int
    document_id: str


def chunk_text(
    text: str,
    *,
    chunk_size: int = 512,
    overlap: int = 64,
) -> list[str]:
    """Split text into overlapping character windows (whitespace-aware breaks)."""
    text = text.strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        if end < len(text):
            split = text.rfind(" ", start, end)
            if split > start:
                end = split
        piece = text[start:end].strip()
        if piece:
            chunks.append(piece)
        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return chunks


def expand_query(query: str, *, max_variants: int = 3) -> list[str]:
    """Produce a small set of query variants for multi-query retrieval."""
    q = query.strip()
    if not q:
        return []
    variants = [q]
    lower = q.lower()
    if lower != q:
        variants.append(lower)
    # Drop stop-heavy short tokens for a keyword-style variant
    words = [w for w in re.findall(r"\w+", q) if len(w) > 2]
    if len(words) >= 2:
        variants.append(" ".join(words))
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
        if len(out) >= max_variants:
            break
    return out


def rerank_by_overlap(
    query: str,
    hits: Sequence[RagHit],
    *,
    top_k: int | None = None,
) -> list[RagHit]:
    """Re-order hits by token overlap with the query (cheap post-retrieval rerank)."""
    q_tokens = set(re.findall(r"\w+", query.lower()))
    if not q_tokens:
        return list(hits)[:top_k]

    def score(hit: RagHit) -> float:
        t_tokens = set(re.findall(r"\w+", hit.text.lower()))
        overlap = len(q_tokens & t_tokens) / max(len(q_tokens), 1)
        return hit.score + overlap

    ranked = sorted(hits, key=score, reverse=True)
    return ranked[:top_k] if top_k else ranked


class RagPipeline:
    """
    End-to-end RAG over VectorDB: chunk documents, embed, upsert, search.

    Pass any embedding function (OpenAI, sentence-transformers, etc.).
    """

    def __init__(
        self,
        client: VectorDbClient,
        collection: str,
        dimension: int,
        embed: EmbedFn,
        *,
        text_field: str = "text",
        chunk_size: int = 512,
        overlap: int = 64,
        metric: str = "cosine",
        bm25_text_field: str | None = None,
        sparse_enabled: bool = False,
    ) -> None:
        self.client = client
        self.collection = collection
        self.dimension = dimension
        self.embed = embed
        self.text_field = text_field
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.metric = metric
        self.bm25_text_field = bm25_text_field or text_field
        self.sparse_enabled = sparse_enabled
        self._collection_ready = False

    def ensure_collection(self) -> None:
        if self._collection_ready:
            return
        names = self.client.list_collections()
        if self.collection not in names:
            self.client.create_collection(
                self.collection,
                self.dimension,
                metric=self.metric,
                bm25_text_field=self.bm25_text_field if self.bm25_text_field else None,
                sparse_enabled=self.sparse_enabled,
            )
        self._collection_ready = True

    def ingest(
        self,
        documents: Sequence[Mapping[str, Any]],
        *,
        bulk: bool = True,
        chunk_size: int | None = None,
    ) -> int:
        """
        Ingest documents with fields: ``id``, ``text``, optional ``payload`` dict.
        """
        self.ensure_collection()
        points: list[dict[str, Any]] = []
        for doc in documents:
            doc_id = str(doc["id"])
            text = str(doc.get("text", ""))
            base_payload = dict(doc.get("payload") or {})
            for i, chunk in enumerate(
                chunk_text(
                    text,
                    chunk_size=chunk_size or self.chunk_size,
                    overlap=self.overlap,
                )
            ):
                point_id = f"{doc_id}#{i}"
                payload = {
                    **base_payload,
                    self.text_field: chunk,
                    "document_id": doc_id,
                    "chunk_index": i,
                }
                vector = self.embed(chunk)
                if len(vector) != self.dimension:
                    raise ValueError(
                        f"embedding dimension {len(vector)} != {self.dimension}"
                    )
                points.append(
                    {
                        "id": point_id,
                        "values": vector,
                        "payload": payload,
                    }
                )

        if not points:
            return 0
        if bulk:
            return self.client.bulk_upsert(self.collection, points)
        return self.client.upsert(self.collection, points)

    def query(
        self,
        query_text: str,
        *,
        top_k: int = 5,
        filter: Mapping[str, Any] | None = None,
        search_mode: str = "dense",
        hybrid_alpha: float = 0.5,
        multi_query: bool = False,
        rerank: bool = False,
    ) -> list[RagHit]:
        """Embed the query, search, optionally fuse multi-query variants and rerank."""
        self.ensure_collection()
        queries = expand_query(query_text) if multi_query else [query_text]
        by_id: dict[str, RagHit] = {}

        for q in queries:
            vector = self.embed(q)
            raw = self.client.search(
                self.collection,
                vector,
                top_k=top_k,
                filter=filter,
                text_query=q if search_mode != "dense" else None,
                search_mode=search_mode,
                hybrid_alpha=hybrid_alpha,
            )
            for rank, row in enumerate(raw):
                pid = row["id"]
                point = self.client.get_point(self.collection, pid)
                payload = (point or {}).get("payload") or {}
                if isinstance(payload, str):
                    payload = {}
                text = str(payload.get(self.text_field, ""))
                doc_id = str(payload.get("document_id", pid.split("#")[0]))
                chunk_index = int(payload.get("chunk_index", 0))
                # RRF-style merge across query variants
                rrf = 1.0 / (60 + rank + 1)
                if pid in by_id:
                    by_id[pid].score += rrf
                else:
                    by_id[pid] = RagHit(
                        id=pid,
                        score=rrf,
                        text=text,
                        payload=payload,
                        chunk_index=chunk_index,
                        document_id=doc_id,
                    )

        hits = sorted(by_id.values(), key=lambda h: h.score, reverse=True)[:top_k]
        if rerank:
            hits = rerank_by_overlap(query_text, hits, top_k=top_k)
        return hits
