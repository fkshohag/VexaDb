#!/usr/bin/env python3
"""
End-to-end RAG demo using real system-design content.

Loads the markdown corpus in `data/system-design/`, chunks it, embeds each
chunk with sentence-transformers (or a deterministic hashed bag-of-words
fallback), upserts into a VectorDB collection through the REST gateway,
then runs a battery of representative system-design questions and prints
the retrieved chunks.

Usage:
    # 1. Stand up the database and gateway
    cargo run -p vectordb-server --release &
    VECTORDB_GRPC=http://127.0.0.1:6334 cargo run -p vectordb-gateway --release &

    # 2. Install Python deps (macOS: use python3 / pip3, not python / pip)
    python3 -m venv .venv && source .venv/bin/activate
    pip install httpx
    pip install -e sdks/python                       # optional: in-repo client + RagPipeline
    pip install sentence-transformers                # optional but recommended

    # 3. Run the demo
    python scripts/rag_system_design.py                       # uses sentence-transformers if installed
    python scripts/rag_system_design.py --embedder hash --reset --hybrid
    python scripts/rag_system_design.py --query "How do I shard a database?"
    python scripts/rag_system_design.py --hybrid              # dense + BM25 fusion
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path

# Make the in-repo Python SDK importable without installation.
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SDK_PATH = ROOT / "sdks" / "python"
if SDK_PATH.exists() and str(SDK_PATH) not in sys.path:
    sys.path.insert(0, str(SDK_PATH))

try:
    from vectordb import VectorDbClient
    from vectordb.rag import RagPipeline
except ImportError as e:
    print(
        "error: cannot import vectordb SDK. Run `pip install -e sdks/python` "
        f"or set PYTHONPATH={SDK_PATH}",
        file=sys.stderr,
    )
    print(f"  underlying: {e}", file=sys.stderr)
    sys.exit(1)


CORPUS_DIR = ROOT / "data" / "system-design"

DEFAULT_QUERIES: list[str] = [
    "How do I scale a relational database when a single server isn't enough?",
    "What is the CAP theorem and how does it shape replication design?",
    "How does the Raft consensus algorithm elect a leader?",
    "When should I use a message queue versus a synchronous API call?",
    "What's the difference between a cache-aside and a write-through cache?",
    "Explain consistent hashing and why it helps with resharding.",
    "How does HNSW work for approximate nearest neighbour search?",
    "What is the outbox pattern and which problem does it solve?",
    "How do I build a rate limiter that works across many app instances?",
    "What's the difference between an API gateway and a service mesh?",
    "Why prefer p95/p99 latency over averages?",
    "When does a B-tree index outperform an LSM-tree index?",
]


# ---------------------------------------------------------------------------
# Embedders
# ---------------------------------------------------------------------------


def make_st_embedder(model_name: str) -> tuple[Callable[[str], list[float]], int]:
    """Real semantic embeddings via sentence-transformers."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print(
            "error: sentence-transformers is not installed. Either:\n"
            "  pip install sentence-transformers\n"
            "  or run with --embedder hash for the deterministic fallback",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"  loading sentence-transformers model: {model_name}")
    t0 = time.perf_counter()
    model = SentenceTransformer(model_name)
    dim = int(model.get_sentence_embedding_dimension())
    print(f"  model loaded in {time.perf_counter() - t0:.1f}s, dim={dim}")

    def embed(text: str) -> list[float]:
        v = model.encode(text, normalize_embeddings=True, show_progress_bar=False)
        return v.tolist()

    return embed, dim


def make_hash_embedder(dim: int = 256) -> tuple[Callable[[str], list[float]], int]:
    """
    Deterministic, dependency-free fallback. Hash unigrams + bigrams + 3-grams of
    each token into a fixed-dim vector and L2-normalize. Quality is far below a
    real model but lets the demo run anywhere.
    """
    bigram_re = re.compile(r"\w+")

    def embed(text: str) -> list[float]:
        v = [0.0] * dim
        toks = bigram_re.findall(text.lower())
        if not toks:
            return v
        # unigrams
        for t in toks:
            v[hash(("u", t)) % dim] += 1.0
        # bigrams
        for a, b in zip(toks, toks[1:]):
            v[hash(("b", a, b)) % dim] += 0.7
        # character 3-grams (catches typos / morphological variants)
        for t in toks:
            for i in range(len(t) - 2):
                v[hash(("c", t[i : i + 3])) % dim] += 0.3
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]

    return embed, dim


# ---------------------------------------------------------------------------
# Corpus loading
# ---------------------------------------------------------------------------


def load_corpus(corpus_dir: Path) -> list[dict]:
    if not corpus_dir.exists():
        print(f"error: corpus directory not found: {corpus_dir}", file=sys.stderr)
        sys.exit(1)
    docs: list[dict] = []
    for md_path in sorted(corpus_dir.glob("*.md")):
        # Skip macOS AppleDouble metadata files and other hidden siblings.
        if md_path.name.startswith(".") or md_path.name.startswith("_"):
            continue
        try:
            text = md_path.read_text(encoding="utf-8").strip()
        except UnicodeDecodeError:
            print(f"  skipping non-utf8 file: {md_path.name}")
            continue
        if not text:
            continue
        # Use the first markdown H1 as title if present
        first = text.splitlines()[0].lstrip("#").strip()
        docs.append(
            {
                "id": md_path.stem,
                "text": text,
                "payload": {
                    "title": first,
                    "source": str(md_path.relative_to(ROOT)),
                    "topic": md_path.stem.split("-", 1)[-1] if "-" in md_path.stem else md_path.stem,
                },
            }
        )
    return docs


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------


def print_hits(query: str, hits: list, *, max_chars: int = 220) -> None:
    print(f"\n>>> {query}")
    if not hits:
        print("    (no results)")
        return
    for i, hit in enumerate(hits, 1):
        title = hit.payload.get("title") or hit.document_id
        snippet = hit.text.replace("\n", " ").strip()
        if len(snippet) > max_chars:
            snippet = snippet[: max_chars - 1] + "…"
        print(
            f"  [{i}] score={hit.score:.4f}  doc={hit.document_id}#{hit.chunk_index}"
        )
        print(f"      title: {title}")
        print(f"      text : {snippet}")


def main() -> int:
    p = argparse.ArgumentParser(description="VectorDB system-design RAG demo")
    p.add_argument("--base", default=os.environ.get("VECTORDB_URL", "http://127.0.0.1:8080"))
    p.add_argument("--api-key", default=os.environ.get("VECTORDB_API_KEY"))
    p.add_argument("--collection", default="sysdesign")
    p.add_argument(
        "--embedder",
        choices=("st", "hash", "auto"),
        default="auto",
        help="st=sentence-transformers, hash=deterministic fallback, auto=st-if-available",
    )
    p.add_argument(
        "--model",
        default="sentence-transformers/all-MiniLM-L6-v2",
        help="sentence-transformers model name",
    )
    p.add_argument("--corpus", default=str(CORPUS_DIR))
    p.add_argument("--chunk-size", type=int, default=600)
    p.add_argument("--overlap", type=int, default=120)
    p.add_argument("--top-k", type=int, default=4)
    p.add_argument("--query", action="append", help="Override the default queries (repeatable)")
    p.add_argument("--reset", action="store_true", help="Drop the collection first")
    p.add_argument("--hybrid", action="store_true", help="Use dense + BM25 hybrid search")
    p.add_argument(
        "--hybrid-alpha",
        type=float,
        default=0.6,
        help="Weight for dense score in hybrid fusion (0..1)",
    )
    p.add_argument(
        "--multi-query",
        action="store_true",
        help="Expand each question into variants and fuse with RRF",
    )
    p.add_argument("--rerank", action="store_true", help="Token-overlap rerank after retrieval")
    p.add_argument("--skip-ingest", action="store_true", help="Reuse existing collection")
    args = p.parse_args()

    print("=== VectorDB system-design RAG demo ===")
    print(f"  gateway:    {args.base}")
    print(f"  collection: {args.collection}")
    print(f"  corpus:     {args.corpus}")

    embedder_choice = args.embedder
    if embedder_choice == "auto":
        try:
            import sentence_transformers  # noqa: F401
            embedder_choice = "st"
        except ImportError:
            embedder_choice = "hash"
            print("  sentence-transformers not installed, falling back to hash embedder")

    if embedder_choice == "st":
        embed, dim = make_st_embedder(args.model)
    else:
        embed, dim = make_hash_embedder(256)
        print(f"  using hash-BoW embedder, dim={dim} (semantics will be weak)")

    docs = load_corpus(Path(args.corpus))
    print(f"  loaded {len(docs)} documents from corpus")

    headers = {}
    if args.api_key:
        headers["x-api-key"] = args.api_key

    client = VectorDbClient(base_url=args.base, api_key=args.api_key)

    if args.reset and args.collection in client.list_collections():
        print(f"  dropping existing collection {args.collection!r}")
        client.delete_collection(args.collection)

    # Hybrid search needs a BM25-enabled collection. We reset+recreate when the
    # caller asked for hybrid and the existing collection wasn't built that way.
    if args.hybrid and args.collection in client.list_collections() and not args.skip_ingest:
        meta = client.describe_collection(args.collection)
        if not meta.get("bm25_text_field"):
            print("  --hybrid requested but collection lacks BM25; recreating")
            client.delete_collection(args.collection)

    pipeline = RagPipeline(
        client=client,
        collection=args.collection,
        dimension=dim,
        embed=embed,
        chunk_size=args.chunk_size,
        overlap=args.overlap,
        bm25_text_field="text" if args.hybrid else None,
        sparse_enabled=False,
    )

    if not args.skip_ingest:
        print("\n## Ingest")
        t0 = time.perf_counter()
        n = pipeline.ingest(docs)
        secs = time.perf_counter() - t0
        print(f"  upserted {n} chunks in {secs:.2f}s ({n / secs:.0f} chunks/s)")
    else:
        print("\n## Ingest skipped (reusing existing collection)")

    queries = args.query or DEFAULT_QUERIES
    search_mode = "hybrid" if args.hybrid else "dense"

    print(f"\n## Retrieval — mode={search_mode}, top_k={args.top_k}, "
          f"multi_query={args.multi_query}, rerank={args.rerank}")
    total_lat = 0.0
    for q in queries:
        t0 = time.perf_counter()
        hits = pipeline.query(
            q,
            top_k=args.top_k,
            search_mode=search_mode,
            hybrid_alpha=args.hybrid_alpha,
            multi_query=args.multi_query,
            rerank=args.rerank,
        )
        total_lat += time.perf_counter() - t0
        print_hits(q, hits)

    avg_ms = (total_lat / max(len(queries), 1)) * 1000
    print(f"\nAverage retrieval latency: {avg_ms:.1f} ms across {len(queries)} queries")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
