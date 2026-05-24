"""End-to-end VectorDB quickstart for the Python SDK.

Run from the repo root:

    pip install -e sdks/python
    python sdks/python/examples/quickstart.py

Honors VECTORDB_URL (default http://127.0.0.1:8080) and VECTORDB_API_KEY.
"""

from __future__ import annotations

import os
import random

from vectordb import VectorDbClient, VectorDbError

COLLECTION = "py-quickstart"
DIM = 8
N = 100


def main() -> None:
    base_url = os.environ.get("VECTORDB_URL", "http://127.0.0.1:8080")
    api_key = os.environ.get("VECTORDB_API_KEY")

    with VectorDbClient(base_url, api_key=api_key) as c:
        print(f"connected: {base_url} -> {c.health()}")

        try:
            c.delete_collection(COLLECTION)
        except VectorDbError as e:
            if e.status_code not in (404, 502):
                raise

        c.create_collection(
            COLLECTION,
            DIM,
            metric="cosine",
            payload_indexes=[
                {"field": "category", "kind": "keyword"},
                {"field": "score", "kind": "numeric"},
            ],
            bm25_text_field="text",
        )
        print(f"created collection {COLLECTION!r} (dim={DIM})")

        rng = random.Random(42)
        points = [
            {
                "id": f"doc-{i:03d}",
                "values": [rng.gauss(0.0, 1.0) for _ in range(DIM)],
                "payload": {
                    "category": ["news", "blog", "paper"][i % 3],
                    "score": i,
                    "text": f"document {i} about quickstart",
                },
            }
            for i in range(N)
        ]
        upserted = c.bulk_upsert(COLLECTION, points, chunk_size=32)
        print(f"upserted {upserted} points")

        query_vec = [rng.gauss(0.0, 1.0) for _ in range(DIM)]

        print("dense top-5:")
        for h in c.search(COLLECTION, query_vec, top_k=5):
            print(f"  {h['id']}  score={h['score']:.4f}")

        print("filtered (category=news) top-5:")
        for h in c.search(
            COLLECTION,
            query_vec,
            top_k=5,
            filter={"must": [{"key": "category", "match": {"value": "news"}}]},
        ):
            print(f"  {h['id']}  score={h['score']:.4f}")

        print("hybrid (RRF) top-5:")
        for h in c.search(
            COLLECTION,
            query_vec,
            top_k=5,
            text_query="quickstart",
            search_mode="hybrid_rrf",
        ):
            print(f"  {h['id']}  score={h['score']:.4f}")

        pt = c.get_point(COLLECTION, "doc-000")
        if pt is not None:
            print(f"get doc-000: payload={pt['payload']}")

        try:
            cluster = c.cluster_status()
            print(
                f"cluster: {cluster['shard_count']} shards x RF={cluster['replication_factor']}, "
                f"{len(cluster['nodes'])} nodes"
            )
        except VectorDbError as e:
            print(f"(cluster status unavailable: {e})")

        deleted = c.delete_points(COLLECTION, ["doc-000", "doc-001"])
        print(f"deleted {deleted} points")

        c.delete_collection(COLLECTION)
        print("done.")


if __name__ == "__main__":
    main()
