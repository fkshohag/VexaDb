#!/usr/bin/env python3
"""
End-to-end load test against a running VectorDB gateway.

Generates random unit vectors, bulk-upserts them, then runs a concurrent
search workload and reports throughput + latency percentiles.

Usage:
    # Stand up gateway first:
    cargo run -p vectordb-server --release &
    VECTORDB_GRPC=http://127.0.0.1:6334 cargo run -p vectordb-gateway --release &

    # Run load test:
    python scripts/load_test.py --count 100000 --queries 5000 --workers 16

Requires Python 3.10+ and `httpx` (or install the Python SDK).
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import httpx
except ImportError:
    print("error: httpx is required. Install with: pip install httpx", file=sys.stderr)
    sys.exit(1)


def random_unit_vector(dim: int) -> list[float]:
    v = [random.random() - 0.5 for _ in range(dim)]
    norm = sum(x * x for x in v) ** 0.5 or 1.0
    return [x / norm for x in v]


def fmt_bytes(n: int) -> str:
    for u in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if n < 1024:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} PiB"


def main() -> int:
    p = argparse.ArgumentParser(description="VectorDB load test (REST gateway)")
    p.add_argument("--base", default=os.environ.get("VECTORDB_URL", "http://127.0.0.1:8080"))
    p.add_argument("--api-key", default=os.environ.get("VECTORDB_API_KEY"))
    p.add_argument("--collection", default="loadtest")
    p.add_argument("--dim", type=int, default=128)
    p.add_argument("--count", type=int, default=100_000, help="vectors to insert")
    p.add_argument("--chunk-size", type=int, default=500, help="server-side WAL chunk")
    p.add_argument("--batch-size", type=int, default=2000, help="vectors per HTTP request")
    p.add_argument("--queries", type=int, default=2000)
    p.add_argument("--workers", type=int, default=16, help="concurrent search workers")
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--metric", default="cosine")
    p.add_argument("--reset", action="store_true", help="delete the collection first")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--skip-insert", action="store_true",
                   help="reuse existing collection; only run search workload")
    p.add_argument("--with-payload", action="store_true",
                   help="attach a small JSON payload to each point")
    args = p.parse_args()

    random.seed(args.seed)
    headers: dict[str, str] = {}
    if args.api_key:
        headers["x-api-key"] = args.api_key
        headers["Authorization"] = f"Bearer {args.api_key}"

    print(f"VectorDB load test — base={args.base} collection={args.collection}")
    print(
        f"config: dim={args.dim} count={args.count} batch={args.batch_size} "
        f"chunk={args.chunk_size} queries={args.queries} workers={args.workers} top_k={args.top_k}"
    )

    with httpx.Client(base_url=args.base, headers=headers, timeout=120.0) as client:
        # 1. Reset
        if args.reset:
            r = client.delete(f"/v1/collections/{args.collection}")
            print(f"  reset: {r.status_code}")

        # 2. Ensure collection
        existing = client.get("/v1/collections")
        existing.raise_for_status()
        if args.collection not in existing.json():
            r = client.post(
                "/v1/collections",
                json={
                    "name": args.collection,
                    "dimension": args.dim,
                    "metric": args.metric,
                },
            )
            r.raise_for_status()
            print(f"  collection created")

        # 3. Bulk upsert
        if not args.skip_insert:
            print("\n## Insert")
            start = time.perf_counter()
            done = 0
            last_print = start
            for batch_start in range(0, args.count, args.batch_size):
                batch_end = min(batch_start + args.batch_size, args.count)
                points = []
                for i in range(batch_start, batch_end):
                    point = {
                        "id": f"p{i}",
                        "values": random_unit_vector(args.dim),
                    }
                    if args.with_payload:
                        point["payload"] = {
                            "ord": i,
                            "category": "even" if i % 2 == 0 else "odd",
                        }
                    points.append(point)
                t = time.perf_counter()
                r = client.post(
                    f"/v1/collections/{args.collection}/bulk",
                    json={"points": points, "chunk_size": args.chunk_size},
                )
                r.raise_for_status()
                done = batch_end
                if time.perf_counter() - last_print >= 2.0:
                    elapsed = time.perf_counter() - start
                    rate = done / elapsed if elapsed else 0
                    eta = (args.count - done) / rate if rate > 0 else 0
                    print(
                        f"    inserted {done:>10,}/{args.count:>10,}  "
                        f"({rate:>8,.0f}/s, eta {eta:5.1f}s)"
                    )
                    last_print = time.perf_counter()
            insert_secs = time.perf_counter() - start
            print(
                f"  insert: {args.count / insert_secs:,.0f} vectors/s "
                f"({insert_secs:.2f}s total, ~{fmt_bytes(args.count * args.dim * 4)} of f32)"
            )

        # 4. Concurrent search workload
        print("\n## Search")
        queries = [random_unit_vector(args.dim) for _ in range(args.queries)]

        latencies: list[float] = []
        start = time.perf_counter()
        # httpx.Client is thread-safe for sync use; share one client across workers.
        with ThreadPoolExecutor(max_workers=args.workers) as pool, \
                httpx.Client(base_url=args.base, headers=headers, timeout=60.0) as cclient:
            futs = [
                pool.submit(_query_with, cclient, args.collection, q, args.top_k)
                for q in queries
            ]
            for fut in as_completed(futs):
                latencies.append(fut.result())
        wall = time.perf_counter() - start

        latencies.sort()
        n = len(latencies)
        p50 = latencies[int(0.50 * (n - 1))]
        p95 = latencies[int(0.95 * (n - 1))]
        p99 = latencies[int(0.99 * (n - 1))]
        avg = statistics.fmean(latencies)
        print(
            f"  search k={args.top_k} workers={args.workers}: "
            f"{args.queries / wall:,.0f} QPS (wall {wall:.2f}s)"
        )
        print(
            f"  latency: p50={p50:.2f}ms p95={p95:.2f}ms p99={p99:.2f}ms avg={avg:.2f}ms"
        )

        # 5. Health snapshot
        h = client.get("/health")
        if h.is_success:
            print(f"\n  /health: {h.json()}")
        m = client.get("/metrics")
        if m.is_success and m.text:
            req_lines = [l for l in m.text.splitlines() if l.startswith("vectordb_http_requests_total")]
            if req_lines:
                print(f"  gateway counter: {req_lines[-1]}")

    return 0


def _query_with(client: "httpx.Client", collection: str, q: list[float], top_k: int) -> float:
    t = time.perf_counter()
    r = client.post(
        f"/v1/collections/{collection}/search",
        json={"vector": q, "top_k": top_k},
    )
    r.raise_for_status()
    return (time.perf_counter() - t) * 1000.0


if __name__ == "__main__":
    sys.exit(main())
