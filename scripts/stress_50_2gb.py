#!/usr/bin/env python3
"""stress_50_2gb.py — create N collections and insert ~T GB of vector data.

Defaults:
    50 collections, ~2 GB total raw f32 vector bytes, dim 768, dim variety per shard.

Usage:
    python3 scripts/stress_50_2gb.py                 # 50 collections, 2 GB
    python3 scripts/stress_50_2gb.py --collections 50 --target-gb 2 --dim 768
    python3 scripts/stress_50_2gb.py --concurrency 8 --batch 500
    python3 scripts/stress_50_2gb.py --teardown      # remove the collections again

Notes
-----
* Vectors are random unit vectors generated client-side (numpy if available, else stdlib).
  We do NOT call LM Studio — embedding 700K texts would take hours and isn't the goal.
* "Total bytes" is computed from raw f32 size (dim * 4 * count). Wire size (JSON) and
  on-disk size will be larger because of HNSW edges, BM25, payload, WAL, etc.
* Each collection gets a distinct name like ``stress-001``, ``stress-002``, …
* The script is idempotent: running it again skips collections that already have the
  target vector count, so you can resume after Ctrl+C.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Iterable

try:
    import numpy as np
    HAVE_NUMPY = True
except ImportError:
    HAVE_NUMPY = False

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8080")


# ─── HTTP helpers ──────────────────────────────────────────────────────
def _req(url: str, method: str = "GET", body=None, timeout: float = 60.0):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except (urllib.error.URLError, ConnectionResetError, BrokenPipeError, TimeoutError) as e:
        # Transient connection errors — surface as 0 to let callers retry.
        return 0, str(e).encode()


def _req_retry(url: str, method: str, body, max_retries: int = 6,
               base_delay: float = 1.0, timeout: float = 120.0):
    """POST with exponential backoff on 5xx / connection errors."""
    last = (0, b"unknown")
    for attempt in range(max_retries):
        code, data = _req(url, method=method, body=body, timeout=timeout)
        if code == 200 or code in (201, 204, 409):
            return code, data
        if 400 <= code < 500 and code not in (408, 425, 429):
            # Real client-side error — don't retry.
            return code, data
        sleep_for = base_delay * (2 ** attempt) + random.uniform(0, 0.4)
        time.sleep(min(sleep_for, 30.0))
        last = (code, data)
    return last


def gateway_ok() -> bool:
    code, _ = _req(f"{GATEWAY}/v1/collections")
    return code == 200


def list_collections() -> list[str]:
    code, body = _req(f"{GATEWAY}/v1/collections")
    if code != 200:
        return []
    return json.loads(body)


def get_vector_count(name: str) -> int:
    code, body = _req(f"{GATEWAY}/v1/collections/{name}")
    if code != 200:
        return -1
    try:
        info = json.loads(body)
        return int(info.get("vector_count", 0))
    except Exception:
        return -1


def create_collection(name: str, dim: int) -> bool:
    code, _ = _req(
        f"{GATEWAY}/v1/collections",
        method="POST",
        body={
            "name": name,
            "dimension": dim,
            "distance": "cosine",
            "bm25_text_field": "",
        },
    )
    return code in (200, 201) or code == 409


def delete_collection(name: str) -> bool:
    code, _ = _req(f"{GATEWAY}/v1/collections/{name}", method="DELETE")
    return code in (200, 204, 404)


def bulk_upsert(name: str, points: list[dict], chunk_size: int = 500) -> int:
    code, body = _req_retry(
        f"{GATEWAY}/v1/collections/{name}/bulk",
        method="POST",
        body={"points": points, "chunk_size": chunk_size},
        timeout=300.0,
    )
    if code != 200:
        raise RuntimeError(f"bulk_upsert {name} failed {code}: {body[:200]!r}")
    return json.loads(body).get("upserted", len(points))


# ─── Vector generation ────────────────────────────────────────────────
def make_unit_vectors(count: int, dim: int, seed: int) -> list[list[float]]:
    if HAVE_NUMPY:
        rng = np.random.default_rng(seed)
        m = rng.standard_normal((count, dim)).astype("float32")
        norms = np.linalg.norm(m, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        m /= norms
        # Round to 6 decimals to keep JSON wire size predictable (~9 chars / number).
        m = np.round(m, 6)
        return m.tolist()
    rng = random.Random(seed)
    out = []
    for _ in range(count):
        v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
        s = sum(x * x for x in v) ** 0.5 or 1.0
        out.append([round(x / s, 6) for x in v])
    return out


# ─── Stats / progress ─────────────────────────────────────────────────
@dataclass
class Stats:
    started_at: float
    target_total: int
    upserted: int = 0
    raw_bytes: int = 0
    per_collection: dict[str, int] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, name: str, n: int, dim: int):
        with self.lock:
            self.upserted += n
            self.raw_bytes += n * dim * 4
            self.per_collection[name] = self.per_collection.get(name, 0) + n

    def fail(self, msg: str):
        with self.lock:
            self.failures.append(msg)

    def snapshot(self):
        with self.lock:
            elapsed = max(time.time() - self.started_at, 1e-6)
            return {
                "upserted": self.upserted,
                "raw_bytes": self.raw_bytes,
                "elapsed": elapsed,
                "rate": self.upserted / elapsed,
                "mb_per_s": (self.raw_bytes / 1024 / 1024) / elapsed,
                "pct": (self.upserted / max(self.target_total, 1)) * 100,
                "failures": list(self.failures),
                "per_collection": dict(self.per_collection),
            }


def fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024 or u == "TB":
            return f"{f:7.2f} {u}"
        f /= 1024
    return f"{f:.2f} TB"


# ─── Worker ────────────────────────────────────────────────────────────
def fill_collection(
    name: str,
    dim: int,
    target_count: int,
    batch: int,
    chunk_size: int,
    seed: int,
    stats: Stats,
):
    existing = max(get_vector_count(name), 0)
    if existing >= target_count:
        return f"skip {name} (already {existing}/{target_count})"

    remaining = target_count - existing
    base_id = existing
    inserted_here = 0

    while remaining > 0:
        n = min(batch, remaining)
        vectors = make_unit_vectors(n, dim, seed + base_id)
        points = [
            {
                "id": f"{name}-{base_id + i:07d}",
                "values": vectors[i],
                "payload": {
                    "collection": name,
                    "i": base_id + i,
                    "dim": dim,
                },
            }
            for i in range(n)
        ]
        try:
            bulk_upsert(name, points, chunk_size=chunk_size)
        except Exception as e:
            stats.fail(f"{name}: {e}")
            return f"FAIL {name}: {e}"
        stats.add(name, n, dim)
        base_id += n
        remaining -= n
        inserted_here += n

    return f"done {name} (+{inserted_here})"


# ─── Driver ────────────────────────────────────────────────────────────
def run(args: argparse.Namespace):
    if not gateway_ok():
        print(f"gateway not reachable at {GATEWAY}", file=sys.stderr)
        sys.exit(1)

    target_total_bytes = int(args.target_gb * 1024 * 1024 * 1024)
    bytes_per_vec = args.dim * 4
    total_vectors = target_total_bytes // bytes_per_vec
    per_collection = total_vectors // args.collections

    print("─" * 72)
    print(f"Gateway      : {GATEWAY}")
    print(f"Collections  : {args.collections} (prefix='{args.prefix}')")
    print(f"Vector dim   : {args.dim}  (raw f32 → {bytes_per_vec} bytes/vec)")
    print(f"Target       : {args.target_gb:.2f} GB raw f32 vector data")
    print(f"             ≈ {total_vectors:,} vectors total ({per_collection:,} per coll)")
    print(f"Batch / chunk: HTTP={args.batch}  server-chunk={args.chunk_size}")
    print(f"Concurrency  : {args.concurrency}")
    print(f"numpy        : {'yes' if HAVE_NUMPY else 'no (slower)'}")
    print("─" * 72)

    if args.teardown:
        existing = list_collections()
        targets = [n for n in existing if n.startswith(args.prefix)]
        print(f"Tearing down {len(targets)} collections matching '{args.prefix}*'")
        for n in targets:
            delete_collection(n)
            print(f"  deleted {n}")
        print("done.")
        return

    names = [f"{args.prefix}{i+1:03d}" for i in range(args.collections)]

    print("creating collections…")
    for name in names:
        if not create_collection(name, args.dim):
            print(f"  failed to create {name}", file=sys.stderr)
            sys.exit(2)
    print(f"  {args.collections} collections ready")

    stats = Stats(started_at=time.time(), target_total=total_vectors)

    stop_progress = threading.Event()

    def progress_loop():
        while not stop_progress.wait(2.0):
            s = stats.snapshot()
            print(
                f"  {s['upserted']:>10,}/{stats.target_total:,} vecs "
                f"({s['pct']:5.1f}%)  {fmt_bytes(s['raw_bytes'])} raw  "
                f"{s['rate']:>8,.0f} v/s  {s['mb_per_s']:5.1f} MB/s",
                end="\r",
                flush=True,
            )

    threading.Thread(target=progress_loop, daemon=True).start()

    print(f"\ninserting via {args.concurrency} workers…")
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [
            ex.submit(
                fill_collection,
                name,
                args.dim,
                per_collection,
                args.batch,
                args.chunk_size,
                seed=hash(name) & 0xFFFFFFFF,
                stats=stats,
            )
            for name in names
        ]
        for fut in as_completed(futs):
            msg = fut.result()
            print(f"\n  {msg}")

    stop_progress.set()
    s = stats.snapshot()

    print()
    print("─" * 72)
    print(f"Inserted     : {s['upserted']:,} vectors")
    print(f"Raw f32 size : {fmt_bytes(s['raw_bytes'])}  ({s['raw_bytes']:,} bytes)")
    print(f"Elapsed      : {s['elapsed']:.1f}s")
    print(f"Throughput   : {s['rate']:,.0f} vectors/sec   {s['mb_per_s']:.2f} MB/s")
    print(f"Collections  : {len(s['per_collection'])} / {args.collections}")
    if s["failures"]:
        print(f"Failures     : {len(s['failures'])}")
        for line in s["failures"][:10]:
            print(f"   {line}")
    else:
        print("Failures     : none")
    print("─" * 72)
    print(f"Tip — list:   curl -s {GATEWAY}/v1/collections | python3 -m json.tool")
    print(f"Tip — info:   curl -s {GATEWAY}/v1/collections/{names[0]} | python3 -m json.tool")
    print(f"Tip — clean:  python3 {sys.argv[0]} --teardown --prefix {args.prefix}")


def main():
    p = argparse.ArgumentParser(description="VectorDB stress test: 50 collections × 2 GB")
    p.add_argument("--collections", type=int, default=50)
    p.add_argument("--target-gb", type=float, default=2.0,
                   help="Total raw f32 vector data across all collections (default 2.0)")
    p.add_argument("--dim", type=int, default=768)
    p.add_argument("--prefix", default="stress-")
    p.add_argument("--batch", type=int, default=0,
                   help="Vectors per HTTP request (0 = auto-pick from dim)")
    p.add_argument("--chunk-size", type=int, default=500,
                   help="Server-side chunk size hint (default 500)")
    p.add_argument("--concurrency", type=int, default=8,
                   help="Parallel collection workers (default 8)")
    p.add_argument("--max-body-bytes", type=int, default=900_000,
                   help="Cap HTTP body to this size (default ~900KB; axum default cap is 2MB)")
    p.add_argument("--teardown", action="store_true",
                   help="Delete all collections matching --prefix and exit")
    args = p.parse_args()
    if args.batch <= 0:
        # Numbers rounded to 6 decimals → ~10 chars each in JSON.
        bytes_per_vec_json = args.dim * 11 + 250  # numbers + id + payload + brackets
        args.batch = max(20, min(500, args.max_body_bytes // bytes_per_vec_json))
    run(args)


if __name__ == "__main__":
    main()
