# Go SDK load test

End-to-end stress harness using the Go SDK. Three phases run sequentially against a single freshly-created collection:

| Phase | Endpoint | What it measures |
|-------|----------|------------------|
| **SINGLE** | `POST /v1/collections/:name/upsert` | Online write path: many small concurrent batches, surfaces per-request commit latency. |
| **BULK** | `POST /v1/collections/:name/bulk` | Batch ingestion path: fewer workers, fat payloads, surfaces server-side throughput. |
| **QUERY** | `POST /v1/collections/:name/search` | Read path: concurrent dense top-k searches. |

Each phase prints a live progress line every `LT_PROGRESS_S` seconds, then a summary with success rate, throughput, and `p50 / p95 / p99 / max / mean` latency. Errors are aggregated and the top 5 unique messages are reported.

## Run

```bash
cd sdks/go
go run ./examples/loadtest
```

## Tunables (env vars)

| Var | Default | Notes |
|-----|---------|-------|
| `VECTORDB_URL` | `http://127.0.0.1:8080` | gateway base URL |
| `VECTORDB_API_KEY` | _(unset)_ | required when gateway enforces auth |
| `LT_COLLECTION` | `loadtest` | created fresh, dropped at end (override with `LT_KEEP_COLLECTION=1`) |
| `LT_DIM` | `768` | vector dimension |
| `LT_TOTAL` | `50000` | points inserted per write phase (single _and_ bulk each insert this many) |
| `LT_SINGLE_CONCURRENCY` | `16` | workers for the single-upsert phase |
| `LT_SINGLE_BATCH` | `32` | points per `/upsert` request |
| `LT_BULK_CONCURRENCY` | `4` | workers for the bulk phase |
| `LT_BULK_CHUNK` | `256` | points per `/bulk` request (the gateway now caps write bodies at 64 MB, so you can crank this much higher; 256 keeps per-request latency sane) |
| `LT_QUERY_CONCURRENCY` | `32` | workers for the search phase |
| `LT_QUERY_TOTAL` | `10000` | total searches |
| `LT_QUERY_TOPK` | `20` | top-k for each search |
| `LT_HTTP_TIMEOUT_S` | `60` | client HTTP timeout (raise for very large bulk chunks) |
| `LT_KEEP_COLLECTION` | `0` | `1` to leave the collection populated for follow-up queries |
| `LT_PROGRESS_S` | `5` | seconds between live progress lines (`0` = disable) |

## Examples

Quick smoke test against a local cluster:

```bash
LT_DIM=64 LT_TOTAL=1000 LT_QUERY_TOTAL=200 go run ./examples/loadtest
```

Heavy ingest (50k points × 2 phases at production dimension):

```bash
LT_TOTAL=50000 LT_HTTP_TIMEOUT_S=180 go run ./examples/loadtest
```

Read-only burn-in (collection populated, queries only — keep collection then re-run with `LT_TOTAL=0` … is not supported; instead run twice with the second run hitting the existing collection):

```bash
# 1) write + keep
LT_TOTAL=50000 LT_KEEP_COLLECTION=1 go run ./examples/loadtest

# 2) push more queries against it
LT_TOTAL=0 LT_QUERY_TOTAL=100000 LT_KEEP_COLLECTION=1 LT_PROGRESS_S=5 go run ./examples/loadtest
```

> Note: `LT_TOTAL=0` will skip the write loops (zero tasks) but still create+drop the collection; pair with `LT_KEEP_COLLECTION=1` to preserve data across runs.

## Reading the output

```
---- BULK upsert ----
  duration:       150.26s
  requests:       10 ok, 0 err  (success=100.00%)
  items:          10000 / 10000 expected
  throughput:     0.1 req/s, 67 items/s
  latency p50:    59.40s
  latency p95:    1m2.18s
  latency p99:    1m2.18s
  latency max:    1m2.26s
  latency mean:   51.28s
```

- `requests` counts HTTP calls, `items` counts points/searches.
- `throughput` is wall-clock, not per-worker.
- Error breakdown (truncated) appears only when `errs > 0` — useful for spotting `413`, `502`, `not leader`, or `failed to reach quorum` signatures.
