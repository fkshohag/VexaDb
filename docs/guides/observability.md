# Observability

VectorDB exposes Prometheus metrics, structured JSON logs, and three HTTP
probes (`/health`, `/live`, `/ready`).

---

## Probes

| Endpoint | Port | Exempt from auth | Purpose |
|----------|------|------------------|---------|
| `GET /health` | gateway `:8080` | yes | high-level status (`ok`) |
| `GET /live` | gateway `:8080` | yes | process is up (always `200` while running) |
| `GET /ready` | gateway `:8080` | yes | `200` only when upstream `HealthResponse.ready` (leader, if Raft) |
| `GET /metrics` | gateway `:8080` | yes | gateway HTTP metrics |
| `GET /metrics` | server `:9090` | yes | server / engine metrics |
| gRPC `Health` | server `:6334` | yes | full status incl. role + leader endpoint |

The detailed `Health` RPC response:

```proto
HealthResponse {
  string status;        // "ok"
  string node_id;
  uint32 shard_count;
  bool is_leader;       // false on followers (or always true outside Raft)
  string raft_role;     // "leader" | "follower" | "candidate" | ""
  string leader_endpoint;  // "http://host:port"
  bool ready;           // false on followers when readiness_requires_leader
}
```

---

## Prometheus

Server `:9090` exposes:

| Metric | Type | Labels |
|--------|------|--------|
| `vectordb_rpc_total` | counter | `rpc`, `status` |
| `vectordb_rpc_duration_seconds` | histogram | `rpc` |
| `vectordb_rpc_inflight` | gauge | `rpc` |

Gateway `:8080/metrics`:

| Metric | Type | Notes |
|--------|------|-------|
| `vectordb_http_requests_total` | counter | All non-probe HTTP requests |

### Scrape config

```yaml
scrape_configs:
  - job_name: vectordb-server
    static_configs:
      - targets: ['vectordb-server:9090']
  - job_name: vectordb-gateway
    static_configs:
      - targets: ['vectordb-gateway:8080']
    metrics_path: /metrics
```

### Useful queries

```promql
# p95 search latency over 5m
histogram_quantile(0.95, sum by (le) (
  rate(vectordb_rpc_duration_seconds_bucket{rpc="search"}[5m])
))

# Errors per second
sum by (rpc) (rate(vectordb_rpc_total{status!="ok"}[5m]))

# Throughput
sum by (rpc) (rate(vectordb_rpc_total[1m]))
```

---

## Logs

`tracing-subscriber` with the `json` feature emits structured logs. Enable
JSON format with the env filter:

```bash
RUST_LOG=info,vectordb=debug \
  cargo run -p vectordb-server --release -- --config config/example-secure.toml
```

Each search/upsert emits a `query completed` line with fields:

| Field | Meaning |
|-------|---------|
| `rpc` | `search`, `upsert`, `bulk_upsert`, ... |
| `collection` | collection name |
| `top_k` / `upserted` / `hits` | per-RPC quantitative fields |
| Duration is reported via the histogram, not the line itself |

Pipe into a log aggregator (Loki, Elasticsearch, Datadog) by setting the env
var `RUST_LOG` and running with stdout capture.

---

## Dashboards

A Grafana dashboard isn’t shipped, but a starter panel set:

1. **Throughput**: `sum by (rpc)(rate(vectordb_rpc_total[1m]))`
2. **p50/p95/p99 latency**: histogram_quantile queries above
3. **Inflight**: `vectordb_rpc_inflight`
4. **Errors**: non-ok status rate
5. **Cluster health**: `up{job=~"vectordb-.*"}`

For Raft, scrape `Health` periodically (or expose role as a metric — see
roadmap).

---

## Health-check examples

### Kubernetes
```yaml
livenessProbe:
  httpGet: { path: /live, port: 8080 }
  initialDelaySeconds: 5
readinessProbe:
  httpGet: { path: /ready, port: 8080 }
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Docker compose health
```yaml
healthcheck:
  test: ["CMD", "curl", "-fsS", "http://localhost:8080/ready"]
  interval: 5s
  retries: 5
```

---

## Tracing future-track

Distributed tracing (OTel) and per-request slow-query log are on the future
track; today, use Prometheus latency histograms + structured logs.
