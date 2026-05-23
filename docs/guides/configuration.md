# Configuration

VectorDB nodes are configured via a TOML file (`--config path.toml`),
overridable by environment variables and CLI flags. Example configs live in
[`config/`](../../config/).

---

## Loading order

1. Built-in defaults
2. TOML file (`--config` or `VECTORDB_CONFIG` env)
3. Environment variables
4. CLI flags

CLI flags always win.

---

## Top-level sections

```toml
[server]      # gRPC bind, readiness behaviour
[storage]    # data directory, WAL fsync
[cluster]    # node id, role, sharding, peers
[auth]       # API keys (optional)
[tls]        # gRPC TLS (optional)
[metrics]    # Prometheus listener (optional)
[raft]       # Raft replication group (optional)
```

---

## `[server]`

```toml
[server]
listen = "0.0.0.0:6334"
readiness_requires_leader = true   # /ready returns 503 on followers
```

| Key | Default | Notes |
|-----|---------|-------|
| `listen` | `0.0.0.0:6334` | gRPC bind |
| `readiness_requires_leader` | `false` | Set `true` so load balancers drain followers |

CLI: `--listen 0.0.0.0:6334`. Env: `VECTORDB_LISTEN`.

---

## `[storage]`

```toml
[storage]
data_dir = "./data"
sync_wal = true
```

| Key | Default | Notes |
|-----|---------|-------|
| `data_dir` | `./data` | Holds `wal.log`, `meta/` (RocksDB), `snapshots/` |
| `sync_wal` | `true` | `false` → faster writes, less durability |

CLI: `--data-dir`. Env: `VECTORDB_DATA_DIR`.

---

## `[cluster]`

```toml
[cluster]
node_id = "node-1"
role = "all_in_one"   # router | data | all_in_one
shard_count = 1
shard_id = 0

# Routers and clients use this list (data nodes can leave it empty)
[[cluster.nodes]]
id = "shard-0"
grpc = "http://127.0.0.1:6334"
shard_ids = [0]

[[cluster.nodes]]
id = "shard-1"
grpc = "http://127.0.0.1:6335"
shard_ids = [1]
```

| Key | Notes |
|-----|-------|
| `role` | `data` (owns shards), `router` (fan-out only), `all_in_one` (single-node dev) |
| `shard_count` | Total shards in the cluster |
| `shard_id` | This data node’s shard (ignored on routers) |
| `cluster.nodes[].shard_ids` | Which shards live on each node |

CLI: `--node-id`. Env: `VECTORDB_NODE_ID`.

---

## `[auth]`

```toml
[auth]
required = true
keys = ["dev-secret-key-change-me", "another-key"]
```

When `required = true`, every gRPC RPC except `Health` checks for a valid API
key in `x-api-key` or `Authorization: Bearer <key>`. The gateway enforces the
same on HTTP (probes excluded).

To rotate, add the new key, redeploy, then remove the old. Multiple keys are
accepted simultaneously.

---

## `[tls]`

```toml
[tls]
cert_file = "./certs/server.crt"
key_file  = "./certs/server.key"
```

When set, the gRPC listener serves TLS only. The gateway has separate env vars:

```bash
VECTORDB_TLS_CERT=/path/server.crt
VECTORDB_TLS_KEY=/path/server.key
```

---

## `[metrics]`

```toml
[metrics]
listen = "0.0.0.0:9090"
```

Exposes Prometheus text on `GET /metrics`. Gateway exposes its own `/metrics`
on `:8080` (HTTP request counters).

---

## `[raft]`

Enables Raft replication for the local shard.

```toml
[raft]
node_id = 1
listen = "0.0.0.0:7334"
vector_endpoint = "http://127.0.0.1:6334"
election_timeout_ms = 750
heartbeat_interval_ms = 150

[[raft.peers]]
id = 1
addr = "127.0.0.1:7334"
grpc = "http://127.0.0.1:6334"

[[raft.peers]]
id = 2
addr = "127.0.0.1:7335"
grpc = "http://127.0.0.1:6335"

[[raft.peers]]
id = 3
addr = "127.0.0.1:7336"
grpc = "http://127.0.0.1:6336"
```

| Key | Notes |
|-----|-------|
| `node_id` | Unique within the Raft group (1, 2, 3…) |
| `listen` | Raft RPC bind (separate from data gRPC) |
| `vector_endpoint` | This node’s Vector API URL — advertised via `HealthResponse.leader_endpoint` |
| `peers[].grpc` | Peer’s Vector API URL — used for leader-redirect |

See [`clustering.md`](clustering.md) for full deployment recipes.

---

## Environment variables (summary)

| Env | Effect |
|-----|--------|
| `VECTORDB_CONFIG` | Path to TOML |
| `VECTORDB_LISTEN` | Override `[server].listen` |
| `VECTORDB_DATA_DIR` | Override `[storage].data_dir` |
| `VECTORDB_NODE_ID` | Override `[cluster].node_id` |
| `VECTORDB_API_KEYS` | Comma-separated keys (gateway only) |
| `VECTORDB_GRPC` | Gateway upstream gRPC URL |
| `VECTORDB_HTTP` | Gateway HTTP listen address |
| `VECTORDB_TLS_CERT` / `VECTORDB_TLS_KEY` | Gateway TLS |

---

## Tuning

| Goal | Knob |
|------|------|
| Higher recall | Increase `ef_search`, `ef_construction`, `m` |
| Faster writes | `sync_wal = false` (less durability) |
| Filtered queries | Declare `payload_indexes` |
| Memory pressure | Enable `scalar_quantization`, increase shard_count |
| Replicated writes | `[raft]` with 3 peers |

HNSW parameters are per-collection, set via `CollectionSpec` (REST: `m`,
`ef_construction`, `ef_search` fields).
