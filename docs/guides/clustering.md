# Clustering & replication

VectorDB scales two orthogonal axes:

1. **Horizontal sharding** — split a logical collection across N shards via
   consistent hash on `point.id`.
2. **Replication** — each shard can be a Raft group of `R` nodes (typically 3).

Total node count = `N · R`. A router fans queries out to one node per shard,
the leader of that shard handles writes.

---

## Topology overview

```mermaid
flowchart LR
    Client --> GW[gateway :8080]
    GW --> RT[router :6333]
    subgraph Shard 0 (Raft)
      L0[leader :6334]
      F0a[follower :6344]
      F0b[follower :6354]
      L0 --- F0a
      L0 --- F0b
    end
    subgraph Shard 1 (Raft)
      L1[leader :6335]
      F1a[follower :6345]
      F1b[follower :6355]
      L1 --- F1a
      L1 --- F1b
    end
    RT --> L0
    RT --> L1
```

---

## Sharding

- Hash function: `xxhash64(point.id) % shard_count`.
- Each data node declares `shard_id`; the router’s `[[cluster.nodes]]`
  list maps shard ids to gRPC endpoints.
- All collection metadata is **shared** (created on every shard); points are
  **partitioned**.

### Two-shard example (no replication)

`config/node-0.toml`:
```toml
[server]
listen = "0.0.0.0:6334"
[storage]
data_dir = "./data/node-0"
[cluster]
node_id = "node-0"
role = "data"
shard_count = 2
shard_id = 0
```

`config/node-1.toml`:
```toml
[server]
listen = "0.0.0.0:6335"
[storage]
data_dir = "./data/node-1"
[cluster]
node_id = "node-1"
role = "data"
shard_count = 2
shard_id = 1
```

`config/router.toml` (already shipped):
```toml
[server]
listen = "0.0.0.0:6333"
[cluster]
node_id = "router-1"
role = "router"
shard_count = 2
[[cluster.nodes]]
id = "shard-0"
grpc = "http://127.0.0.1:6334"
shard_ids = [0]
[[cluster.nodes]]
id = "shard-1"
grpc = "http://127.0.0.1:6335"
shard_ids = [1]
```

Run:
```bash
cargo run -p vectordb-server -- --config config/node-0.toml &
cargo run -p vectordb-server -- --config config/node-1.toml &
cargo run -p vectordb-server -- --config config/router.toml &

# Clients hit the router (or the gateway in front of it).
VECTORDB_GRPC=http://127.0.0.1:6333 cargo run -p vectordb-gateway
```

The router fans out search to all shards (each returns `top_k * 2`) and
merges. Upserts go to exactly one shard.

---

## Replication (Raft)

Each shard can become its own Raft group. Writes go to the leader; followers
mirror via `AppendEntries` and apply WAL entries on commit.

Shipped configs: `config/replica-1.toml`, `replica-2.toml`, `replica-3.toml`
(all `shard_count = 1, shard_id = 0`, ports `6334–6336` for data and
`7334–7336` for Raft).

```bash
cargo run -p vectordb-server -- --config config/replica-1.toml
cargo run -p vectordb-server -- --config config/replica-2.toml
cargo run -p vectordb-server -- --config config/replica-3.toml
```

After a few hundred ms one node wins election. Verify:

```bash
curl -s http://127.0.0.1:8080/health
# is_leader: true  raft_role: "leader"  leader_endpoint: "http://127.0.0.1:6334"
```

### Writes from any node

The Rust SDK auto-redirects writes:

```rust
let mut c = VectorDbClient::connect("http://127.0.0.1:6335").await?;
c.upsert("docs", vec![/* ... */]).await?;
// On follower → status FAILED_PRECONDITION + x-vectordb-leader → SDK retries leader
```

The error metadata `x-vectordb-leader: http://host:port` is what other SDKs
should look for. Manually:

```bash
grpcurl ... # FAILED_PRECONDITION  ... metadata: x-vectordb-leader: http://127.0.0.1:6334
```

### Multi-shard + Raft

Combine: spin up `R = 3` replicas per shard with distinct `shard_id` values
and disjoint Raft `node_id` numbers, then point the router at one Raft node
(typically the current leader, or any — the SDK redirects).

For 2 shards × 3 replicas each you’ll have 6 data nodes plus 1 router and
1 gateway.

---

## Failure handling

| Failure | Behaviour |
|---------|-----------|
| Leader crash | One follower wins election in `~election_timeout_ms`. Writes resume. |
| Follower crash | Quorum still met; leader keeps committing; follower catches up on restart. |
| Router crash | Restart; routers are stateless. Multiple routers can run in parallel. |
| Network partition | Minority side rejects writes; majority side stays available. |
| Disk loss | Restore from latest snapshot (`snapshots/snap-<ts>/`) — see [`operations.md`](operations.md). |

---

## Adding a node

(Manual today; live membership change is on the future track.)

1. Create the new replica config with the next free `node_id` and unique
   `listen` / `data_dir`.
2. Add the new peer to **every** existing replica’s `[[raft.peers]]` list and
   restart them rolling.
3. Start the new node — it will catch up via WAL replay then receive ongoing
   `AppendEntries`.

For a brand-new shard, also update the router’s `[[cluster.nodes]]` and bump
`shard_count` everywhere. After this a re-key (rebalance) is needed; in this
release the recommended path is:

- snapshot all shards
- bring up the new layout
- re-ingest from a backup or a re-shard tool you write against the gRPC API

---

## Read scaling

For read-heavy workloads:

- Run multiple **routers** in front of the same shards.
- Followers serve **reads** today (search/get); only writes need the leader.
- Add more replicas per shard for higher read throughput.

---

## Picking shard count

| Estimated points / shard | RAM per shard (approx) |
|--------------------------|------------------------|
| 1 M × 1536 dim f32 | ~6 GB graph + payloads |
| 10 M × 768 dim f32 | ~30 GB |
| 100 M × 384 dim quantized | ~40 GB |

Aim for shards that fit comfortably in RAM with overhead for HNSW neighbour
lists. Sharding is cheap; over-shard slightly to leave room for growth.
