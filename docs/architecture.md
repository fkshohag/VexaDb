# Architecture & Design

VectorDB is a Rust workspace of focused crates, deployable as one of three node
roles (`data`, `router`, `all_in_one`) plus an optional REST `gateway` and a
Prometheus metrics endpoint. This page describes how data flows through the
system and how each piece fits together.

> Diagrams use [Mermaid](https://mermaid.js.org/). GitHub, VS Code, and most
> Markdown viewers render them inline.

---

## 1. High-level system

```mermaid
flowchart LR
    subgraph Clients
      A1[Python / Node / Go / Java SDK]
      A2[Rust SDK / CLI]
      A3[curl / browser]
    end

    A1 -- HTTP/JSON --> GW[vectordb-gateway<br/>:8080]
    A3 -- HTTP/JSON --> GW
    A2 -- gRPC --> RT[vectordb-server<br/>router :6333]
    GW -- gRPC --> RT

    subgraph "Cluster (sharded)"
      RT -- "shard hash<br/>fan-out" --> S0[Shard 0<br/>data node :6334]
      RT --> S1[Shard 1<br/>data node :6335]
    end

    subgraph "Shard 0 (Raft group, optional)"
      S0 --- F0a[Follower :6334b]
      S0 --- F0b[Follower :6334c]
    end

    S0 -. metrics .-> P[(Prometheus :9090)]
    GW -. metrics .-> P
```

- **Gateway** translates REST → gRPC and adds API-key auth.
- **Router** owns the shard map, fans out queries, and merges top-k results.
- **Data nodes** own the HNSW index, WAL, RocksDB metadata, and (optionally)
  a Raft group for replication.
- **Prometheus** scrapes `/metrics` from server and gateway.

---

## 2. Crates & responsibilities

```mermaid
flowchart TB
    core[vectordb-core<br/>HNSW · distance · sparse · BM25 · fusion · SIMD]
    storage[vectordb-storage<br/>WAL · RocksDB · payload index · snapshots]
    repl[vectordb-replication<br/>Raft per shard]
    cluster[vectordb-cluster<br/>consistent hash · shard map]
    proto[vectordb-proto<br/>gRPC service definitions]
    auth[vectordb-auth<br/>API-key validation]
    server[vectordb-server<br/>node binary]
    router[vectordb-router<br/>fan-out + merge]
    client[vectordb-client<br/>Rust SDK]
    cli[vectordb-cli<br/>admin CLI]
    gateway[vectordb-gateway<br/>HTTP/JSON]
    bench[vectordb-bench<br/>perf harness]

    storage --> core
    repl --> storage
    server --> storage
    server --> repl
    server --> router
    server --> auth
    server --> proto
    router --> proto
    router --> cluster
    client --> proto
    client --> auth
    cli --> client
    gateway --> client
    gateway --> auth
    bench --> storage
```

| Crate | Role |
|-------|------|
| `vectordb-core` | Vectors, metrics, HNSW, sparse, BM25, fusion, SIMD, scalar quant |
| `vectordb-storage` | WAL, RocksDB metadata, payload index, snapshots, search orchestration |
| `vectordb-replication` | Raft node per shard |
| `vectordb-cluster` | Consistent-hash ring, shard router |
| `vectordb-proto` | Protobuf + tonic generated stubs |
| `vectordb-auth` | Shared `Bearer` / `x-api-key` checking |
| `vectordb-server` | Process binary (data, router, all-in-one) |
| `vectordb-router` | Fan-out + merge top-k as a `VectorService` impl |
| `vectordb-client` | Async gRPC SDK with leader auto-redirect |
| `vectordb-cli` | `vectordb` admin CLI |
| `vectordb-gateway` | Axum REST/JSON façade |
| `vectordb-bench` | Quick performance harness |

---

## 3. Storage layout

```mermaid
flowchart LR
    M[CollectionEngine<br/>RwLock&lt;HashMap&gt;] --> C0[Collection &quot;docs&quot;]
    M --> C1[Collection &quot;kb&quot;]

    C0 --> H0[HnswIndex<br/>in-memory]
    C0 --> P0[payloads<br/>HashMap&lt;id,Value&gt;]
    C0 --> PI0[PayloadIndexes<br/>keyword·numeric·bool]
    C0 --> S0[SparseInvertedIndex]
    C0 --> B0[Bm25Index]
    C0 --> Q0[ScalarQuantizer · 8-bit codes]

    M --> WAL[(WAL · wal.log<br/>append-only)]
    M --> RDB[(RocksDB · meta/<br/>collection configs)]
    M --> SNAP[(snapshots/<br/>snap-&lt;ts&gt;/)]
```

- The HNSW graph and payloads live in **memory**; the WAL + RocksDB give
  durability and crash recovery on restart.
- Snapshots are **filesystem copies** of `wal.log` + `meta/` taken atomically
  and labelled by timestamp.

---

## 4. Write path

```mermaid
sequenceDiagram
    actor Client
    participant GW as Gateway (REST)
    participant SVR as Server (gRPC)
    participant RAFT as Raft (optional)
    participant ENG as CollectionEngine
    participant WAL as WAL (wal.log)
    participant HNSW as HNSW + indexes

    Client->>GW: POST /v1/collections/:c/upsert {points}
    GW->>SVR: Upsert(points) [Bearer/x-api-key]
    alt Replication enabled
        SVR->>RAFT: propose(WalEntry::Upsert)
        RAFT->>RAFT: replicate to quorum<br/>via AppendEntries
        RAFT->>ENG: apply on commit
    else No replication
        SVR->>ENG: commit_entry(WalEntry::Upsert)
    end
    ENG->>WAL: append(entry) + fsync (sync_wal=true)
    ENG->>HNSW: insert vector + payload + sparse + BM25
    ENG-->>SVR: Ok
    SVR-->>GW: UpsertResponse{upserted:N}
    GW-->>Client: 200 {"upserted": N}
```

- **Leader-only writes**: in Raft mode, followers return `not leader` and the
  client SDK auto-redirects using the `x-vectordb-leader` metadata.
- **WAL first, index second**: entries are durable before HNSW mutation, so
  crashes replay cleanly.

---

## 5. Read path (search)

```mermaid
sequenceDiagram
    actor Client
    participant GW as Gateway
    participant RT as Router
    participant S0 as Shard 0 (data)
    participant S1 as Shard 1 (data)

    Client->>GW: POST /v1/collections/:c/search
    GW->>RT: Search(query, top_k=K)
    par Fan-out
        RT->>S0: Search(K * 2)
        RT->>S1: Search(K * 2)
    end
    S0-->>RT: hits[0..2K]
    S1-->>RT: hits[0..2K]
    RT->>RT: merge_top_k → K
    RT-->>GW: SearchResponse
    GW-->>Client: [{id, score}, ...]
```

Per shard, search resolves dense / sparse / BM25 / hybrid based on the request:

```mermaid
flowchart TB
    Q[SearchRequest] --> M{search_mode}
    M -->|dense| D[HNSW + ef_search]
    M -->|sparse| S[Inverted index]
    M -->|bm25| B[BM25 over payload text]
    M -->|hybrid_rrf| HR[RRF fusion of dense + sparse + BM25]
    M -->|hybrid_weighted| HW[Weighted fusion · alpha]
    D --> F[filter_json + payload-index pushdown]
    S --> F
    B --> F
    HR --> F
    HW --> F
    F --> O[top-K]
```

For filtered search the engine picks the cheaper plan:

- If a payload-indexed filter is **selective** (≤ 50k candidate ids), brute-force
  distance over the candidate set.
- Otherwise oversearch HNSW (`k · 16`, capped) and post-filter.

---

## 6. Sharding

```mermaid
flowchart LR
    P[Point id &quot;abc-123&quot;] --> H[xxhash64]
    H --> R[ring · shard_count]
    R --> R0[shard 0]
    R --> R1[shard 1]
    R --> RN[shard N-1]
```

- Sharding is **consistent-hash** over `point.id` (`xxhash64 % shard_count`).
- The router holds `[[cluster.nodes]]` with each node’s `grpc` and the shards
  it owns.
- Each shard is independent; cross-shard traffic only happens at the router
  during fan-out search.

---

## 7. Replication (Raft)

```mermaid
sequenceDiagram
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    L->>L: append WalEntry to local log
    par AppendEntries
        L->>F1: AppendEntries(term, prev, [entry])
        L->>F2: AppendEntries(term, prev, [entry])
    end
    F1-->>L: ok (match_index++)
    F2-->>L: ok (match_index++)
    Note over L: quorum reached → commit_index++
    L->>L: apply WalEntry to engine
    par Commit notify
        L->>F1: AppendEntries(commit_index)
        L->>F2: AppendEntries(commit_index)
    end
    F1->>F1: apply WalEntry locally
    F2->>F2: apply WalEntry locally
```

Election timeouts and heartbeat intervals are configured under `[raft]` in the
node TOML. Each shard is its own Raft group; for `shard_count = N` and
replication factor `R`, you run `N · R` data nodes.

The leader’s gRPC endpoint is advertised in `HealthResponse.leader_endpoint`
and in error metadata `x-vectordb-leader`. SDKs use this to redirect writes.

---

## 8. Bulk import & WAL compaction

```mermaid
sequenceDiagram
    actor Client
    participant SVR as Server
    participant ENG as Engine
    participant WAL as WAL

    Client->>SVR: BulkUpsert(points, chunk_size=500)
    loop chunks of 500
        SVR->>ENG: bulk_upsert(chunk)
        ENG->>WAL: append(WalEntry::BulkUpsert)
        ENG->>ENG: apply each point
    end
    SVR-->>Client: {upserted: N}

    Client->>SVR: CompactWal{snapshot_first:true}
    SVR->>ENG: snapshot_and_compact_wal()
    ENG->>ENG: copy data dir → snapshots/snap-<ts>
    ENG->>ENG: rewrite WAL from in-memory state
    ENG->>WAL: truncate + replay current state
    ENG->>WAL: append(WalEntry::Checkpoint)
    SVR-->>Client: stats {before, after, snapshot}
```

For client streaming, `ImportStream` accepts `ImportChunk`s with a `finalize`
flag to flush remaining buffer and ack one cumulative count.

---

## 9. Online reindex

```mermaid
sequenceDiagram
    actor Client
    participant SVR as Server
    participant ENG as Engine
    participant OLD as Old HNSW
    participant NEW as New HNSW

    Client->>SVR: ReindexCollection(name)
    SVR->>ENG: reindex_collection(name)
    ENG->>OLD: iter_points() → (id, vector)*
    ENG->>NEW: insert each (id, vector)
    ENG->>ENG: swap NEW into CollectionState
    SVR-->>Client: {vectors_reindexed: N}
```

Reindex is **online**: reads continue against the old index until the swap
(under a single write lock) replaces it.

---

## 10. Hybrid search internals

```mermaid
flowchart TB
    DQ[dense query] --> HNSW
    SQ[sparse query] --> SIDX[sparse inverted index]
    TQ[text query] --> BM25
    HNSW --> Hd[hits_d]
    SIDX --> Hs[hits_s]
    BM25 --> Hb[hits_b]
    Hd --> FU{mode}
    Hs --> FU
    Hb --> FU
    FU -->|hybrid_rrf| RRF[RRF fusion · 1/(60+rank)]
    FU -->|hybrid_weighted| WW[α·dense + (1-α)·lexical]
    RRF --> R[top-K]
    WW --> R
```

See [`guides/hybrid-search.md`](guides/hybrid-search.md) for usage.

---

## 11. Memory & disk model

| Layer | Lives in | Persisted? | Recovery |
|-------|---------|------------|----------|
| HNSW graph | RAM | ✗ | Replayed from WAL |
| Payloads | RAM | ✗ | Replayed from WAL |
| Sparse / BM25 indexes | RAM | ✗ | Replayed from WAL |
| WAL (`wal.log`) | Disk | ✓ (append-only) | Read on startup |
| Collection metadata | RocksDB (`meta/`) | ✓ | Read on startup |
| Snapshots | `snapshots/snap-<ts>/` | ✓ | Manual restore |
| Quantized codes | RAM | rebuilt | After enabling on a collection |

To restore from a snapshot, copy `snap-<ts>/wal.log` and `snap-<ts>/meta/` into
the data directory and start the server.

---

## 12. Deployment topologies

### Single node (dev / small)
```mermaid
flowchart LR
    Client --> GW[gateway :8080]
    GW --> N[all_in_one :6334]
```

### Sharded (no replication)
```mermaid
flowchart LR
    Client --> GW[gateway]
    GW --> RT[router :6333]
    RT --> S0[shard 0 :6334]
    RT --> S1[shard 1 :6335]
```

### Sharded + Raft (HA)
```mermaid
flowchart LR
    Client --> GW
    GW --> RT
    subgraph Shard 0 group
      RT --> L0[leader :6334]
      L0 --- F0a[follower :6344]
      L0 --- F0b[follower :6354]
    end
    subgraph Shard 1 group
      RT --> L1[leader :6335]
      L1 --- F1a[follower :6345]
      L1 --- F1b[follower :6355]
    end
```

The router or any SDK can talk directly to a leader; followers reject writes
and return the leader endpoint for redirect.

---

## 13. Failure handling

| Failure | Behaviour |
|---------|-----------|
| Single node crash | WAL replay restores in-memory state on restart |
| Leader crash (Raft) | Follower wins election; SDK redirects writes to new leader |
| Disk corruption | Restore from latest snapshot in `snapshots/` |
| Router crash | Restart; routers are stateless |
| Gateway crash | Restart; gateway is stateless |

---

## 14. Where to read the code

- HNSW algorithm: `crates/vectordb-core/src/hnsw.rs`
- SIMD distance: `crates/vectordb-core/src/simd.rs`
- Filter DSL: `crates/vectordb-core/src/filter.rs`
- Engine + WAL: `crates/vectordb-storage/src/{engine.rs, wal.rs}`
- Snapshots: `crates/vectordb-storage/src/snapshot.rs`
- Hybrid search: `crates/vectordb-storage/src/search.rs`
- Raft: `crates/vectordb-replication/src/{node.rs, rpc.rs}`
- Router fan-out: `crates/vectordb-router/src/service.rs`
- Server endpoints: `crates/vectordb-server/src/service.rs`
- Gateway routes: `crates/vectordb-gateway/src/main.rs`
