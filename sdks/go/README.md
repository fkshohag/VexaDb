# VexaDb Go SDK

Official Go client for the VexaDb HTTP gateway. API shape follows the Milvus Go SDK v2
option-builder pattern (`entity` + `vexaclient`). Reference docs live under
`milvus-sdk-go/` (not a dependency).

## Packages

| Package | Purpose |
|---------|---------|
| `vexaclient` | REST client (collections, upsert, search, query, admin, management) |
| `entity` | Schemas, vectors, metrics, `ResultSet` columns, load/compaction/segment types |
| `index` | Milvus-parity `index.Index` constructors (`NewHNSWIndex`, `NewInvertedIndex`, …) |
| `rag` | Chunking, ingest, and retrieval helpers |

## Install

```bash
cd sdks/go
go get github.com/vectordb/vectordb/sdks/go/vexaclient
```

Local development:

```go
replace github.com/vectordb/vectordb/sdks/go => ../sdks/go
```

## Collection management (Milvus v2.6 parity)

```go
// Create with rich options
schema := entity.NewSchema().
    WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(64)).
    WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(1536))

_ = cli.CreateCollection(ctx, vexaclient.NewCreateCollectionOption("books", schema).
    WithMetricType(entity.COSINE).
    WithProperty("collection.ttl.seconds", 86400).
    WithConsistencyLevel(entity.ClStrong))

// Read-side: option signatures or plain string
has, _ := cli.HasCollection(ctx, vexaclient.NewHasCollectionOption("books"))
col, _ := cli.DescribeCollection(ctx, vexaclient.NewDescribeCollectionOption("books"))
stats, _ := cli.GetCollectionStats(ctx, vexaclient.NewGetCollectionStatsOption("books"))

// Rename + properties
_ = cli.RenameCollection(ctx, vexaclient.NewRenameCollectionOption("books", "library"))
_ = cli.AlterCollectionProperties(ctx,
    vexaclient.NewAlterCollectionPropertiesOption("library").
        WithProperty("mmap.enabled", true))
_ = cli.DropCollectionProperties(ctx,
    vexaclient.NewDropCollectionPropertiesOption("library", "mmap.enabled"))

// Aliases
_ = cli.CreateAlias(ctx, vexaclient.NewCreateAliasOption("library", "books_v2"))
_ = cli.AlterAlias(ctx,  vexaclient.NewAlterAliasOption("books_v2", "library_v3"))
a, _ := cli.DescribeAlias(ctx, vexaclient.NewDescribeAliasOption("books_v2"))
list, _ := cli.ListAliases(ctx, vexaclient.NewListAliasesOption("")) // all
_ = cli.DropAlias(ctx, vexaclient.NewDropAliasOption("books_v2"))

// Replica / shard topology
replicas, _ := cli.DescribeReplica(ctx, vexaclient.NewDescribeReplicaOption("library"))
_ = a; _ = col; _ = stats; _ = has; _ = list; _ = replicas
```

## Database management (Milvus v2.6 parity)

VexaDb scopes collections by database (defaulting to `default`). The Go SDK
mirrors Milvus's `Database/*` surface:

```go
// CRUD
_ = cli.CreateDatabase(ctx, vexaclient.NewCreateDatabaseOption("analytics").
    WithProperty("database.replica.number", 2))

names, _ := cli.ListDatabase(ctx, vexaclient.NewListDatabaseOption())
db, _   := cli.DescribeDatabase(ctx, vexaclient.NewDescribeDatabaseOption("analytics"))

// Properties
_ = cli.AlterDatabaseProperties(ctx,
    vexaclient.NewAlterDatabasePropertiesOption("analytics").
        WithProperty("tier", "hot"))
_ = cli.DropDatabaseProperties(ctx,
    vexaclient.NewDropDatabasePropertiesOption("analytics", "tier"))

// Drop. The server rejects a drop on a non-empty database; pass
// WithForce(true) to cascade-drop all child collections + aliases.
_ = cli.DropDatabase(ctx, vexaclient.NewDropDatabaseOption("analytics").WithForce(true))

// Switching the active database — UseDatabase is the Milvus v2.6 name,
// UsingDatabase is the v2.5 alias; both update the x-vexa-db header used
// on subsequent requests.
_ = cli.UseDatabase(ctx, vexaclient.NewUseDatabaseOption("analytics"))

_ = names; _ = db
```

Collections are unique per-database. `cli.UseDatabase("analytics")` followed
by `cli.CreateCollection(ctx, NewSimpleCreateCollectionOption("docs", 128))`
creates `analytics/docs`; the same call after `UseDatabase("research")`
creates a separate `research/docs`.

## Management (Milvus v2.6 parity)

The Go SDK mirrors Milvus's `Management/*` surface: index lifecycle,
load/release, flush, compaction, and persistent-segment introspection.
VexaDb keeps collections memory-resident, so load/release are no-op
successes and `GetLoadState` always reports `Loaded`. Flush fsyncs the
WAL inline; Compact returns a job ID you can poll.

```go
import (
    "github.com/vectordb/vectordb/sdks/go/entity"
    "github.com/vectordb/vectordb/sdks/go/index"
    "github.com/vectordb/vectordb/sdks/go/vexaclient"
)

// ---- Index lifecycle ---------------------------------------------------

// Vector index: VexaDb auto-builds HNSW at CreateCollection time. Calling
// CreateIndex on the vector field rebuilds it (Milvus parity).
hnsw := index.NewHNSWIndex(index.COSINE, 16, 200)
task, _ := cli.CreateIndex(ctx,
    vexaclient.NewCreateIndexOption("docs", "vector", hnsw))
_ = task.Await(ctx)

// Scalar index: maps to VexaDb's payload-index engine path.
inv := index.NewInvertedIndex()
_, _ = cli.CreateIndex(ctx,
    vexaclient.NewCreateIndexOption("docs", "category", inv).
        WithIndexName("cat_idx"))

names, _ := cli.ListIndexes(ctx, vexaclient.NewListIndexOption("docs"))
desc, _  := cli.DescribeIndex(ctx, vexaclient.NewDescribeIndexOption("docs", "cat_idx"))
_ = desc.State // entity.IndexStateFinished

_ = cli.AlterIndexProperties(ctx,
    vexaclient.NewAlterIndexPropertiesOption("docs", "cat_idx").
        WithProperty("mmap.enabled", true))
_ = cli.DropIndexProperties(ctx,
    vexaclient.NewDropIndexPropertiesOption("docs", "cat_idx", "mmap.enabled"))
_ = cli.DropIndex(ctx, vexaclient.NewDropIndexOption("docs", "cat_idx"))

// ---- Load / Release ---------------------------------------------------

load, _ := cli.LoadCollection(ctx,
    vexaclient.NewLoadCollectionOption("docs").WithReplica(2))
_ = load.Await(ctx) // Always returns immediately on VexaDb.

state, _ := cli.GetLoadState(ctx, vexaclient.NewGetLoadStateOption("docs"))
_ = state.State // entity.LoadStateLoaded

// Refresh = trigger reindex so freshly inserted data is searchable.
refresh, _ := cli.RefreshLoad(ctx, vexaclient.NewRefreshLoadOption("docs"))
_ = refresh.Await(ctx)

_ = cli.ReleaseCollection(ctx, vexaclient.NewReleaseCollectionOption("docs"))

// LoadPartitions / ReleasePartitions accept partition names for
// source compatibility but VexaDb operates on the whole collection.
_, _ = cli.LoadPartitions(ctx, vexaclient.NewLoadPartitionsOption("docs", "p1", "p2"))

// ---- Flush / Compact / Segments ---------------------------------------

flushTask, _ := cli.Flush(ctx, vexaclient.NewFlushOption("docs"))
_ = flushTask.Await(ctx)
segIDs, _, flushTs, _ := flushTask.GetFlushStats()
_ = segIDs; _ = flushTs

id, _ := cli.Compact(ctx, vexaclient.NewCompactOption("docs"))
info, _ := cli.GetCompactionState(ctx, vexaclient.NewGetCompactionStateOption(id))
_ = info.State // entity.CompactionStateCompleted

segs, _ := cli.GetPersistentSegmentInfo(ctx,
    vexaclient.NewGetPersistentSegmentInfoOption("docs"))
for _, s := range segs {
    _ = s.Flushed() // true for snapshot-captured segments
}
```

### Mapping notes

- **Vector index types**: `HNSW`, `AUTOINDEX`, and `FLAT` map to VexaDb's
  always-on HNSW. IVF / DiskANN / GPU constructors compile against the
  SDK but the gateway rejects them with `400 Bad Request`.
- **Scalar index types**: `INVERTED` / `BITMAP` / `TRIE` → VexaDb's
  `keyword` payload index; `STL_SORT` → `numeric`; sparse → `sparse`.
- **Load**: always `Loaded`/`100%` for existing collections.
- **Partitions**: see the [Partitions](#partitions-milvus-v26-parity) section
  below. `LoadPartitions`/`ReleasePartitions` from the Management surface
  still alias to whole-collection load/release because VexaDb is
  always-resident.
- **Compaction IDs**: minted from epoch-ms so they sort chronologically;
  state lookups only resolve on the originating shard (single-shard
  cluster: always works).

## Partitions (Milvus v2.6 parity)

VexaDb implements partitions as **logical subsets of a collection**: every
collection ships with the built-in `_default` partition, and additional
partitions are tracked in `CollectionConfig::partitions`. Upserts into a
non-default partition stamp a reserved `_partition` payload field on the
point, so partition filtering reuses the existing filter DSL and
`DropPartition` is a true cascade that deletes the partition's data.

```go
import (
    "github.com/vectordb/vectordb/sdks/go/vexaclient"
)

// Lifecycle
_ = cli.CreatePartition(ctx,
    vexaclient.NewCreatePartitionOption("docs", "hot"))

ok, _ := cli.HasPartition(ctx,
    vexaclient.NewHasPartitionOption("docs", "hot"))
_ = ok

names, _ := cli.ListPartitions(ctx,
    vexaclient.NewListPartitionOption("docs"))
// names => ["_default", "hot"]

stats, _ := cli.GetPartitionStats(ctx,
    vexaclient.NewGetPartitionStatsOption("docs", "hot"))
// stats["row_count"], stats["partition_name"], stats["collection"]

// DropPartition is a cascade: it deletes every point tagged with `hot`
// in the same MetaOp, then removes the name from the collection config.
_ = cli.DropPartition(ctx,
    vexaclient.NewDropPartitionOption("docs", "hot"))
```

### Mapping notes

- The `_default` partition is reserved and cannot be dropped; the server
  returns `400 Bad Request` if you try.
- Existing data inserted **before** the partition was created keeps its
  original payload (no automatic backfill of `_partition`). Such points
  count toward the `_default` partition's `row_count`.
- Upserts that target a partition automatically inject
  `payload._partition = "<name>"`. Searches/queries can scope to a
  partition by adding `_partition == "<name>"` to the filter expression
  (the existing filter DSL already understands this field).
- Upserting into an unknown partition returns
  `NotFound: partition not found: <name>` from the gateway.
- RBAC privileges (`CreatePartition`, `DropPartition`, `DescribePartition`,
  `ShowPartitions`, `GetPartitionStatistics`) are **collection-scoped** —
  built-in roles `read_write` and `read_only` are already granted the
  appropriate subset.

## Resource groups (Milvus v2.6 parity)

VexaDb implements resource groups as a **persistent cluster-wide registry**:
each group stores a `ResourceGroupConfig` (node requests/limits, transfer
policies, node label filters) in RocksDB and replicates changes through
the WAL/Raft path. The built-in `__default_resource_group` is auto-seeded
on first open and cannot be dropped.

This is **registry-only** mode: `Requests`/`Limits` are stored verbatim and
returned by `DescribeResourceGroup`, but the scheduler does **not** yet
enforce node capacity or move shards between groups. `TransferReplica` is a
validated no-op success (both groups must exist); `DescribeReplica`
synthesizes a single logical replica from cluster topology.

```go
import (
    "github.com/vectordb/vectordb/sdks/go/entity"
    "github.com/vectordb/vectordb/sdks/go/vexaclient"
)

// Create with full config (Milvus parity)
cfg := &entity.ResourceGroupConfig{
    Requests: entity.ResourceGroupLimit{NodeNum: 2},
    Limits:   entity.ResourceGroupLimit{NodeNum: 4},
    NodeFilter: entity.ResourceGroupNodeFilter{
        NodeLabels: map[string]string{"zone": "a"},
    },
}
_ = cli.CreateResourceGroup(ctx,
    vexaclient.NewCreateResourceGroupOption("hot").WithConfig(cfg))

// Or use the shortcut helpers
_ = cli.CreateResourceGroup(ctx,
    vexaclient.NewCreateResourceGroupOption("warm").
        WithNodeRequest(1).WithNodeLimit(3))

names, _ := cli.ListResourceGroups(ctx,
    vexaclient.NewListResourceGroupsOption())
// names => ["__default_resource_group", "hot", "warm"]

rg, _ := cli.DescribeResourceGroup(ctx,
    vexaclient.NewDescribeResourceGroupOption("hot"))
// rg.Config.Requests.NodeNum, rg.NumAvailableNode, ...

_ = cli.UpdateResourceGroup(ctx,
    vexaclient.NewUpdateResourceGroupOption("hot", cfg))

_ = cli.DropResourceGroup(ctx,
    vexaclient.NewDropResourceGroupOption("warm"))

// Replica introspection (collection-scoped REST)
replicas, _ := cli.DescribeReplica(ctx,
    vexaclient.NewDescribeReplicaOption("docs"))
// replicas[0].Placement[] => {ShardID, NodeID, NodeAddress}
// replicas[0].ResourceGroupName => "__default_resource_group"

// TransferReplica validates both groups exist, then returns success
_ = cli.TransferReplica(ctx,
    vexaclient.NewTransferReplicaOption("docs", "hot", "cold", 1).
        WithDBName("default"))
```

### Mapping notes

| Milvus API | VexaDb behavior |
|------------|-----------------|
| `CreateResourceGroup` | Persists `MetaOp::CreateResourceGroup` via Raft |
| `DropResourceGroup` | Rejects `__default_resource_group` |
| `UpdateResourceGroup` | Full config replace (Milvus semantics) |
| `ListResourceGroups` | Sorted name list; always includes default |
| `DescribeResourceGroup` | Returns config + counters (`num_available_node` is `0` until topology wiring) |
| `TransferReplica` | No-op success after validating both RGs exist |
| `DescribeReplica` | One synthetic replica per collection from shard primaries |

- REST routes: `GET/POST /v1/resource-groups`, `GET/PATCH/DELETE
  /v1/resource-groups/:name`, `GET /v1/collections/:name/replicas`,
  `POST /v1/admin/transfer-replica`.
- RBAC privileges are **global** (`CreateResourceGroup`, `DropResourceGroup`,
  `DescribeResourceGroup`, `ListResourceGroups`, `UpdateResourceGroup`,
  `TransferReplica`, `DescribeReplica`). Built-in `read_write` grants all;
  `read_only` grants describe/list/describe-replica only.
- `DescribeReplica` with an **empty** collection name still falls back to
  the legacy cluster-status derivation (backward compatible).

## Vector module (Milvus v2.6 parity)

The SDK mirrors Milvus's
[`Vector` namespace](milvus-sdk-go/v2.6.x/Vector) — Insert / Upsert /
Delete / Get / Query / Search / HybridSearch / QueryIterator /
SearchIterator / RunAnalyzer. Every operation accepts the same option
shape as Milvus, with one or two VexaDb-specific notes called out below.

### Insert / Upsert

```go
res, err := cli.Upsert(ctx,
    vexaclient.NewColumnBasedInsertOption("docs").
        WithIDs([]string{"a", "b"}).
        WithFloatVectorColumn("vector", 4, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}).
        WithVarcharColumn("color", []string{"red", "blue"}).
        WithPartition("hot").
        WithPartialUpdate(true))
fmt.Println(res.Upserted, res.IDs)
```

Supported column builders (all map to VexaDb's single dense float32
storage on the server side):

| Method | Notes |
|--------|-------|
| `WithVarcharColumn` / `WithInt64Column` / `WithFloatColumn` / `WithBoolColumn` | Stored verbatim as JSON payload values |
| `WithInt8Column` / `WithInt16Column` / `WithInt32Column` | Widened to int64 in the payload |
| `WithFloatVectorColumn(name, dim, data)` | Primary dense vector path |
| `WithFloat16VectorColumn` / `WithBFloat16VectorColumn` | Accept float32 representations (Milvus parity; VexaDb stores float32 internally) |
| `WithBinaryVectorColumn` | One bit per dimension expanded to `0.0/1.0` floats |
| `WithInt8VectorColumn` | Widened element-wise to float32 |
| `WithSparseColumn` | Per-row optional `*entity.SparseVector` |
| `WithPartition(name)` | Tags every point's payload with `_partition: <name>` |
| `WithPartialUpdate(true)` | Honored by `Upsert` (no-op on `Insert`) |

### Delete

```go
del, err := cli.Delete(ctx,
    vexaclient.NewDeleteOption("docs").
        WithExpr("color == 'red'").
        WithPartition("hot"))
fmt.Println(del.DeleteCount)

// Or by typed IDs:
cli.Delete(ctx, vexaclient.NewDeleteOption("docs").
    WithInt64IDs("id", []int64{1, 2, 3}))
```

Delete supports the union of `IDs`, `WithExpr`, and `WithPartition`. The
gateway parses the expression into a `Filter` and walks the local index,
emitting one WAL `Delete` per matching point so replication and
snapshots stay consistent.

### Get / Query

`Get` is now a thin wrapper around `Query` with primary-key lookup:

```go
rs, err := cli.Get(ctx,
    vexaclient.NewQueryOption("docs").
        WithStringIDs("id", []string{"a", "b"}).
        WithOutputFields("color"))

// Or by filter:
rs2, err := cli.Query(ctx,
    vexaclient.NewQueryOption("docs").
        WithFilter("color == 'red'").
        WithLimit(50).
        WithPartitions("hot").
        WithConsistencyLevel("Strong").
        WithTemplateParam("c", "red"))
```

### Search

Single-vector ANN with the full Milvus option surface:

```go
hits, err := cli.Search(ctx,
    vexaclient.NewSearchOption("docs", 10, []entity.Vector{entity.FloatVector{1, 0, 0, 0}}).
        WithFilter("color == 'red'").
        WithOutputFields("color").
        WithPayload(true).
        WithPartitions("hot").
        WithConsistencyLevel("Strong").
        WithOffset(10).
        WithGroupByField("color").
        WithGroupSize(2).
        WithAnnParam(map[string]any{"ef": 64}).
        WithSearchParam("metric_type", "L2").
        WithFunctionReranker("cosine_rerank"))
```

### HybridSearch (multi-AnnRequest + reranker)

```go
dense := entity.FloatVector{0.3, -0.6, 0.1, 0.9}
sparse, _ := entity.NewSliceSparseEmbedding([]uint32{1, 21}, []float32{0.1, 0.2})
rss, err := cli.HybridSearch(ctx,
    vexaclient.NewHybridSearchOption("docs", 3,
        vexaclient.NewAnnRequest("vector", 10, dense).WithFilter("color == 'red'"),
        vexaclient.NewAnnRequest("sparse", 10, sparse),
        vexaclient.NewAnnRequest("text", 10, "hello world"),
    ).WithReranker(vexaclient.NewWeightedReranker(0.5, 0.3, 0.2)).
      WithPartitions("hot").
      WithOutputFields("color"))
```

Rerankers:

| Constructor | Behavior |
|-------------|----------|
| `NewRRFReranker()` | Reciprocal Rank Fusion (default) |
| `NewWeightedReranker(w1, w2, …)` | Per-leg min-max normalize + weighted sum |
| `NewFunctionReranker(name)` | Pass-through dedup by best per-leg score (name forwarded but not executed) |

### Iterators (server-side cursor)

```go
it, _ := cli.QueryIterator(ctx,
    vexaclient.NewQueryIteratorOption("docs").
        WithBatchSize(100).
        WithFilter("color == 'red'").
        WithOutputFields("id", "color"))
defer it.Close()
for {
    rs, err := it.Next(ctx)
    if err == io.EOF { break }
    if err != nil { log.Fatal(err) }
    ids, _ := rs.IDs()
    fmt.Println(ids)
}
```

`SearchIterator` walks ANN search results page-by-page by widening the
top-k window each call:

```go
sit, _ := cli.SearchIterator(ctx,
    vexaclient.NewSearchIteratorOption("docs", entity.FloatVector{1, 0, 0, 0}).
        WithBatchSize(50).
        WithOutputFields("color"))
defer sit.Close()
for {
    rs, err := sit.Next(ctx)
    if err == io.EOF { break }
    // …
}
```

### RunAnalyzer

```go
res, err := cli.RunAnalyzer(ctx,
    vexaclient.NewRunAnalyzerOption("Hello world, hello again!").
        WithAnalyzerParams(map[string]any{
            "tokenizer": "standard",
            "filter": []any{map[string]any{"type": "stop", "stop_words": []string{"hello"}}},
        }))
for _, t := range res[0].Tokens {
    fmt.Println(t.Text, t.StartOffset, t.EndOffset, t.Position, t.Hash)
}
```

VexaDb uses the same BM25 tokenizer that powers `bm25_text_field` search:
lowercase + alphanumeric split + optional stopword filtering.

### Mapping notes

| Milvus API | VexaDb behavior |
|------------|-----------------|
| `Insert` / `Upsert` | One WAL record per chunk; `_partition` tag injected when `WithPartition` is set |
| `WithPartialUpdate(true)` | Honored by `Upsert` (existing payload fields preserved); ignored on `Insert` |
| `Delete` with `WithExpr` | Engine resolves matching IDs and writes one WAL `Delete` per point |
| `Delete` with `WithPartition` | Restricts the scan to points tagged for that partition (and untagged points when partition == `_default`) |
| `Get` | Aliased to `Query.WithIDs` (returns a `ResultSet`); legacy `GetByID` returns the original `map[string]any` shape |
| `Search.WithGroupByField` / `WithIgnoreGrowing` | Forwarded but the server has no growing segments so they round-trip as a normal search |
| `Search.WithAnnParam` / `WithSearchParam` | Forwarded as `ann_param` / `search_params` map in the request body |
| `HybridSearch` | Router fans out to every shard with `2*limit` headroom, then re-reranks server-side with the requested strategy |
| `QueryIterator` | Backed by the new `POST /v1/collections/:name/scroll` endpoint; cursor format is `<shard_idx>|<per_shard_cursor>` |
| `SearchIterator` | SDK-side paging via progressively wider `Search.WithOffset` calls |
| `RunAnalyzer` | `POST /v1/admin/analyze`; reuses the BM25 tokenizer + optional stopword filter |

REST routes added in this module:

- `POST /v1/collections/:name/hybrid-search`
- `POST /v1/collections/:name/scroll`
- `POST /v1/admin/analyze`
- Existing `DELETE /v1/collections/:name/points` now accepts
  `{ids?, filter?, partition?}`
- Existing `POST /v1/collections/:name/upsert` now accepts an optional
  `partition` body field

RBAC privileges:

| Endpoint | Privilege | Scope |
|----------|-----------|-------|
| `/hybrid-search` | `Search` | Collection |
| `/scroll` | `Query` | Collection |
| `/admin/analyze` | `Query` | Global |
| `DELETE /points` | `Delete` | Collection |

## Client management

`New` accepts a `ClientConfig` modeled after Milvus's
[`ClientConfig`](milvus-sdk-go/v2.6.x/Client/ClientConfig.md):

| Field | Purpose |
|-------|---------|
| `Address` | Gateway URL (`host:port`, `http://…`, or `https://…`) — **required** |
| `Username` / `Password` | Auto-calls `/v1/auth/login` and stores the returned token |
| `APIKey` | API token (`tokenid:secret`) or legacy superuser key — wins over user/pass |
| `DBName` | Sent as `x-vexa-db` on every request (multi-DB forward-compat) |
| `EnableTLSAuth` | Force TLS even with a schemeless `Address` |
| `InsecureSkipVerify` | Skip cert verification for dev / self-signed |
| `DisableConn` | Skip the post-construct `/v1/version` probe (tests) |
| `Timeout` | Per-request HTTP timeout (default 60s) |
| `HTTPClient` | Override the transport (advanced) |
| `UserAgent` | Custom `User-Agent` (default `vexadb-go/<version>`) |
| `RetryRateLimit` | `{MaxRetry, MaxBackoff}` — retries on 429/503/502/504 |
| `ServerVersion` | Populated by `New` from `/v1/version` |

`GetServerVersion` and the richer `ServerInfo` mirror the Milvus Client API:

```go
cli, _ := vexaclient.New(ctx, &vexaclient.ClientConfig{Address: "http://127.0.0.1:8080"})
defer cli.Close(ctx)

ver, _ := cli.GetServerVersion(ctx, vexaclient.NewGetServerVersionOption())
info, _ := cli.ServerInfo(ctx, vexaclient.NewServerInfoOption())
fmt.Println(ver, info.GitCommit)

_ = cli.UsingDatabase(ctx, vexaclient.NewUsingDatabaseOption("analytics"))
```

## Quick example

```go
import (
    "context"
    "github.com/vectordb/vectordb/sdks/go/entity"
    "github.com/vectordb/vectordb/sdks/go/vexaclient"
)

cli, err := vexaclient.New(ctx, &vexaclient.ClientConfig{
    Address: "http://127.0.0.1:8080",
    APIKey:  os.Getenv("VECTORDB_API_KEY"),
})
defer cli.Close(ctx)

_ = cli.CreateCollection(ctx, vexaclient.NewSimpleCreateCollectionOption("books", 128))
_, _ = cli.Insert(ctx, vexaclient.NewColumnBasedInsertOption("books").
    WithIDs([]string{"a"}).
    WithFloatVectorColumn("vector", 128, [][]float32{vec}))

hits, _ := cli.Search(ctx, vexaclient.NewSearchOption("books", 5, []entity.Vector{entity.FloatVector(q)}).
    WithFilter("category == 'books'").
    WithPayload(true))
```

## RAG

```go
pipe := rag.NewPipeline(cli, "kb", 1536, myEmbed)
_, _ = pipe.Ingest(ctx, []rag.Document{{ID: "1", Text: "..."}}, true)
hits, _ := pipe.Query(ctx, "question", rag.QueryOpts{TopK: 5, Rerank: true})
```

## Authentication

```go
cli, _ := vexaclient.New(ctx, &vexaclient.ClientConfig{Address: addr})
tok, _ := cli.Login(ctx, vexaclient.NewLoginOption("root", "hunter2"))
// reconnect with the token
cli, _ = vexaclient.New(ctx, &vexaclient.ClientConfig{Address: addr, APIKey: tok.Token})

_ = cli.CreateUser(ctx, vexaclient.NewCreateUserOption("alice", "pw").WithRoles(entity.RoleReadWrite))
_ = cli.CreateRole(ctx, vexaclient.NewCreateRoleOption("auditor").WithDescription("RO + stats"))
_ = cli.GrantPrivilege(ctx, vexaclient.NewGrantPrivilegeOption(
    "auditor", entity.ObjectCollection, "Search", "*"))
_ = cli.GrantRole(ctx, vexaclient.NewGrantRoleOption("alice", "auditor"))
```

Supports `CreateUser`, `UpdatePassword`, `DropUser`, `ListUsers`,
`DescribeUser`, `CreateRole`, `DropRole`, `ListRoles`, `DescribeRole`,
`Grant/RevokeRole`, `Grant/RevokePrivilege(V2)`, privilege groups, and
`BackupRBAC` / `RestoreRBAC`.

## Examples

- `go run ./examples/quickstart`
- `go run ./examples/loadtest`
- `go run ./examples/rag_demo`

Set `VECTORDB_URL` (default `http://127.0.0.1:8080`) and optional `VECTORDB_API_KEY`.

## Tests

```bash
cd sdks/go && go test ./...
```
