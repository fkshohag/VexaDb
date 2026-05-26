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
- **Partitions**: VexaDb has none. `LoadPartitions`/`ReleasePartitions`
  alias to the whole-collection variants.
- **Compaction IDs**: minted from epoch-ms so they sort chronologically;
  state lookups only resolve on the originating shard (single-shard
  cluster: always works).

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
