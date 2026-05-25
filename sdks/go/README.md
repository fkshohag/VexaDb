# VexaDb Go SDK

Official Go client for the VexaDb HTTP gateway. API shape follows the Milvus Go SDK v2
option-builder pattern (`entity` + `vexaclient`). Reference docs live under
`milvus-sdk-go/` (not a dependency).

## Packages

| Package | Purpose |
|---------|---------|
| `vexaclient` | REST client (collections, upsert, search, query, admin) |
| `entity` | Schemas, vectors, metrics, `ResultSet` columns |
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
