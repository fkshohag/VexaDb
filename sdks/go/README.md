# VectorDB Go SDK

REST client and RAG helpers for [VectorDB](../../README.md).

## Install

```bash
cd sdks/go
go get github.com/vectordb/vectordb/sdks/go
```

Or use a `replace` in your module while developing:

```go
replace github.com/vectordb/vectordb/sdks/go => ../sdks/go
```

## Example

```go
client := vectordb.NewClient("http://127.0.0.1:8080", os.Getenv("VECTORDB_API_KEY"))
ctx := context.Background()
_ = client.CreateCollection(ctx, "docs", 128, vectordb.CreateCollectionOpts{})
_, _ = client.Upsert(ctx, "docs", []vectordb.Point{{ID: "a", Values: vec}})
hits, _ := client.Search(ctx, "docs", query, vectordb.SearchOpts{TopK: 5})
```

## RAG

```go
rag := vectordb.NewRagPipeline(client, "kb", 1536, myEmbed)
_, _ = rag.Ingest(ctx, []vectordb.Document{{ID: "1", Text: "..."}}, true)
hits, _ := rag.Query(ctx, "question", vectordb.QueryOpts{TopK: 5, Rerank: true})
```

Demo: `go run ./examples/rag_demo` (gateway on `:8080`).

Tests: `go test ./...`
