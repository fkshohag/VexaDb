// Quickstart: end-to-end VexaDb example using the Go SDK.
//
//	cd sdks/go
//	go run ./examples/quickstart
//
// Set VECTORDB_URL (default http://127.0.0.1:8080) and VECTORDB_API_KEY (if
// the gateway requires auth).
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/vectordb/vectordb/sdks/go/entity"
	"github.com/vectordb/vectordb/sdks/go/vexaclient"
)

const (
	collectionName = "go-quickstart"
	dimension      = 8
	numPoints      = 100
)

func main() {
	baseURL := getenv("VECTORDB_URL", "http://127.0.0.1:8080")
	apiKey := os.Getenv("VECTORDB_API_KEY")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cli, err := vexaclient.New(ctx, &vexaclient.ClientConfig{Address: baseURL, APIKey: apiKey})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer cli.Close(ctx)

	log.Printf("connected: %s", baseURL)

	if err := cli.DropCollection(ctx, vexaclient.NewDropCollectionOption(collectionName)); err != nil {
		if e, ok := err.(*vexaclient.VexaError); !ok || (e.StatusCode != 404 && e.StatusCode != 502) {
			log.Printf("(ignoring) cleanup drop: %v", err)
		}
	}

	create := vexaclient.NewSimpleCreateCollectionOption(collectionName, dimension).
		WithMetricType(entity.COSINE).
		WithPayloadIndexes(
			entity.PayloadIndex{Field: "category", Kind: entity.IndexKeyword},
			entity.PayloadIndex{Field: "score", Kind: entity.IndexNumeric},
		).
		WithBM25TextField("text")
	if err := cli.CreateCollection(ctx, create); err != nil {
		log.Fatalf("create collection: %v", err)
	}
	log.Printf("created collection %q (dim=%d)", collectionName, dimension)

	rng := rand.New(rand.NewPCG(42, 0xdeadbeef))
	ids := make([]string, numPoints)
	vectors := make([][]float32, numPoints)
	categories := make([]string, numPoints)
	scores := make([]int64, numPoints)
	texts := make([]string, numPoints)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc-%03d", i)
		v := make([]float32, dimension)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vectors[i] = v
		categories[i] = []string{"news", "blog", "paper"}[i%3]
		scores[i] = int64(i)
		texts[i] = fmt.Sprintf("document %d about quickstart", i)
	}

	insert := vexaclient.NewColumnBasedInsertOption(collectionName).
		WithIDs(ids).
		WithFloatVectorColumn("vector", dimension, vectors).
		WithVarcharColumn("category", categories).
		WithInt64Column("score", scores).
		WithVarcharColumn("text", texts)

	res, err := cli.BulkUpsert(ctx, collectionName, insert, 32)
	if err != nil {
		log.Fatalf("bulk upsert: %v", err)
	}
	log.Printf("upserted %d points", res.Upserted)

	queryVec := make([]float32, dimension)
	for j := range queryVec {
		queryVec[j] = float32(rng.NormFloat64())
	}

	hits, err := cli.Search(ctx, vexaclient.NewSearchOption(collectionName, 5, []entity.Vector{entity.FloatVector(queryVec)}))
	if err != nil {
		log.Fatalf("dense search: %v", err)
	}
	log.Printf("dense top-5:")
	for _, h := range hits {
		log.Printf("  %s  score=%.4f", h.ID, h.Score)
	}

	hits, err = cli.Search(ctx, vexaclient.NewSearchOption(collectionName, 5, []entity.Vector{entity.FloatVector(queryVec)}).
		WithFilterJSON(map[string]any{
			"must": []map[string]any{
				{"key": "category", "match": map[string]any{"value": "news"}},
			},
		}))
	if err != nil {
		log.Fatalf("filtered search: %v", err)
	}
	log.Printf("filtered (category=news) top-5:")
	for _, h := range hits {
		log.Printf("  %s  score=%.4f", h.ID, h.Score)
	}

	hits, err = cli.Search(ctx, vexaclient.NewSearchOption(collectionName, 5, []entity.Vector{entity.FloatVector(queryVec)}).
		WithTextQuery("quickstart").
		WithSearchMode("hybrid_rrf").
		WithHybridAlpha(0.5))
	if err != nil {
		log.Fatalf("hybrid search: %v", err)
	}
	log.Printf("hybrid (RRF) top-5:")
	for _, h := range hits {
		log.Printf("  %s  score=%.4f", h.ID, h.Score)
	}

	if pt, err := cli.GetByID(ctx, vexaclient.NewGetOption(collectionName, "doc-000")); err != nil {
		log.Fatalf("get point: %v", err)
	} else if pt != nil {
		log.Printf("get doc-000: payload=%v", pt["payload"])
	}

	if status, err := cli.ClusterStatus(ctx); err == nil {
		log.Printf("cluster: %d shards x RF=%d, %d nodes",
			status.ShardCount, status.ReplicationFactor, len(status.Nodes))
	}

	if del, err := cli.Delete(ctx, vexaclient.NewDeleteOption(collectionName, "doc-000", "doc-001")); err != nil {
		log.Fatalf("delete points: %v", err)
	} else {
		log.Printf("deleted %d points", del.Deleted)
	}

	if err := cli.DropCollection(ctx, vexaclient.NewDropCollectionOption(collectionName)); err != nil {
		log.Printf("(cleanup) drop collection: %v", err)
	}
	log.Printf("done.")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
