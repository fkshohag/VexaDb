// Quickstart: end-to-end VectorDB example using the Go SDK.
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

	vectordb "github.com/vectordb/vectordb/sdks/go"
)

const (
	collectionName = "go-quickstart"
	dimension      = 8
	numPoints      = 100
)

func main() {
	baseURL := getenv("VECTORDB_URL", "http://127.0.0.1:8080")
	apiKey := os.Getenv("VECTORDB_API_KEY")

	c := vectordb.NewClient(baseURL, apiKey)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if h, err := c.Health(ctx); err != nil {
		log.Fatalf("health check failed: %v", err)
	} else {
		log.Printf("connected: %s (%v)", baseURL, h)
	}

	if err := c.DeleteCollection(ctx, collectionName); err != nil {
		if e, ok := err.(*vectordb.VectorDbError); !ok || (e.StatusCode != 404 && e.StatusCode != 502) {
			log.Printf("(ignoring) cleanup delete: %v", err)
		}
	}

	if err := c.CreateCollection(ctx, collectionName, dimension, vectordb.CreateCollectionOpts{
		Metric: "cosine",
		PayloadIndexes: []vectordb.PayloadIndex{
			{Field: "category", Kind: "keyword"},
			{Field: "score", Kind: "numeric"},
		},
		BM25TextField: "text",
	}); err != nil {
		log.Fatalf("create collection: %v", err)
	}
	log.Printf("created collection %q (dim=%d)", collectionName, dimension)

	rng := rand.New(rand.NewPCG(42, 0xdeadbeef))
	points := make([]vectordb.Point, numPoints)
	for i := range points {
		v := make([]float32, dimension)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		points[i] = vectordb.Point{
			ID:     fmt.Sprintf("doc-%03d", i),
			Values: v,
			Payload: map[string]any{
				"category": []string{"news", "blog", "paper"}[i%3],
				"score":    i,
				"text":     fmt.Sprintf("document %d about quickstart", i),
			},
		}
	}

	upserted, err := c.BulkUpsert(ctx, collectionName, points, 32)
	if err != nil {
		log.Fatalf("bulk upsert: %v", err)
	}
	log.Printf("upserted %d points", upserted)

	queryVec := make([]float32, dimension)
	for j := range queryVec {
		queryVec[j] = float32(rng.NormFloat64())
	}

	hits, err := c.Search(ctx, collectionName, queryVec, vectordb.SearchOpts{TopK: 5})
	if err != nil {
		log.Fatalf("dense search: %v", err)
	}
	log.Printf("dense top-5:")
	for _, h := range hits {
		log.Printf("  %s  score=%.4f", h.ID, h.Score)
	}

	hits, err = c.Search(ctx, collectionName, queryVec, vectordb.SearchOpts{
		TopK: 5,
		Filter: map[string]any{
			"must": []map[string]any{
				{"key": "category", "match": map[string]any{"value": "news"}},
			},
		},
	})
	if err != nil {
		log.Fatalf("filtered search: %v", err)
	}
	log.Printf("filtered (category=news) top-5:")
	for _, h := range hits {
		log.Printf("  %s  score=%.4f", h.ID, h.Score)
	}

	hits, err = c.Search(ctx, collectionName, queryVec, vectordb.SearchOpts{
		TopK:        5,
		TextQuery:   "quickstart",
		SearchMode:  "hybrid_rrf",
		HybridAlpha: 0.5,
	})
	if err != nil {
		log.Fatalf("hybrid search: %v", err)
	}
	log.Printf("hybrid (RRF) top-5:")
	for _, h := range hits {
		log.Printf("  %s  score=%.4f", h.ID, h.Score)
	}

	if pt, err := c.GetPoint(ctx, collectionName, "doc-000"); err != nil {
		log.Fatalf("get point: %v", err)
	} else if pt != nil {
		log.Printf("get doc-000: payload=%v", pt["payload"])
	}

	if status, err := c.ClusterStatus(ctx); err == nil {
		log.Printf("cluster: %d shards x RF=%d, %d nodes",
			status.ShardCount, status.ReplicationFactor, len(status.Nodes))
	}

	if del, err := c.DeletePoints(ctx, collectionName, []string{"doc-000", "doc-001"}); err != nil {
		log.Fatalf("delete points: %v", err)
	} else {
		log.Printf("deleted %d points", del)
	}

	if err := c.DeleteCollection(ctx, collectionName); err != nil {
		log.Printf("(cleanup) delete collection: %v", err)
	}
	log.Printf("done.")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
