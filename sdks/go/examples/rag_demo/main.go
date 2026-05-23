package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/vectordb/vectordb/sdks/go"
)

const dim = 32

func fakeEmbed(_ context.Context, text string) ([]float32, error) {
	sum := sha256.Sum256([]byte(text))
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = float32(binary.BigEndian.Uint16(sum[i*2:i*2+2]))/65535*2 - 1
	}
	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		norm = 1
	}
	for i := range vec {
		vec[i] = float32(float64(vec[i]) / norm)
	}
	return vec, nil
}

func main() {
	base := os.Getenv("VECTORDB_URL")
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	client := vectordb.NewClient(base, os.Getenv("VECTORDB_API_KEY"))
	rag := vectordb.NewRagPipeline(client, "rag_demo_go", dim, fakeEmbed)

	ctx := context.Background()
	_, err := rag.Ingest(ctx, []vectordb.Document{
		{
			ID: "intro",
			Text: "Vector databases store high-dimensional embeddings for similarity search. " +
				"They power RAG and recommendation systems.",
		},
		{
			ID: "ops",
			Text: "VectorDB supports HNSW indexing, metadata filters, hybrid BM25+dense search, " +
				"and Raft replication.",
		},
	}, true)
	if err != nil {
		panic(err)
	}

	hits, err := rag.Query(ctx, "hybrid search and replication", vectordb.QueryOpts{
		TopK:   3,
		Rerank: true,
	})
	if err != nil {
		panic(err)
	}
	for _, h := range hits {
		snip := h.Text
		if len(snip) > 80 {
			snip = snip[:80] + "..."
		}
		fmt.Printf("%.4f [%s] %s\n", h.Score, h.DocumentID, snip)
	}
}
