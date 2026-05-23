package vectordb

import "testing"

func TestChunkText(t *testing.T) {
	chunks := ChunkText("one two three four five six seven eight nine ten eleven twelve", 20, 5)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
}

func TestExpandQuery(t *testing.T) {
	v := ExpandQuery("Vector Database", 3)
	if len(v) == 0 {
		t.Fatal("expected variants")
	}
}

func TestRerankByOverlap(t *testing.T) {
	hits := []RagHit{
		{ID: "a", Score: 0.9, Text: "cats and dogs"},
		{ID: "b", Score: 0.95, Text: "vector database storage"},
	}
	ranked := RerankByOverlap("vector database", hits, 0)
	if ranked[0].ID != "b" {
		t.Fatalf("expected b first, got %s", ranked[0].ID)
	}
}
