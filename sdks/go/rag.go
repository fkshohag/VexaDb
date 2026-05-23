package vectordb

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// EmbedFunc maps text to a dense embedding vector.
type EmbedFunc func(ctx context.Context, text string) ([]float32, error)

// Document is ingested into a RAG collection.
type Document struct {
	ID      string
	Text    string
	Payload map[string]any
}

// RagHit is a retrieved chunk with metadata.
type RagHit struct {
	ID          string
	Score       float64
	Text        string
	Payload     map[string]any
	ChunkIndex  int
	DocumentID  string
}

// ChunkText splits text into overlapping windows.
func ChunkText(text string, chunkSize, overlap int) []string {
	if chunkSize <= 0 {
		chunkSize = 512
	}
	if overlap < 0 {
		overlap = 64
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if len(text) <= chunkSize {
		return []string{text}
	}
	var chunks []string
	start := 0
	for start < len(text) {
		end := start + chunkSize
		if end > len(text) {
			end = len(text)
		} else {
			if split := strings.LastIndex(text[start:end], " "); split > 0 {
				end = start + split
			}
		}
		piece := strings.TrimSpace(text[start:end])
		if piece != "" {
			chunks = append(chunks, piece)
		}
		if end >= len(text) {
			break
		}
		next := end - overlap
		if next <= start {
			next = start + 1
		}
		start = next
	}
	return chunks
}

var wordRe = regexp.MustCompile(`\w+`)

// ExpandQuery returns a small set of query variants.
func ExpandQuery(query string, maxVariants int) []string {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil
	}
	if maxVariants <= 0 {
		maxVariants = 3
	}
	variants := []string{q}
	lower := strings.ToLower(q)
	if lower != q {
		variants = append(variants, lower)
	}
	words := wordRe.FindAllString(q, -1)
	var long []string
	for _, w := range words {
		if len([]rune(w)) > 2 {
			long = append(long, w)
		}
	}
	if len(long) >= 2 {
		variants = append(variants, strings.Join(long, " "))
	}
	seen := map[string]bool{}
	var out []string
	for _, v := range variants {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
		if len(out) >= maxVariants {
			break
		}
	}
	return out
}

// RerankByOverlap re-orders hits by token overlap with the query.
func RerankByOverlap(query string, hits []RagHit, topK int) []RagHit {
	qTokens := tokenSet(strings.ToLower(query))
	if len(qTokens) == 0 {
		if topK > 0 && len(hits) > topK {
			return hits[:topK]
		}
		return hits
	}
	type scored struct {
		hit   RagHit
		score float64
	}
	scoredHits := make([]scored, len(hits))
	for i, h := range hits {
		tTokens := tokenSet(strings.ToLower(h.Text))
		overlap := 0
		for t := range qTokens {
			if tTokens[t] {
				overlap++
			}
		}
		boost := float64(overlap) / float64(len(qTokens))
		scoredHits[i] = scored{hit: h, score: h.Score + boost}
	}
	sort.Slice(scoredHits, func(i, j int) bool {
		return scoredHits[i].score > scoredHits[j].score
	})
	out := make([]RagHit, len(scoredHits))
	for i, s := range scoredHits {
		h := s.hit
		h.Score = s.score
		out[i] = h
	}
	if topK > 0 && len(out) > topK {
		return out[:topK]
	}
	return out
}

func tokenSet(s string) map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}) {
		if w != "" {
			m[w] = true
		}
	}
	return m
}

// RagPipeline chunks documents, embeds, and searches a collection.
type RagPipeline struct {
	Client           *Client
	Collection       string
	Dimension        int
	Embed            EmbedFunc
	TextField        string
	ChunkSize        int
	Overlap          int
	Metric           string
	BM25TextField    string
	SparseEnabled    bool
	collectionReady  bool
}

// NewRagPipeline builds a RAG helper with defaults.
func NewRagPipeline(client *Client, collection string, dimension int, embed EmbedFunc) *RagPipeline {
	return &RagPipeline{
		Client:     client,
		Collection: collection,
		Dimension:  dimension,
		Embed:      embed,
		TextField:  "text",
		ChunkSize:  512,
		Overlap:    64,
		Metric:     "cosine",
	}
}

func (p *RagPipeline) EnsureCollection(ctx context.Context) error {
	if p.collectionReady {
		return nil
	}
	names, err := p.Client.ListCollections(ctx)
	if err != nil {
		return err
	}
	found := false
	for _, n := range names {
		if n == p.Collection {
			found = true
			break
		}
	}
	if !found {
		bm25 := p.BM25TextField
		if bm25 == "" {
			bm25 = p.TextField
		}
		err = p.Client.CreateCollection(ctx, p.Collection, p.Dimension, CreateCollectionOpts{
			Metric:         p.Metric,
			SparseEnabled:  p.SparseEnabled,
			BM25TextField:  bm25,
		})
		if err != nil {
			return err
		}
	}
	p.collectionReady = true
	return nil
}

// Ingest chunks and upserts documents.
func (p *RagPipeline) Ingest(ctx context.Context, docs []Document, bulk bool) (uint64, error) {
	if err := p.EnsureCollection(ctx); err != nil {
		return 0, err
	}
	var points []Point
	for _, doc := range docs {
		chunks := ChunkText(doc.Text, p.ChunkSize, p.Overlap)
		for i, chunk := range chunks {
			vec, err := p.Embed(ctx, chunk)
			if err != nil {
				return 0, err
			}
			if len(vec) != p.Dimension {
				return 0, fmt.Errorf("embedding dimension %d != %d", len(vec), p.Dimension)
			}
			payload := map[string]any{}
			for k, v := range doc.Payload {
				payload[k] = v
			}
			payload[p.TextField] = chunk
			payload["document_id"] = doc.ID
			payload["chunk_index"] = i
			points = append(points, Point{
				ID:      fmt.Sprintf("%s#%d", doc.ID, i),
				Values:  vec,
				Payload: payload,
			})
		}
	}
	if len(points) == 0 {
		return 0, nil
	}
	if bulk {
		return p.Client.BulkUpsert(ctx, p.Collection, points, 500)
	}
	return p.Client.Upsert(ctx, p.Collection, points)
}

// QueryOpts configures retrieval.
type QueryOpts struct {
	TopK        int
	Filter      map[string]any
	SearchMode  string
	HybridAlpha float32
	MultiQuery  bool
	Rerank      bool
}

// Query embeds the query and returns ranked chunks.
func (p *RagPipeline) Query(ctx context.Context, queryText string, opts QueryOpts) ([]RagHit, error) {
	if err := p.EnsureCollection(ctx); err != nil {
		return nil, err
	}
	topK := opts.TopK
	if topK <= 0 {
		topK = 5
	}
	queries := []string{queryText}
	if opts.MultiQuery {
		queries = ExpandQuery(queryText, 3)
	}
	byID := map[string]RagHit{}
	for _, q := range queries {
		vec, err := p.Embed(ctx, q)
		if err != nil {
			return nil, err
		}
		searchMode := opts.SearchMode
		if searchMode == "" {
			searchMode = "dense"
		}
		textQ := ""
		if searchMode != "dense" {
			textQ = q
		}
		raw, err := p.Client.Search(ctx, p.Collection, vec, SearchOpts{
			TopK:        topK,
			Filter:      opts.Filter,
			TextQuery:   textQ,
			SearchMode:  searchMode,
			HybridAlpha: opts.HybridAlpha,
		})
		if err != nil {
			return nil, err
		}
		for rank, row := range raw {
			point, err := p.Client.GetPoint(ctx, p.Collection, row.ID)
			if err != nil {
				return nil, err
			}
			payload := map[string]any{}
			if point != nil {
				if pl, ok := point["payload"].(map[string]any); ok {
					payload = pl
				}
			}
			text, _ := payload[p.TextField].(string)
			docID, _ := payload["document_id"].(string)
			if docID == "" {
				parts := strings.SplitN(row.ID, "#", 2)
				docID = parts[0]
			}
			chunkIdx := 0
			if ci, ok := payload["chunk_index"].(float64); ok {
				chunkIdx = int(ci)
			}
			rrf := 1.0 / float64(60+rank+1)
			if existing, ok := byID[row.ID]; ok {
				existing.Score += rrf
				byID[row.ID] = existing
			} else {
				byID[row.ID] = RagHit{
					ID:         row.ID,
					Score:      rrf,
					Text:       text,
					Payload:    payload,
					ChunkIndex: chunkIdx,
					DocumentID: docID,
				}
			}
		}
	}
	hits := make([]RagHit, 0, len(byID))
	for _, h := range byID {
		hits = append(hits, h)
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	if len(hits) > topK {
		hits = hits[:topK]
	}
	if opts.Rerank {
		hits = RerankByOverlap(queryText, hits, topK)
	}
	return hits, nil
}
