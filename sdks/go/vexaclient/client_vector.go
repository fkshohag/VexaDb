package vexaclient

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

type upsertResp struct {
	Upserted uint64 `json:"upserted"`
}

type deleteResp struct {
	Deleted uint64 `json:"deleted"`
}

type searchHitJSON struct {
	ID      string         `json:"id"`
	Score   float32        `json:"score"`
	Payload map[string]any `json:"payload,omitempty"`
	Vector  []float32      `json:"vector,omitempty"`
}

type queryResp struct {
	Points []struct {
		ID      string         `json:"id"`
		Values  []float32      `json:"values"`
		Payload map[string]any `json:"payload"`
	} `json:"points"`
}

// Insert inserts entities (alias for Upsert).
func (c *Client) Insert(ctx context.Context, opt *ColumnBasedInsertOption) (InsertResult, error) {
	return c.upsertColumn(ctx, opt)
}

// Upsert inserts or updates entities (column-based). Returns an
// `UpsertResult` shape matching Milvus's API; the result is convertible to
// the legacy `InsertResult` for source compatibility.
func (c *Client) Upsert(ctx context.Context, opt *ColumnBasedInsertOption) (UpsertResult, error) {
	r, err := c.upsertColumn(ctx, opt)
	return UpsertResult{IDs: r.IDs, Upserted: r.Upserted}, err
}

func (c *Client) upsertColumn(ctx context.Context, opt *ColumnBasedInsertOption) (InsertResult, error) {
	points, err := opt.buildPoints()
	if err != nil {
		return InsertResult{}, err
	}
	return c.upsertPoints(ctx, opt.Collection, opt.Partition, points)
}

// InsertRows inserts entities (row-based).
func (c *Client) InsertRows(ctx context.Context, opt *RowBasedInsertOption) (InsertResult, error) {
	return c.upsertRows(ctx, opt)
}

func (c *Client) upsertRows(ctx context.Context, opt *RowBasedInsertOption) (InsertResult, error) {
	points, err := opt.buildPoints()
	if err != nil {
		return InsertResult{}, err
	}
	return c.upsertPoints(ctx, opt.Collection, opt.Partition, points)
}

func (c *Client) upsertPoints(ctx context.Context, collection, partition string, points []gatewayPoint) (InsertResult, error) {
	ids := make([]string, len(points))
	bodyPoints := make([]map[string]any, len(points))
	for i, p := range points {
		ids[i] = p.ID
		m := map[string]any{"id": p.ID, "values": p.Values}
		if len(p.Payload) > 0 {
			m["payload"] = p.Payload
		}
		if p.Sparse != nil && len(p.Sparse.Indices) > 0 {
			m["sparse"] = map[string]any{"indices": p.Sparse.Indices, "values": p.Sparse.Values}
		}
		bodyPoints[i] = m
	}
	body := map[string]any{"points": bodyPoints}
	if partition != "" {
		body["partition"] = partition
	}
	var out upsertResp
	err := c.do(ctx, http.MethodPost, "/v1/collections/"+collection+"/upsert", body, &out)
	return InsertResult{IDs: ids, Upserted: out.Upserted}, err
}

// BulkUpsert ingests many points in chunked WAL writes.
func (c *Client) BulkUpsert(ctx context.Context, collection string, opt *ColumnBasedInsertOption, chunkSize int) (InsertResult, error) {
	points, err := opt.buildPoints()
	if err != nil {
		return InsertResult{}, err
	}
	if chunkSize <= 0 {
		chunkSize = 500
	}
	ids := make([]string, len(points))
	bodyPoints := make([]map[string]any, len(points))
	for i, p := range points {
		ids[i] = p.ID
		m := map[string]any{"id": p.ID, "values": p.Values}
		if len(p.Payload) > 0 {
			m["payload"] = p.Payload
		}
		bodyPoints[i] = m
	}
	var out upsertResp
	err = c.do(ctx, http.MethodPost, "/v1/collections/"+collection+"/bulk",
		map[string]any{"points": bodyPoints, "chunk_size": chunkSize}, &out)
	return InsertResult{IDs: ids, Upserted: out.Upserted}, err
}

// Delete removes points by ID, filter expression, partition, or any
// combination thereof. Mirrors Milvus's `Client.Delete(DeleteOption)`.
func (c *Client) Delete(ctx context.Context, opt *DeleteOption) (DeleteResult, error) {
	body := map[string]any{"ids": opt.IDs}
	if opt.FilterExpr != "" {
		body["filter"] = opt.FilterExpr
	}
	if opt.Partition != "" {
		body["partition"] = opt.Partition
	}
	var out deleteResp
	err := c.do(ctx, http.MethodDelete, "/v1/collections/"+opt.Collection+"/points", body, &out)
	return DeleteResult{Deleted: out.Deleted, DeleteCount: int64(out.Deleted)}, err
}

// GetByID returns one point by ID (back-compat helper).
func (c *Client) GetByID(ctx context.Context, opt *GetOption) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodGet, "/v1/collections/"+opt.Collection+"/points/"+opt.ID, nil, &out)
	if err != nil {
		if e, ok := err.(*VexaError); ok && e.StatusCode == 404 {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

// Get retrieves entities by primary key (Milvus parity). The IDs come from
// `opt.IDs` (set via `WithStringIDs` / `WithInt64IDs` / `WithIDs`). The
// result is a `ResultSet` containing one column per output field.
func (c *Client) Get(ctx context.Context, opt *QueryOption) (*entity.ResultSet, error) {
	return c.Query(ctx, opt)
}

// Search runs ANN search for the first query vector in the option.
func (c *Client) Search(ctx context.Context, opt *SearchOption) ([]SearchResult, error) {
	rs, err := c.SearchResultSet(ctx, opt)
	if err != nil {
		return nil, err
	}
	return resultSetToSearchResults(rs), nil
}

// SearchResultSet runs search and returns a Milvus-style ResultSet.
func (c *Client) SearchResultSet(ctx context.Context, opt *SearchOption) (*entity.ResultSet, error) {
	if len(opt.Vectors) == 0 {
		return nil, fmt.Errorf("SearchOption requires at least one query vector")
	}
	vec := opt.Vectors[0].ToFloat32Slice()
	body := map[string]any{
		"vector":        vec,
		"top_k":         opt.Limit,
		"filter":        filterBody(opt.FilterExpr, opt.FilterJSON),
		"search_mode":   opt.SearchMode,
		"hybrid_alpha":  opt.HybridAlpha,
		"output_fields": opt.OutputFields,
		"with_payload":  opt.IncludePayload,
		"with_vector":   opt.IncludeVector,
	}
	if opt.TextQuery != "" {
		body["text_query"] = opt.TextQuery
	}
	if opt.SparseQuery != nil {
		body["sparse_query"] = map[string]any{
			"indices": opt.SparseQuery.Indices,
			"values":  opt.SparseQuery.Values,
		}
	}
	// Milvus-parity passthroughs. The server side ignores fields it does
	// not honor yet (consistency_level, group_by, etc.).
	if opt.Offset > 0 {
		body["offset"] = opt.Offset
	}
	if len(opt.Partitions) > 0 {
		body["partitions"] = opt.Partitions
	}
	if opt.ConsistencyLevel != "" {
		body["consistency_level"] = opt.ConsistencyLevel
	}
	if opt.GroupByField != "" {
		body["group_by_field"] = opt.GroupByField
		body["group_size"] = opt.GroupSize
		body["strict_group_size"] = opt.StrictGroupSize
	}
	if opt.IgnoreGrowing {
		body["ignore_growing"] = true
	}
	if len(opt.AnnParam) > 0 {
		body["ann_param"] = opt.AnnParam
	}
	if len(opt.SearchParams) > 0 {
		body["search_params"] = opt.SearchParams
	}
	if opt.FunctionReranker != "" {
		body["function_reranker"] = opt.FunctionReranker
	}
	if len(opt.TemplateParams) > 0 {
		body["template_params"] = opt.TemplateParams
	}
	var hits []searchHitJSON
	if err := c.do(ctx, http.MethodPost, "/v1/collections/"+opt.Collection+"/search", body, &hits); err != nil {
		return nil, err
	}
	return searchHitsToResultSet(hits), nil
}

// Query runs filter-only retrieval. Use `WithIDs` for primary-key lookup
// (Milvus's `Get`) or `WithFilter` for arbitrary expressions.
func (c *Client) Query(ctx context.Context, opt *QueryOption) (*entity.ResultSet, error) {
	body := map[string]any{
		"filter":        filterBody(opt.FilterExpr, opt.FilterJSON),
		"ids":           opt.IDs,
		"limit":         opt.Limit,
		"offset":        opt.Offset,
		"output_fields": opt.OutputFields,
		"with_payload":  opt.IncludePayload,
		"with_vector":   opt.IncludeVector,
	}
	if len(opt.Partitions) > 0 {
		body["partitions"] = opt.Partitions
	}
	if opt.ConsistencyLevel != "" {
		body["consistency_level"] = opt.ConsistencyLevel
	}
	if len(opt.TemplateParams) > 0 {
		body["template_params"] = opt.TemplateParams
	}
	var out queryResp
	if err := c.do(ctx, http.MethodPost, "/v1/collections/"+opt.Collection+"/query", body, &out); err != nil {
		return nil, err
	}
	return queryToResultSet(out, opt.IncludeVector), nil
}

func searchHitsToResultSet(hits []searchHitJSON) *entity.ResultSet {
	n := len(hits)
	rs := &entity.ResultSet{
		RowCount: n,
		Scores:   make([]float32, n),
		Fields:   map[string]entity.Column{},
	}
	ids := make([]string, n)
	payloads := make([]map[string]any, n)
	vectors := make([][]float32, 0)
	hasVec := false
	for i, h := range hits {
		ids[i] = h.ID
		rs.Scores[i] = h.Score
		if h.Payload != nil {
			payloads[i] = h.Payload
		}
		if len(h.Vector) > 0 {
			hasVec = true
			vectors = append(vectors, h.Vector)
		}
	}
	rs.Fields["id"] = entity.StringColumn{Field: "id", Data: ids}
	if payloads[0] != nil || len(payloads) > 0 {
		rs.Fields["payload"] = entity.JSONFieldColumn{Field: "payload", Data: payloads}
	}
	if hasVec && len(vectors) == n {
		rs.Fields["vector"] = entity.FloatVectorColumn{Field: "vector", Data: vectors, Dim: len(vectors[0])}
	}
	return rs
}

func queryToResultSet(out queryResp, withVector bool) *entity.ResultSet {
	n := len(out.Points)
	rs := &entity.ResultSet{
		RowCount: n,
		Fields:   map[string]entity.Column{},
	}
	ids := make([]string, n)
	payloads := make([]map[string]any, n)
	var vectors [][]float32
	for i, p := range out.Points {
		ids[i] = p.ID
		payloads[i] = p.Payload
		if withVector && len(p.Values) > 0 {
			vectors = append(vectors, p.Values)
		}
	}
	rs.Fields["id"] = entity.StringColumn{Field: "id", Data: ids}
	rs.Fields["payload"] = entity.JSONFieldColumn{Field: "payload", Data: payloads}
	if len(vectors) == n {
		dim := 0
		if n > 0 {
			dim = len(vectors[0])
		}
		rs.Fields["vector"] = entity.FloatVectorColumn{Field: "vector", Data: vectors, Dim: dim}
	}
	return rs
}

func resultSetToSearchResults(rs *entity.ResultSet) []SearchResult {
	ids, _ := rs.IDs()
	out := make([]SearchResult, rs.RowCount)
	var payloads []map[string]any
	if col, ok := rs.Fields["payload"].(entity.JSONFieldColumn); ok {
		payloads = col.Data
	}
	for i := 0; i < rs.RowCount; i++ {
		id := ""
		if i < len(ids) {
			id = ids[i]
		}
		var score float32
		if i < len(rs.Scores) {
			score = rs.Scores[i]
		}
		var pl map[string]any
		if payloads != nil && i < len(payloads) {
			pl = payloads[i]
		}
		out[i] = SearchResult{ID: id, Score: score, Payload: pl}
	}
	return out
}
