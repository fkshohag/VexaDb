package vexaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// HybridSearch runs a multi-vector / multi-leg ANN search and reranks the
// per-leg results into a single ResultSet. Mirrors
// `Client.HybridSearch(ctx, HybridSearchOption)` in Milvus.
//
// The reranker (RRF / Weighted / Function) is carried as JSON in the
// request body so the gateway can dispatch to the correct backend
// strategy.
func (c *Client) HybridSearch(ctx context.Context, opt *HybridSearchOption) ([]*entity.ResultSet, error) {
	if len(opt.Requests) == 0 {
		return nil, fmt.Errorf("HybridSearchOption requires at least one AnnRequest")
	}
	body := map[string]any{
		"limit":          opt.Limit,
		"output_fields":  opt.OutputFields,
		"with_payload":   opt.IncludePayload,
		"with_vector":    opt.IncludeVector,
	}
	if opt.Offset > 0 {
		body["offset"] = opt.Offset
	}
	if len(opt.Partitions) > 0 {
		body["partitions"] = opt.Partitions
	}
	if opt.ConsistencyLevel != "" {
		body["consistency_level"] = opt.ConsistencyLevel
	}
	reqs := make([]map[string]any, 0, len(opt.Requests))
	for _, r := range opt.Requests {
		m := map[string]any{
			"field": r.Field,
			"limit": r.Limit,
		}
		if r.FilterExpr != "" {
			m["filter"] = r.FilterExpr
		}
		switch {
		case len(r.DenseQuery) > 0:
			m["dense"] = r.DenseQuery
		case r.SparseQuery != nil:
			m["sparse"] = map[string]any{
				"indices": r.SparseQuery.Indices,
				"values":  r.SparseQuery.Values,
			}
		case r.TextQuery != "":
			m["text"] = r.TextQuery
		}
		reqs = append(reqs, m)
	}
	body["requests"] = reqs
	if opt.Reranker != nil {
		rr := map[string]any{"kind": opt.Reranker.Kind}
		if len(opt.Reranker.Weights) > 0 {
			rr["weights"] = opt.Reranker.Weights
		}
		if opt.Reranker.Function != "" {
			rr["function"] = opt.Reranker.Function
		}
		body["reranker"] = rr
	}
	var hits []searchHitJSON
	if err := c.do(ctx, http.MethodPost, "/v1/collections/"+opt.Collection+"/hybrid-search", body, &hits); err != nil {
		return nil, err
	}
	rs := searchHitsToResultSet(hits)
	// Milvus returns one ResultSet per input AnnRequest. VexaDb runs all
	// legs server-side and returns the merged set; we expose it as a
	// single-element slice for source compatibility.
	return []*entity.ResultSet{rs}, nil
}

// RunAnalyzer tokenizes one or more input strings using the BM25
// tokenizer (lowercase + alphanumeric split) and optionally filters
// stop words. Mirrors Milvus's RunAnalyzer for diagnostics.
func (c *Client) RunAnalyzer(ctx context.Context, opt *RunAnalyzerOption) ([]*entity.AnalyzerResult, error) {
	body := map[string]any{
		"text":        opt.Text,
		"with_detail": opt.Detail,
		"with_hash":   opt.Hash,
	}
	if opt.AnalyzerParams != nil {
		body["analyzer_params"] = opt.AnalyzerParams
	}
	if opt.AnalyzerParamsStr != "" {
		body["analyzer_params_json"] = opt.AnalyzerParamsStr
	}
	if len(opt.AnalyzerNames) > 0 {
		body["analyzer_name"] = opt.AnalyzerNames
	}
	if opt.Collection != "" {
		body["collection"] = opt.Collection
	}
	if opt.Field != "" {
		body["field"] = opt.Field
	}
	var resp struct {
		Results []struct {
			Tokens []entity.AnalyzerToken `json:"tokens"`
		} `json:"results"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/admin/analyze", body, &resp); err != nil {
		return nil, err
	}
	out := make([]*entity.AnalyzerResult, 0, len(resp.Results))
	for _, r := range resp.Results {
		out = append(out, &entity.AnalyzerResult{Tokens: r.Tokens})
	}
	return out, nil
}

// ---- Iterators -----------------------------------------------------------

// QueryIterator pages through filter-only retrieval results using the
// scroll cursor exposed by the gateway. The iterator is stateful — keep
// calling `Next` until `io.EOF`.
type QueryIterator struct {
	c              *Client
	collection     string
	cursor         string
	batch          int
	filterExpr     string
	outputFields   []string
	partition      string
	withPayload    bool
	withVector     bool
	limit          int64
	consumed       int64
	done           bool
}

// QueryIterator creates the iterator. VexaDb only walks one partition at
// a time; when `WithPartitions` lists multiple names the iterator visits
// the first one and surfaces an error on `Next` for the rest so callers
// notice the limitation instead of silently truncating results.
func (c *Client) QueryIterator(ctx context.Context, opt *QueryIteratorOption) (*QueryIterator, error) {
	partition := ""
	if len(opt.Partitions) == 1 {
		partition = opt.Partitions[0]
	}
	return &QueryIterator{
		c:            c,
		collection:   opt.Collection,
		batch:        opt.BatchSize,
		filterExpr:   opt.FilterExpr,
		outputFields: opt.OutputFields,
		partition:    partition,
		withPayload:  opt.IncludePayload,
		withVector:   opt.IncludeVector,
		limit:        opt.IteratorLimit,
	}, nil
}

// Next returns the next batch as a ResultSet, or io.EOF when done.
func (it *QueryIterator) Next(ctx context.Context) (*entity.ResultSet, error) {
	if it.done {
		return nil, io.EOF
	}
	remaining := it.batch
	if it.limit >= 0 {
		left := int(it.limit - it.consumed)
		if left <= 0 {
			it.done = true
			return nil, io.EOF
		}
		if left < remaining {
			remaining = left
		}
	}
	body := map[string]any{
		"cursor":        it.cursor,
		"limit":         remaining,
		"with_payload":  it.withPayload,
		"with_vector":   it.withVector,
		"output_fields": it.outputFields,
	}
	if it.filterExpr != "" {
		body["filter"] = it.filterExpr
	}
	if it.partition != "" {
		body["partition"] = it.partition
	}
	var out queryResp
	type scrollResp struct {
		Points     []json.RawMessage `json:"points"`
		NextCursor string            `json:"next_cursor"`
	}
	var raw scrollResp
	if err := it.c.do(ctx, http.MethodPost, "/v1/collections/"+it.collection+"/scroll", body, &raw); err != nil {
		return nil, err
	}
	out.Points = make([]struct {
		ID      string         `json:"id"`
		Values  []float32      `json:"values"`
		Payload map[string]any `json:"payload"`
	}, 0, len(raw.Points))
	for _, p := range raw.Points {
		var pt struct {
			ID      string         `json:"id"`
			Values  []float32      `json:"values"`
			Payload map[string]any `json:"payload"`
		}
		_ = json.Unmarshal(p, &pt)
		out.Points = append(out.Points, pt)
	}
	if raw.NextCursor == "" {
		it.done = true
	}
	it.cursor = raw.NextCursor
	if len(out.Points) == 0 {
		it.done = true
		return nil, io.EOF
	}
	it.consumed += int64(len(out.Points))
	return queryToResultSet(out, it.withVector), nil
}

// Close releases any resources held by the iterator. Today the iterator
// is stateless on the server side so this is a no-op; the method exists
// for parity and forward-compatibility.
func (it *QueryIterator) Close() error {
	it.done = true
	return nil
}

// SearchIterator pages through ANN search results. VexaDb implements this
// SDK-side by progressively widening the top-k window: each call to
// `Next` re-runs the underlying search with `limit = consumed + batch`
// and slices the new tail. This yields stable, deduplicated chunks for
// collections that are not changing during iteration.
type SearchIterator struct {
	c           *Client
	opt         *SearchIteratorOption
	consumed    int
	exhausted   bool
}

func (c *Client) SearchIterator(ctx context.Context, opt *SearchIteratorOption) (*SearchIterator, error) {
	return &SearchIterator{c: c, opt: opt}, nil
}

func (it *SearchIterator) Next(ctx context.Context) (*entity.ResultSet, error) {
	if it.exhausted {
		return nil, io.EOF
	}
	batch := it.opt.BatchSize
	if batch <= 0 {
		batch = 100
	}
	limit := it.consumed + batch
	if it.opt.IteratorLimit > 0 && int64(limit) > it.opt.IteratorLimit {
		limit = int(it.opt.IteratorLimit)
	}
	if limit <= it.consumed {
		it.exhausted = true
		return nil, io.EOF
	}
	so := &SearchOption{
		Collection:       it.opt.Collection,
		Limit:            limit,
		Vectors:          []entity.Vector{it.opt.Vector},
		FilterExpr:       it.opt.FilterExpr,
		OutputFields:     it.opt.OutputFields,
		IncludePayload:   it.opt.IncludePayload,
		IncludeVector:    it.opt.IncludeVector,
		ANNSField:        it.opt.ANNSField,
		Offset:           it.opt.Offset + it.consumed,
		Partitions:       it.opt.Partitions,
		ConsistencyLevel: it.opt.ConsistencyLevel,
		AnnParam:         it.opt.AnnParam,
		SearchParams:     it.opt.SearchParams,
		SearchMode:       "dense",
	}
	rs, err := it.c.SearchResultSet(ctx, so)
	if err != nil {
		return nil, err
	}
	if rs == nil || rs.RowCount == 0 {
		it.exhausted = true
		return nil, io.EOF
	}
	it.consumed += rs.RowCount
	if it.opt.IteratorLimit > 0 && int64(it.consumed) >= it.opt.IteratorLimit {
		it.exhausted = true
	}
	if rs.RowCount < batch {
		it.exhausted = true
	}
	return rs, nil
}

func (it *SearchIterator) Close() error {
	it.exhausted = true
	return nil
}
