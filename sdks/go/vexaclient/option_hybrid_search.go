package vexaclient

import "github.com/vectordb/vectordb/sdks/go/entity"

// AnnRequest is one leg of a HybridSearch call (Milvus parity). Exactly
// one of `DenseQuery`, `SparseQuery`, or `TextQuery` should be populated.
type AnnRequest struct {
	Field       string
	Limit       int
	DenseQuery  []float32
	SparseQuery *entity.SparseVector
	TextQuery   string
	FilterExpr  string
}

// NewAnnRequest constructs an AnnRequest from a vector. The vector may be
// a `[]float32`, an `entity.FloatVector`, or an `*entity.SparseVector`.
func NewAnnRequest(field string, limit int, query any) *AnnRequest {
	r := &AnnRequest{Field: field, Limit: limit}
	switch v := query.(type) {
	case []float32:
		r.DenseQuery = v
	case entity.FloatVector:
		r.DenseQuery = []float32(v)
	case *entity.SparseVector:
		r.SparseQuery = v
	case entity.SparseVector:
		c := v
		r.SparseQuery = &c
	case string:
		r.TextQuery = v
	}
	return r
}

// WithFilter applies a per-leg boolean expression.
func (r *AnnRequest) WithFilter(expr string) *AnnRequest {
	r.FilterExpr = expr
	return r
}

// Reranker is the strategy used to merge per-leg results. Use the
// constructor helpers `NewRRFReranker`, `NewWeightedReranker`, or
// `NewFunctionReranker` to build one.
type Reranker struct {
	Kind     string
	Weights  []float32
	Function string
}

// NewRRFReranker returns a Reciprocal Rank Fusion reranker.
func NewRRFReranker() *Reranker { return &Reranker{Kind: "rrf"} }

// NewWeightedReranker assigns one weight per AnnRequest. Missing entries
// default to 1.0 server-side.
func NewWeightedReranker(weights ...float32) *Reranker {
	return &Reranker{Kind: "weighted", Weights: weights}
}

// NewFunctionReranker carries a user-defined reranker name. VexaDb treats
// the function as a pass-through (best-score dedup) for source
// compatibility; the name field is logged but not executed.
func NewFunctionReranker(name string) *Reranker {
	return &Reranker{Kind: "function", Function: name}
}

// HybridSearchOption configures a multi-vector / multi-leg search.
type HybridSearchOption struct {
	Collection       string
	Limit            int
	Requests         []*AnnRequest
	Reranker         *Reranker
	Partitions       []string
	OutputFields     []string
	IncludePayload   bool
	IncludeVector    bool
	ConsistencyLevel string
	Offset           int
}

// NewHybridSearchOption builds a HybridSearch request with the supplied
// AnnRequests and a default RRF reranker.
func NewHybridSearchOption(collection string, limit int, requests ...*AnnRequest) *HybridSearchOption {
	return &HybridSearchOption{
		Collection:     collection,
		Limit:          limit,
		Requests:       requests,
		Reranker:       NewRRFReranker(),
		IncludePayload: true,
	}
}

func (o *HybridSearchOption) WithReranker(r *Reranker) *HybridSearchOption {
	o.Reranker = r
	return o
}

func (o *HybridSearchOption) WithFunctionRerankers(name string) *HybridSearchOption {
	o.Reranker = NewFunctionReranker(name)
	return o
}

func (o *HybridSearchOption) WithPartitions(names ...string) *HybridSearchOption {
	o.Partitions = names
	return o
}

func (o *HybridSearchOption) WithOutputFields(fields ...string) *HybridSearchOption {
	o.OutputFields = fields
	return o
}

func (o *HybridSearchOption) WithPayload(on bool) *HybridSearchOption {
	o.IncludePayload = on
	return o
}

func (o *HybridSearchOption) WithVector(on bool) *HybridSearchOption {
	o.IncludeVector = on
	return o
}

func (o *HybridSearchOption) WithConsistencyLevel(level string) *HybridSearchOption {
	o.ConsistencyLevel = level
	return o
}

func (o *HybridSearchOption) WithOffset(offset int) *HybridSearchOption {
	o.Offset = offset
	return o
}
