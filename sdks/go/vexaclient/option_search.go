package vexaclient

import "github.com/vectordb/vectordb/sdks/go/entity"

// SearchOption configures vector ANN search.
type SearchOption struct {
	Collection   string
	Limit        int
	Vectors      []entity.Vector
	FilterExpr   string
	FilterJSON   map[string]any
	FilterIDs    []string
	OutputFields   []string
	IncludePayload bool
	IncludeVector  bool
	SparseQuery  *entity.SparseVector
	TextQuery    string
	SearchMode   string
	HybridAlpha  float32
	ANNSField    string
	Offset       int
}

// NewSearchOption creates a search for one or more query vectors.
func NewSearchOption(collection string, limit int, vectors []entity.Vector) *SearchOption {
	return &SearchOption{
		Collection:  collection,
		Limit:       limit,
		Vectors:     vectors,
		SearchMode:  "dense",
		HybridAlpha: 0.5,
		IncludePayload: false,
	}
}

func (o *SearchOption) WithFilter(expr string) *SearchOption {
	o.FilterExpr = expr
	return o
}

func (o *SearchOption) WithFilterJSON(f map[string]any) *SearchOption {
	o.FilterJSON = f
	return o
}

func (o *SearchOption) WithFilterIDs(ids []string) *SearchOption {
	o.FilterIDs = ids
	return o
}

func (o *SearchOption) WithOutputFields(fields ...string) *SearchOption {
	o.OutputFields = fields
	return o
}

func (o *SearchOption) WithPayload(on bool) *SearchOption {
	o.IncludePayload = on
	return o
}

func (o *SearchOption) WithVector(on bool) *SearchOption {
	o.IncludeVector = on
	return o
}

func (o *SearchOption) WithSparseQuery(q *entity.SparseVector) *SearchOption {
	o.SparseQuery = q
	return o
}

func (o *SearchOption) WithTextQuery(q string) *SearchOption {
	o.TextQuery = q
	return o
}

func (o *SearchOption) WithSearchMode(mode string) *SearchOption {
	o.SearchMode = mode
	return o
}

func (o *SearchOption) WithHybridAlpha(a float32) *SearchOption {
	o.HybridAlpha = a
	return o
}

func (o *SearchOption) WithANNSField(field string) *SearchOption {
	o.ANNSField = field
	return o
}

// QueryOption configures filter-only retrieval.
type QueryOption struct {
	Collection   string
	FilterExpr   string
	FilterJSON   map[string]any
	IDs          []string
	Limit        int
	Offset       int
	OutputFields   []string
	IncludePayload bool
	IncludeVector  bool
}

// NewQueryOption creates a filter-only query.
func NewQueryOption(collection string) *QueryOption {
	return &QueryOption{
		Collection:  collection,
		Limit:       100,
		IncludePayload: true,
	}
}

func (o *QueryOption) WithFilter(expr string) *QueryOption {
	o.FilterExpr = expr
	return o
}

func (o *QueryOption) WithFilterJSON(f map[string]any) *QueryOption {
	o.FilterJSON = f
	return o
}

func (o *QueryOption) WithIDs(ids []string) *QueryOption {
	o.IDs = ids
	return o
}

func (o *QueryOption) WithLimit(limit int) *QueryOption {
	o.Limit = limit
	return o
}

func (o *QueryOption) WithOffset(offset int) *QueryOption {
	o.Offset = offset
	return o
}

func (o *QueryOption) WithOutputFields(fields ...string) *QueryOption {
	o.OutputFields = fields
	return o
}

func (o *QueryOption) WithPayload(on bool) *QueryOption {
	o.IncludePayload = on
	return o
}

func (o *QueryOption) WithVector(on bool) *QueryOption {
	o.IncludeVector = on
	return o
}

// DeleteOption deletes points by ID.
type DeleteOption struct {
	Collection string
	IDs        []string
}

func NewDeleteOption(collection string, ids []string) *DeleteOption {
	return &DeleteOption{Collection: collection, IDs: ids}
}

// GetOption fetches one point by ID.
type GetOption struct {
	Collection string
	ID         string
}

func NewGetOption(collection, id string) *GetOption {
	return &GetOption{Collection: collection, ID: id}
}

func filterBody(expr string, jsonFilter map[string]any) any {
	if expr != "" {
		return expr
	}
	if len(jsonFilter) > 0 {
		return jsonFilter
	}
	return nil
}
