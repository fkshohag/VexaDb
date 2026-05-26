package vexaclient

import (
	"encoding/json"
	"strconv"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// toJSONMap is a permissive helper that converts an arbitrary value into
// a `map[string]any`. It is used by `WithAnnParam` to accept either a
// struct, a `map[string]any`, or a JSON string.
func toJSONMap(v any) map[string]any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

// SearchOption configures vector ANN search.
type SearchOption struct {
	Collection     string
	Limit          int
	Vectors        []entity.Vector
	FilterExpr     string
	FilterJSON     map[string]any
	FilterIDs      []string
	OutputFields   []string
	IncludePayload bool
	IncludeVector  bool
	SparseQuery    *entity.SparseVector
	TextQuery      string
	SearchMode     string
	HybridAlpha    float32
	ANNSField      string
	Offset         int
	// Milvus parity: partition scope. Empty slice = all partitions.
	Partitions []string
	// ConsistencyLevel mirrors Milvus's enum (Strong, Bounded, Session,
	// Eventually). VexaDb is strongly consistent under Raft replication
	// today so the field is forwarded but does not change behavior.
	ConsistencyLevel string
	// Group-by parameters (Milvus parity). The SDK forwards these to the
	// gateway; group-by is a no-op on the server today and will round-trip
	// as the equivalent ungrouped search.
	GroupByField    string
	GroupSize       int
	StrictGroupSize bool
	// IgnoreGrowing skips growing segments — VexaDb is single-segment so
	// it has no effect; preserved for source compatibility.
	IgnoreGrowing bool
	// AnnParam carries index-specific tuning knobs (ef, nprobe, etc.).
	AnnParam map[string]any
	// Custom user search params (Milvus's WithSearchParam).
	SearchParams map[string]string
	// FunctionReranker name (Milvus parity). The SDK forwards the name to
	// the gateway; rerank is best-effort.
	FunctionReranker string
	// Template params for expression evaluation (Milvus parity).
	TemplateParams map[string]any
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

func (o *SearchOption) WithOffset(offset int) *SearchOption {
	o.Offset = offset
	return o
}

func (o *SearchOption) WithPartitions(names ...string) *SearchOption {
	o.Partitions = names
	return o
}

func (o *SearchOption) WithConsistencyLevel(level string) *SearchOption {
	o.ConsistencyLevel = level
	return o
}

func (o *SearchOption) WithGroupByField(field string) *SearchOption {
	o.GroupByField = field
	return o
}

func (o *SearchOption) WithGroupSize(size int) *SearchOption {
	o.GroupSize = size
	return o
}

func (o *SearchOption) WithStrictGroupSize(strict bool) *SearchOption {
	o.StrictGroupSize = strict
	return o
}

func (o *SearchOption) WithIgnoreGrowing(ignore bool) *SearchOption {
	o.IgnoreGrowing = ignore
	return o
}

// WithAnnParam stores index-specific tuning knobs. The underlying object
// may be any JSON-serializable structure (e.g. a struct returned by
// NewHNSWAnnParam in Milvus); the SDK turns it into a map at request time.
func (o *SearchOption) WithAnnParam(ap any) *SearchOption {
	o.AnnParam = toJSONMap(ap)
	return o
}

func (o *SearchOption) WithSearchParam(key, value string) *SearchOption {
	if o.SearchParams == nil {
		o.SearchParams = map[string]string{}
	}
	o.SearchParams[key] = value
	return o
}

func (o *SearchOption) WithFunctionReranker(name string) *SearchOption {
	o.FunctionReranker = name
	return o
}

func (o *SearchOption) WithTemplateParam(key string, val any) *SearchOption {
	if o.TemplateParams == nil {
		o.TemplateParams = map[string]any{}
	}
	o.TemplateParams[key] = val
	return o
}

// QueryOption configures filter-only retrieval.
type QueryOption struct {
	Collection     string
	FilterExpr     string
	FilterJSON     map[string]any
	IDs            []string
	Limit          int
	Offset         int
	OutputFields   []string
	IncludePayload bool
	IncludeVector  bool
	// Milvus parity additions.
	Partitions       []string
	ConsistencyLevel string
	TemplateParams   map[string]any
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

func (o *QueryOption) WithPartitions(names ...string) *QueryOption {
	o.Partitions = names
	return o
}

func (o *QueryOption) WithConsistencyLevel(level string) *QueryOption {
	o.ConsistencyLevel = level
	return o
}

func (o *QueryOption) WithTemplateParam(key string, val any) *QueryOption {
	if o.TemplateParams == nil {
		o.TemplateParams = map[string]any{}
	}
	o.TemplateParams[key] = val
	return o
}

// WithInt64IDs sets primary keys as int64 IDs (formatted as strings).
func (o *QueryOption) WithInt64IDs(_field string, ids []int64) *QueryOption {
	out := make([]string, len(ids))
	for i, v := range ids {
		out[i] = strconv.FormatInt(v, 10)
	}
	o.IDs = out
	return o
}

// WithStringIDs sets primary keys as strings (varchar PK).
func (o *QueryOption) WithStringIDs(_field string, ids []string) *QueryOption {
	o.IDs = ids
	return o
}

// DeleteOption deletes points by ID, filter expression, or both.
type DeleteOption struct {
	Collection string
	IDs        []string
	// FilterExpr is a Milvus-style boolean expression. Either or both of
	// IDs / FilterExpr can be set; the server deletes their union.
	FilterExpr string
	// Partition is an optional partition scope (Milvus parity). Empty
	// string = "all partitions" (legacy behavior).
	Partition string
}

// NewDeleteOption keeps the legacy one-shot constructor that takes a
// pre-built id list. Prefer the chainable form
// `NewDeleteOption(coll).WithStringIDs(field, ids)` for new code.
func NewDeleteOption(collection string, ids ...string) *DeleteOption {
	// Legacy callers passed `[]string` as the second argument; reroute
	// that into the variadic so both shapes compile.
	flat := make([]string, 0, len(ids))
	for _, x := range ids {
		flat = append(flat, x)
	}
	return &DeleteOption{Collection: collection, IDs: flat}
}

func (o *DeleteOption) WithExpr(expr string) *DeleteOption {
	o.FilterExpr = expr
	return o
}

func (o *DeleteOption) WithPartition(name string) *DeleteOption {
	o.Partition = name
	return o
}

func (o *DeleteOption) WithInt64IDs(_field string, ids []int64) *DeleteOption {
	out := make([]string, len(ids))
	for i, v := range ids {
		out[i] = strconv.FormatInt(v, 10)
	}
	o.IDs = out
	return o
}

func (o *DeleteOption) WithStringIDs(_field string, ids []string) *DeleteOption {
	o.IDs = ids
	return o
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
