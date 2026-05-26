package vexaclient

import "github.com/vectordb/vectordb/sdks/go/entity"

// QueryIteratorOption mirrors Milvus's QueryIterator builder.
type QueryIteratorOption struct {
	Collection       string
	BatchSize        int
	Partitions       []string
	FilterExpr       string
	OutputFields     []string
	ConsistencyLevel string
	IteratorLimit    int64 // <0 == unlimited
	IncludePayload   bool
	IncludeVector    bool
}

// NewQueryIteratorOption initializes the iterator with sensible defaults
// (batch_size = 100, unlimited, with_payload = true).
func NewQueryIteratorOption(collection string) *QueryIteratorOption {
	return &QueryIteratorOption{
		Collection:     collection,
		BatchSize:      100,
		IteratorLimit:  -1,
		IncludePayload: true,
	}
}

func (o *QueryIteratorOption) WithBatchSize(n int) *QueryIteratorOption {
	o.BatchSize = n
	return o
}
func (o *QueryIteratorOption) WithPartitions(names ...string) *QueryIteratorOption {
	o.Partitions = names
	return o
}
func (o *QueryIteratorOption) WithFilter(expr string) *QueryIteratorOption {
	o.FilterExpr = expr
	return o
}
func (o *QueryIteratorOption) WithOutputFields(fields ...string) *QueryIteratorOption {
	o.OutputFields = fields
	return o
}
func (o *QueryIteratorOption) WithConsistencyLevel(level string) *QueryIteratorOption {
	o.ConsistencyLevel = level
	return o
}
func (o *QueryIteratorOption) WithIteratorLimit(limit int64) *QueryIteratorOption {
	o.IteratorLimit = limit
	return o
}
func (o *QueryIteratorOption) WithPayload(on bool) *QueryIteratorOption {
	o.IncludePayload = on
	return o
}
func (o *QueryIteratorOption) WithVector(on bool) *QueryIteratorOption {
	o.IncludeVector = on
	return o
}

// SearchIteratorOption mirrors Milvus's SearchIterator builder.
type SearchIteratorOption struct {
	Collection       string
	Vector           entity.Vector
	BatchSize        int
	Partitions       []string
	FilterExpr       string
	OutputFields     []string
	ConsistencyLevel string
	IteratorLimit    int64
	ANNSField        string
	AnnParam         map[string]any
	SearchParams     map[string]string
	Offset           int
	IncludePayload   bool
	IncludeVector    bool
}

// NewSearchIteratorOption initializes the iterator with the given query
// vector and sensible defaults.
func NewSearchIteratorOption(collection string, vec entity.Vector) *SearchIteratorOption {
	return &SearchIteratorOption{
		Collection:     collection,
		Vector:         vec,
		BatchSize:      100,
		IteratorLimit:  -1,
		IncludePayload: true,
	}
}

func (o *SearchIteratorOption) WithBatchSize(n int) *SearchIteratorOption {
	o.BatchSize = n
	return o
}
func (o *SearchIteratorOption) WithPartitions(names ...string) *SearchIteratorOption {
	o.Partitions = names
	return o
}
func (o *SearchIteratorOption) WithFilter(expr string) *SearchIteratorOption {
	o.FilterExpr = expr
	return o
}
func (o *SearchIteratorOption) WithOutputFields(fields ...string) *SearchIteratorOption {
	o.OutputFields = fields
	return o
}
func (o *SearchIteratorOption) WithConsistencyLevel(level string) *SearchIteratorOption {
	o.ConsistencyLevel = level
	return o
}
func (o *SearchIteratorOption) WithIteratorLimit(limit int64) *SearchIteratorOption {
	o.IteratorLimit = limit
	return o
}
func (o *SearchIteratorOption) WithANNSField(field string) *SearchIteratorOption {
	o.ANNSField = field
	return o
}
func (o *SearchIteratorOption) WithAnnParam(p any) *SearchIteratorOption {
	o.AnnParam = toJSONMap(p)
	return o
}
func (o *SearchIteratorOption) WithSearchParam(k, v string) *SearchIteratorOption {
	if o.SearchParams == nil {
		o.SearchParams = map[string]string{}
	}
	o.SearchParams[k] = v
	return o
}
func (o *SearchIteratorOption) WithOffset(offset int) *SearchIteratorOption {
	o.Offset = offset
	return o
}
func (o *SearchIteratorOption) WithPayload(on bool) *SearchIteratorOption {
	o.IncludePayload = on
	return o
}
func (o *SearchIteratorOption) WithVector(on bool) *SearchIteratorOption {
	o.IncludeVector = on
	return o
}
