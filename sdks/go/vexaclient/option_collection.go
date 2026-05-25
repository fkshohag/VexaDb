package vexaclient

import "github.com/vectordb/vectordb/sdks/go/entity"

// CreateCollectionOption configures collection creation.
type CreateCollectionOption struct {
	Name            string
	Schema          *entity.Schema
	MetricType      entity.MetricType
	PayloadIndexes  []entity.PayloadIndex
	SparseEnabled   bool
	BM25TextField   string
	ScalarQuantize  bool
}

// NewCreateCollectionOption builds options from a schema.
func NewCreateCollectionOption(name string, schema *entity.Schema) *CreateCollectionOption {
	return &CreateCollectionOption{
		Name:       name,
		Schema:     schema,
		MetricType: entity.COSINE,
	}
}

// NewSimpleCreateCollectionOption creates a minimal id + vector schema.
func NewSimpleCreateCollectionOption(name string, dim int64) *CreateCollectionOption {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(512)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(dim))
	return NewCreateCollectionOption(name, schema)
}

func (o *CreateCollectionOption) WithMetricType(m entity.MetricType) *CreateCollectionOption {
	o.MetricType = m
	return o
}

func (o *CreateCollectionOption) WithPayloadIndexes(idxs ...entity.PayloadIndex) *CreateCollectionOption {
	o.PayloadIndexes = idxs
	return o
}

func (o *CreateCollectionOption) WithSparseEnabled(on bool) *CreateCollectionOption {
	o.SparseEnabled = on
	return o
}

func (o *CreateCollectionOption) WithBM25TextField(field string) *CreateCollectionOption {
	o.BM25TextField = field
	return o
}

func (o *CreateCollectionOption) WithScalarQuantization(on bool) *CreateCollectionOption {
	o.ScalarQuantize = on
	return o
}

func (o *CreateCollectionOption) WithDynamicSchema(on bool) *CreateCollectionOption {
	if o.Schema == nil {
		o.Schema = entity.NewSchema()
	}
	o.Schema.DynamicFields = on
	return o
}

// DropCollectionOption drops a collection by name.
type DropCollectionOption struct {
	Name string
}

func NewDropCollectionOption(name string) *DropCollectionOption {
	return &DropCollectionOption{Name: name}
}
