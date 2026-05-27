package vexaclient

import "github.com/vectordb/vectordb/sdks/go/entity"

// CreateCollectionOption configures collection creation.
//
// Mirrors Milvus's `NewCreateCollectionOption` surface. Most builders set
// fields that VexaDb consumes today (metric, payload indexes, BM25, …);
// the rest (`AutoID`, `ShardNum`, `NumPartitions`, …) are forward-compat
// hints stored in [Properties] so server-side handling can be added later
// without breaking SDK callers.
type CreateCollectionOption struct {
	Name              string
	Schema            *entity.Schema
	MetricType        entity.MetricType
	PayloadIndexes    []entity.PayloadIndex
	SparseEnabled     bool
	BM25TextField     string
	ScalarQuantize    bool
	Properties        map[string]string
	ConsistencyLevel  entity.ConsistencyLevel
	IndexOptions      []*CreateIndexOption // forward-compat
}

// (CreateIndexOption now lives in option_management.go and matches Milvus's
// `(collection, field, index.Index)` signature.)

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

// WithProperty sets a custom property key-value pair. Milvus parity:
// known keys today are `collection.ttl.seconds`, `mmap.enabled`, and
// `partitionkey.isolation`; unknown keys are accepted as opaque metadata.
func (o *CreateCollectionOption) WithProperty(key string, value any) *CreateCollectionOption {
	if o.Properties == nil {
		o.Properties = make(map[string]string)
	}
	o.Properties[key] = stringify(value)
	return o
}

// WithConsistencyLevel sets the consistency level (VexaDb is always Strong
// today; the value is forwarded as a property for parity).
func (o *CreateCollectionOption) WithConsistencyLevel(cl entity.ConsistencyLevel) *CreateCollectionOption {
	o.ConsistencyLevel = cl
	return o.WithProperty("consistency_level", cl.String())
}

// WithAutoID enables auto-id on the primary key (forward-compat property).
func (o *CreateCollectionOption) WithAutoID(on bool) *CreateCollectionOption {
	if o.Schema != nil {
		o.Schema.AutoID = on
	}
	return o.WithProperty("auto_id", on)
}

// WithShardNum is a Milvus-compat property; VexaDb's shard count is set at
// cluster bootstrap, so this becomes a hint only.
func (o *CreateCollectionOption) WithShardNum(n int32) *CreateCollectionOption {
	return o.WithProperty("shard_num", n)
}

// WithNumPartitions is a Milvus-compat property; VexaDb does not partition
// at SDK level today.
func (o *CreateCollectionOption) WithNumPartitions(n int64) *CreateCollectionOption {
	return o.WithProperty("num_partitions", n)
}

// WithPKFieldName / WithVectorFieldName — Milvus parity; convenience for
// when the schema is built outside the SDK and the user wants to set the
// PK / vector field names directly.
func (o *CreateCollectionOption) WithPKFieldName(name string) *CreateCollectionOption {
	if o.Schema != nil {
		for _, f := range o.Schema.Fields {
			f.IsPrimary = f.Name == name
		}
	}
	return o
}

func (o *CreateCollectionOption) WithVectorFieldName(_ string) *CreateCollectionOption {
	// VexaDb uses a single dense vector column inferred from the schema.
	// Kept as a no-op for Milvus parity.
	return o
}

// WithVarcharPK creates a varchar primary key with the given max length.
func (o *CreateCollectionOption) WithVarcharPK(on bool, maxLen int) *CreateCollectionOption {
	if !on {
		return o
	}
	if o.Schema == nil {
		o.Schema = entity.NewSchema()
	}
	for _, f := range o.Schema.Fields {
		f.IsPrimary = false
	}
	o.Schema.Fields = append(o.Schema.Fields, entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeVarChar).
		WithIsPrimaryKey(true).
		WithMaxLength(maxLen))
	return o
}

// WithIndexOptions attaches index hints (Milvus parity). Stored on the
// option for now; not yet sent to the server.
func (o *CreateCollectionOption) WithIndexOptions(opts ...*CreateIndexOption) *CreateCollectionOption {
	o.IndexOptions = append(o.IndexOptions, opts...)
	return o
}

// DropCollectionOption drops a collection by name.
type DropCollectionOption struct {
	Name string
}

func NewDropCollectionOption(name string) *DropCollectionOption {
	return &DropCollectionOption{Name: name}
}

// ---- Read-side options (Milvus parity) ----------------------------------

// ListCollectionOption is currently empty; reserved for filters.
type ListCollectionOption struct{}

// NewListCollectionOption returns an empty option.
func NewListCollectionOption() *ListCollectionOption { return &ListCollectionOption{} }

// HasCollectionOption checks for existence by name.
type HasCollectionOption struct{ Name string }

// NewHasCollectionOption — Milvus parity.
func NewHasCollectionOption(name string) *HasCollectionOption {
	return &HasCollectionOption{Name: name}
}

// DescribeCollectionOption fetches full collection metadata.
type DescribeCollectionOption struct{ Name string }

func NewDescribeCollectionOption(name string) *DescribeCollectionOption {
	return &DescribeCollectionOption{Name: name}
}

// GetCollectionStatsOption fetches collection statistics.
type GetCollectionStatsOption struct{ Name string }

func NewGetCollectionStatsOption(name string) *GetCollectionStatsOption {
	return &GetCollectionStatsOption{Name: name}
}

// ---- Mutation options (Milvus parity) ----------------------------------

// RenameCollectionOption renames a collection atomically.
type RenameCollectionOption struct {
	OldName string
	NewName string
}

func NewRenameCollectionOption(oldName, newName string) *RenameCollectionOption {
	return &RenameCollectionOption{OldName: oldName, NewName: newName}
}

// AlterCollectionPropertiesOption sets / overwrites property keys.
type AlterCollectionPropertiesOption struct {
	Collection string
	Properties map[string]string
}

func NewAlterCollectionPropertiesOption(collection string) *AlterCollectionPropertiesOption {
	return &AlterCollectionPropertiesOption{Collection: collection}
}

func (o *AlterCollectionPropertiesOption) WithProperty(key string, value any) *AlterCollectionPropertiesOption {
	if o.Properties == nil {
		o.Properties = make(map[string]string)
	}
	o.Properties[key] = stringify(value)
	return o
}

// DropCollectionPropertiesOption removes property keys.
type DropCollectionPropertiesOption struct {
	Collection string
	Keys       []string
}

func NewDropCollectionPropertiesOption(collection string, keys ...string) *DropCollectionPropertiesOption {
	return &DropCollectionPropertiesOption{Collection: collection, Keys: keys}
}

// AlterCollectionFieldPropertiesOption sets a property on a single field.
// VexaDb stores these as `field.<name>.<key>` entries in collection
// properties for forward-compat.
type AlterCollectionFieldPropertiesOption struct {
	Collection string
	Field      string
	Properties map[string]string
}

func NewAlterCollectionFieldPropertiesOption(collection, field string) *AlterCollectionFieldPropertiesOption {
	return &AlterCollectionFieldPropertiesOption{Collection: collection, Field: field}
}

func (o *AlterCollectionFieldPropertiesOption) WithProperty(key string, value any) *AlterCollectionFieldPropertiesOption {
	if o.Properties == nil {
		o.Properties = make(map[string]string)
	}
	o.Properties[key] = stringify(value)
	return o
}

// AddCollectionFieldOption registers a new field. VexaDb's payload is
// dynamic JSON, so for non-indexed fields this is a no-op success. For
// indexable scalar fields, the SDK registers a matching payload index.
type AddCollectionFieldOption struct {
	Collection string
	Field      *entity.Field
}

func NewAddCollectionFieldOption(collection string, field *entity.Field) *AddCollectionFieldOption {
	return &AddCollectionFieldOption{Collection: collection, Field: field}
}

// ---- Alias options (Milvus parity) -------------------------------------

type CreateAliasOption struct {
	Collection string
	Alias      string
}

func NewCreateAliasOption(collection, alias string) *CreateAliasOption {
	return &CreateAliasOption{Collection: collection, Alias: alias}
}

type DropAliasOption struct{ Alias string }

func NewDropAliasOption(alias string) *DropAliasOption {
	return &DropAliasOption{Alias: alias}
}

type AlterAliasOption struct {
	Alias      string
	Collection string
}

func NewAlterAliasOption(alias, collection string) *AlterAliasOption {
	return &AlterAliasOption{Alias: alias, Collection: collection}
}

type DescribeAliasOption struct{ Alias string }

func NewDescribeAliasOption(alias string) *DescribeAliasOption {
	return &DescribeAliasOption{Alias: alias}
}

// ListAliasesOption — when Collection is empty, lists all aliases.
type ListAliasesOption struct{ Collection string }

func NewListAliasesOption(collection string) *ListAliasesOption {
	return &ListAliasesOption{Collection: collection}
}

// DescribeReplicaOption — Milvus parity. When `Collection` is set the SDK
// calls the new `/v1/collections/:name/replicas` endpoint and returns one
// `ReplicaInfo` with rich shard-to-node placement. When empty, the SDK
// falls back to deriving replicas from the cluster status endpoint.
type DescribeReplicaOption struct {
	Collection string
	DBName     string
}

func NewDescribeReplicaOption(collection string) *DescribeReplicaOption {
	return &DescribeReplicaOption{Collection: collection}
}

// WithDBName scopes the call to a specific database (Milvus parity).
func (o *DescribeReplicaOption) WithDBName(db string) *DescribeReplicaOption {
	o.DBName = db
	return o
}
