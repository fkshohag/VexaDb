package vexaclient

import (
	"github.com/vectordb/vectordb/sdks/go/index"
)

// ---- Index lifecycle options (Milvus parity) ----------------------------

// CreateIndexOption builds a CreateIndex request. Required parameters
// are positional through `NewCreateIndexOption`; everything else flows
// through builder methods.
type CreateIndexOption struct {
	CollectionName string
	FieldName      string
	Index          index.Index
	// IndexName overrides the default index name (defaults to FieldName).
	IndexName string
}

// NewCreateIndexOption mirrors Milvus's constructor exactly so existing
// call sites compile against vexaclient without edits.
func NewCreateIndexOption(collection, field string, idx index.Index) *CreateIndexOption {
	return &CreateIndexOption{
		CollectionName: collection,
		FieldName:      field,
		Index:          idx,
	}
}

// WithIndexName sets a custom name for the index. When unset the field
// name is used as the index name (matches Milvus's behaviour).
func (o *CreateIndexOption) WithIndexName(name string) *CreateIndexOption {
	o.IndexName = name
	return o
}

// DropIndexOption removes an index by name.
type DropIndexOption struct {
	CollectionName string
	IndexName      string
}

// NewDropIndexOption — Milvus parity.
func NewDropIndexOption(collection, indexName string) *DropIndexOption {
	return &DropIndexOption{CollectionName: collection, IndexName: indexName}
}

// DescribeIndexOption fetches the build state of an index.
type DescribeIndexOption struct {
	CollectionName string
	IndexName      string
}

// NewDescribeIndexOption — Milvus parity.
func NewDescribeIndexOption(collection, indexName string) *DescribeIndexOption {
	return &DescribeIndexOption{CollectionName: collection, IndexName: indexName}
}

// ListIndexOption lists indexes defined on a collection.
type ListIndexOption struct {
	CollectionName string
	// When set, only indexes attached to this field are returned.
	FieldName string
}

// NewListIndexOption — Milvus parity.
func NewListIndexOption(collection string) *ListIndexOption {
	return &ListIndexOption{CollectionName: collection}
}

// WithFieldName limits the returned indexes to the given field.
func (o *ListIndexOption) WithFieldName(field string) *ListIndexOption {
	o.FieldName = field
	return o
}

// AlterIndexPropertiesOption merges index properties.
type AlterIndexPropertiesOption struct {
	CollectionName string
	IndexName      string
	Properties     map[string]string
}

// NewAlterIndexPropertiesOption — Milvus parity.
func NewAlterIndexPropertiesOption(collection, indexName string) *AlterIndexPropertiesOption {
	return &AlterIndexPropertiesOption{CollectionName: collection, IndexName: indexName}
}

// WithProperty adds/updates a single property.
func (o *AlterIndexPropertiesOption) WithProperty(key string, value any) *AlterIndexPropertiesOption {
	if o.Properties == nil {
		o.Properties = make(map[string]string)
	}
	o.Properties[key] = stringify(value)
	return o
}

// DropIndexPropertiesOption removes specific property keys from an index.
type DropIndexPropertiesOption struct {
	CollectionName string
	IndexName      string
	Keys           []string
}

// NewDropIndexPropertiesOption — Milvus parity. Pass one or more keys
// to remove.
func NewDropIndexPropertiesOption(collection, indexName string, keys ...string) *DropIndexPropertiesOption {
	return &DropIndexPropertiesOption{
		CollectionName: collection,
		IndexName:      indexName,
		Keys:           keys,
	}
}

// ---- Load / Release options (Milvus parity) -----------------------------

// LoadCollectionOption controls LoadCollection.
//
// VexaDb keeps collections memory-resident so most knobs (replica,
// resource group, refresh) are no-ops. They're preserved verbatim from
// Milvus's option so existing call sites compile against vexaclient.
type LoadCollectionOption struct {
	CollectionName       string
	Replica              int
	ResourceGroups       []string
	LoadFields           []string
	SkipLoadDynamicField bool
	Refresh              bool
}

// NewLoadCollectionOption — Milvus parity.
func NewLoadCollectionOption(name string) *LoadCollectionOption {
	return &LoadCollectionOption{CollectionName: name}
}

// WithReplica sets the desired number of replicas. VexaDb ignores the
// value but records it on the request for future routing.
func (o *LoadCollectionOption) WithReplica(num int) *LoadCollectionOption {
	o.Replica = num
	return o
}

// WithResourceGroup tags the load request with one or more resource
// groups. VexaDb does not partition resources today; the value is
// forwarded as an opaque hint.
func (o *LoadCollectionOption) WithResourceGroup(groups ...string) *LoadCollectionOption {
	o.ResourceGroups = groups
	return o
}

// WithLoadFields restricts which scalar fields to bring into memory.
// VexaDb keeps the full payload resident, so this is a hint only.
func (o *LoadCollectionOption) WithLoadFields(fields ...string) *LoadCollectionOption {
	o.LoadFields = fields
	return o
}

// WithSkipLoadDynamicField skips the dynamic field (Milvus parity).
func (o *LoadCollectionOption) WithSkipLoadDynamicField(skip bool) *LoadCollectionOption {
	o.SkipLoadDynamicField = skip
	return o
}

// WithRefresh forces a reindex before returning (matches Milvus's "load
// with refresh" semantic; VexaDb maps this to ReindexCollection).
func (o *LoadCollectionOption) WithRefresh(refresh bool) *LoadCollectionOption {
	o.Refresh = refresh
	return o
}

// LoadPartitionsOption mirrors Milvus's NewLoadPartitionsOption. VexaDb
// does not implement partitions; the partition names are accepted and
// the call is treated as `LoadCollection`.
type LoadPartitionsOption struct {
	CollectionName       string
	PartitionNames       []string
	Replica              int
	ResourceGroups       []string
	LoadFields           []string
	SkipLoadDynamicField bool
	Refresh              bool
}

// NewLoadPartitionsOption — Milvus parity.
func NewLoadPartitionsOption(collection string, partitions ...string) *LoadPartitionsOption {
	return &LoadPartitionsOption{CollectionName: collection, PartitionNames: partitions}
}

// WithReplica — Milvus parity (ignored by VexaDb).
func (o *LoadPartitionsOption) WithReplica(num int) *LoadPartitionsOption {
	o.Replica = num
	return o
}

// WithResourceGroup — Milvus parity (ignored by VexaDb).
func (o *LoadPartitionsOption) WithResourceGroup(groups ...string) *LoadPartitionsOption {
	o.ResourceGroups = groups
	return o
}

// WithLoadFields — Milvus parity (ignored by VexaDb).
func (o *LoadPartitionsOption) WithLoadFields(fields ...string) *LoadPartitionsOption {
	o.LoadFields = fields
	return o
}

// WithSkipLoadDynamicField — Milvus parity (ignored by VexaDb).
func (o *LoadPartitionsOption) WithSkipLoadDynamicField(skip bool) *LoadPartitionsOption {
	o.SkipLoadDynamicField = skip
	return o
}

// WithRefresh — Milvus parity. Triggers a reindex when true.
func (o *LoadPartitionsOption) WithRefresh(refresh bool) *LoadPartitionsOption {
	o.Refresh = refresh
	return o
}

// ReleaseCollectionOption — Milvus parity.
type ReleaseCollectionOption struct {
	CollectionName string
}

// NewReleaseCollectionOption — Milvus parity.
func NewReleaseCollectionOption(name string) *ReleaseCollectionOption {
	return &ReleaseCollectionOption{CollectionName: name}
}

// ReleasePartitionsOption — Milvus parity. Partition names are accepted
// for source compat; VexaDb releases the entire collection.
type ReleasePartitionsOption struct {
	CollectionName string
	PartitionNames []string
}

// NewReleasePartitionsOption — Milvus parity.
func NewReleasePartitionsOption(collection string, partitions ...string) *ReleasePartitionsOption {
	return &ReleasePartitionsOption{CollectionName: collection, PartitionNames: partitions}
}

// NewReleasePartitionsOptions is an alias matching Milvus's docs typo
// (`NewReleasePartitionsOptions`). Both spellings work.
func NewReleasePartitionsOptions(collection string, partitions ...string) *ReleasePartitionsOption {
	return NewReleasePartitionsOption(collection, partitions...)
}

// GetLoadStateOption — Milvus parity.
type GetLoadStateOption struct {
	CollectionName string
	PartitionNames []string
}

// NewGetLoadStateOption — Milvus parity.
func NewGetLoadStateOption(collection string, partitions ...string) *GetLoadStateOption {
	return &GetLoadStateOption{CollectionName: collection, PartitionNames: partitions}
}

// RefreshLoadOption — Milvus parity.
type RefreshLoadOption struct {
	CollectionName string
}

// NewRefreshLoadOption — Milvus parity.
func NewRefreshLoadOption(name string) *RefreshLoadOption {
	return &RefreshLoadOption{CollectionName: name}
}

// ---- Flush / Compact options (Milvus parity) ----------------------------

// FlushOption — Milvus parity.
type FlushOption struct {
	CollectionName string
}

// NewFlushOption — Milvus parity.
func NewFlushOption(name string) *FlushOption {
	return &FlushOption{CollectionName: name}
}

// CompactOption — Milvus parity.
type CompactOption struct {
	CollectionName string
}

// NewCompactOption — Milvus parity.
func NewCompactOption(name string) *CompactOption {
	return &CompactOption{CollectionName: name}
}

// GetCompactionStateOption — Milvus parity.
type GetCompactionStateOption struct {
	CompactionID uint64
}

// NewGetCompactionStateOption — Milvus parity. Milvus uses int64; VexaDb
// uses uint64 for compaction IDs (epoch-derived). We accept int64 to keep
// the call shape source-compatible and convert.
func NewGetCompactionStateOption(compactionID int64) *GetCompactionStateOption {
	return &GetCompactionStateOption{CompactionID: uint64(compactionID)}
}

// ---- Segments option (Milvus parity) ------------------------------------

// GetPersistentSegmentInfoOption — Milvus parity.
type GetPersistentSegmentInfoOption struct {
	CollectionName string
}

// NewGetPersistentSegmentInfoOption — Milvus parity.
func NewGetPersistentSegmentInfoOption(name string) *GetPersistentSegmentInfoOption {
	return &GetPersistentSegmentInfoOption{CollectionName: name}
}
