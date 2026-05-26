package vexaclient

// CreatePartitionOption mirrors Milvus's `NewCreatePartitionOption`.
type CreatePartitionOption struct {
	CollectionName string
	PartitionName  string
}

// NewCreatePartitionOption matches the Milvus v2.6 signature
// `(collectionName, partitionName)`.
func NewCreatePartitionOption(collection, partition string) *CreatePartitionOption {
	return &CreatePartitionOption{CollectionName: collection, PartitionName: partition}
}

// DropPartitionOption mirrors Milvus's `NewDropPartitionOption`. Dropping a
// partition cascades on VexaDb: every point tagged with the partition is
// deleted as part of the same MetaOp.
type DropPartitionOption struct {
	CollectionName string
	PartitionName  string
}

func NewDropPartitionOption(collection, partition string) *DropPartitionOption {
	return &DropPartitionOption{CollectionName: collection, PartitionName: partition}
}

// HasPartitionOption mirrors Milvus's `NewHasPartitionOption`.
type HasPartitionOption struct {
	CollectionName string
	PartitionName  string
}

func NewHasPartitionOption(collection, partition string) *HasPartitionOption {
	return &HasPartitionOption{CollectionName: collection, PartitionName: partition}
}

// ListPartitionsOption mirrors Milvus's `NewListPartitionOption`.
type ListPartitionsOption struct {
	CollectionName string
}

func NewListPartitionOption(collection string) *ListPartitionsOption {
	return &ListPartitionsOption{CollectionName: collection}
}

// GetPartitionStatsOption mirrors Milvus's `NewGetPartitionStatsOption`. The
// returned map is server-defined; at minimum it contains `row_count`,
// `partition_name`, and `collection`.
type GetPartitionStatsOption struct {
	CollectionName string
	PartitionName  string
}

func NewGetPartitionStatsOption(collection, partition string) *GetPartitionStatsOption {
	return &GetPartitionStatsOption{CollectionName: collection, PartitionName: partition}
}
