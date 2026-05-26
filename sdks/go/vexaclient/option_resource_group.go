package vexaclient

import "github.com/vectordb/vectordb/sdks/go/entity"

// ---- CreateResourceGroup ------------------------------------------------

// CreateResourceGroupOption mirrors Milvus's `NewCreateResourceGroupOption`.
// `Config` may be nil; VexaDb fills in zero limits/requests in that case.
type CreateResourceGroupOption struct {
	Name        string
	NodeRequest int
	NodeLimit   int
	Config      *entity.ResourceGroupConfig
}

// NewCreateResourceGroupOption matches the Milvus v2.6 signature.
func NewCreateResourceGroupOption(name string) *CreateResourceGroupOption {
	return &CreateResourceGroupOption{Name: name}
}

// WithNodeRequest sets the soft lower bound on the number of nodes assigned
// to this group. Equivalent to `Config.Requests.NodeNum`.
func (o *CreateResourceGroupOption) WithNodeRequest(n int) *CreateResourceGroupOption {
	o.NodeRequest = n
	return o
}

// WithNodeLimit sets the hard upper bound on the number of nodes.
// Equivalent to `Config.Limits.NodeNum`.
func (o *CreateResourceGroupOption) WithNodeLimit(n int) *CreateResourceGroupOption {
	o.NodeLimit = n
	return o
}

// WithConfig sets the full resource group config in one shot. When set, it
// takes precedence over `WithNodeRequest`/`WithNodeLimit`.
func (o *CreateResourceGroupOption) WithConfig(cfg *entity.ResourceGroupConfig) *CreateResourceGroupOption {
	o.Config = cfg
	return o
}

// EffectiveConfig folds `WithNodeRequest`/`WithNodeLimit` into the supplied
// `Config` (or returns the explicit `Config` when one was provided).
func (o *CreateResourceGroupOption) EffectiveConfig() *entity.ResourceGroupConfig {
	if o.Config != nil {
		return o.Config
	}
	return &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: int32(o.NodeRequest)},
		Limits:   entity.ResourceGroupLimit{NodeNum: int32(o.NodeLimit)},
	}
}

// ---- DropResourceGroup --------------------------------------------------

type DropResourceGroupOption struct {
	Name string
}

func NewDropResourceGroupOption(name string) *DropResourceGroupOption {
	return &DropResourceGroupOption{Name: name}
}

// ---- ListResourceGroups -------------------------------------------------

type ListResourceGroupsOption struct{}

func NewListResourceGroupsOption() *ListResourceGroupsOption { return &ListResourceGroupsOption{} }

// ---- DescribeResourceGroup ----------------------------------------------

type DescribeResourceGroupOption struct {
	Name string
}

func NewDescribeResourceGroupOption(name string) *DescribeResourceGroupOption {
	return &DescribeResourceGroupOption{Name: name}
}

// ---- UpdateResourceGroup ------------------------------------------------

type UpdateResourceGroupOption struct {
	Name   string
	Config *entity.ResourceGroupConfig
}

// NewUpdateResourceGroupOption matches the Milvus v2.6 signature
// `(name, config)`.
func NewUpdateResourceGroupOption(name string, cfg *entity.ResourceGroupConfig) *UpdateResourceGroupOption {
	return &UpdateResourceGroupOption{Name: name, Config: cfg}
}

// ---- TransferReplica ----------------------------------------------------

type TransferReplicaOption struct {
	CollectionName string
	SourceGroup    string
	TargetGroup    string
	ReplicaNum     int64
	DBName         string
}

// NewTransferReplicaOption matches the Milvus v2.6 signature
// `(collectionName, sourceGroup, targetGroup, replicaNum)`.
func NewTransferReplicaOption(collection, source, target string, replicaNum int64) *TransferReplicaOption {
	return &TransferReplicaOption{
		CollectionName: collection,
		SourceGroup:    source,
		TargetGroup:    target,
		ReplicaNum:     replicaNum,
	}
}

// WithDBName scopes the operation to a specific database.
func (o *TransferReplicaOption) WithDBName(db string) *TransferReplicaOption {
	o.DBName = db
	return o
}

// DescribeReplicaOption / NewDescribeReplicaOption live in
// `option_collection.go` so the older `Client.DescribeReplica`
// surface keeps working; see that file for the type definition.
