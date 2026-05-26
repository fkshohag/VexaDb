package entity

// DefaultResourceGroupName is the built-in resource group present on every
// VexaDb cluster. Mirrors Milvus's `__default_resource_group` and cannot be
// dropped.
const DefaultResourceGroupName = "__default_resource_group"

// ResourceGroupLimit mirrors Milvus's `entity.ResourceGroupLimit`. A
// `NodeNum == 0` value means "unspecified" and is treated as a no-op by the
// scheduler.
type ResourceGroupLimit struct {
	NodeNum int32 `json:"node_num"`
}

// ResourceGroupTransfer mirrors Milvus's `entity.ResourceGroupTransfer`.
// Identifies one peer resource group that can lend / receive nodes.
type ResourceGroupTransfer struct {
	ResourceGroup string `json:"resource_group"`
}

// ResourceGroupNodeFilter mirrors Milvus's `entity.ResourceGroupNodeFilter`.
// Labels are matched against topology node labels when populating an RG.
type ResourceGroupNodeFilter struct {
	NodeLabels map[string]string `json:"node_labels,omitempty"`
}

// ResourceGroupConfig mirrors Milvus's `entity.ResourceGroupConfig`. Fields
// are stored verbatim by VexaDb; the registry-only implementation does not
// currently enforce `Requests`/`Limits` against actual node capacity.
type ResourceGroupConfig struct {
	Requests     ResourceGroupLimit       `json:"requests"`
	Limits       ResourceGroupLimit       `json:"limits"`
	TransferFrom []*ResourceGroupTransfer `json:"transfer_from,omitempty"`
	TransferTo   []*ResourceGroupTransfer `json:"transfer_to,omitempty"`
	NodeFilter   ResourceGroupNodeFilter  `json:"node_filter,omitempty"`
}

// ResourceGroup is the full description returned by `DescribeResourceGroup`.
// Maps loosely to Milvus's `entity.ResourceGroup` but folds the legacy node
// list (`Nodes`) into `NumAvailableNode` since VexaDb reports topology via a
// separate API.
type ResourceGroup struct {
	Name              string              `json:"name"`
	Config            ResourceGroupConfig `json:"config"`
	NumAvailableNode  int32               `json:"num_available_node"`
	NumLoadedReplica  map[string]int32    `json:"num_loaded_replica,omitempty"`
	NumIncomingNode   map[string]int32    `json:"num_incoming_node,omitempty"`
	NumOutgoingNode   map[string]int32    `json:"num_outgoing_node,omitempty"`
	CreatedTimestamp  uint64              `json:"created_at_ms,omitempty"`
}

// ReplicaShard describes a single shard inside a replica. `NodeAddress` is
// the gRPC endpoint the shard is currently served from. Returned in
// `entity.ReplicaInfo.Placement` by `Client.DescribeReplica`.
type ReplicaShard struct {
	ShardID     uint32 `json:"shard_id"`
	NodeID      string `json:"node_id"`
	NodeAddress string `json:"node_address"`
}
