package entity

// Collection mirrors Milvus's entity.Collection: the response returned by
// describe-collection-style APIs. Field names follow VexaDb's REST shape;
// builders should use [Schema] / [Field] for type-safe construction.
type Collection struct {
	Name              string            `json:"name"`
	Dimension         int               `json:"dimension"`
	Metric            string            `json:"metric"`
	M                 int               `json:"m,omitempty"`
	EFConstruction    int               `json:"ef_construction,omitempty"`
	EFSearch          int               `json:"ef_search,omitempty"`
	PayloadIndexes    []PayloadIndex    `json:"payload_indexes,omitempty"`
	SparseEnabled     bool              `json:"sparse_enabled,omitempty"`
	BM25TextField     string            `json:"bm25_text_field,omitempty"`
	ScalarQuantization bool             `json:"scalar_quantization,omitempty"`
	Properties        map[string]string `json:"properties,omitempty"`
	Aliases           []string          `json:"aliases,omitempty"`
	VectorCount       uint64            `json:"vector_count"`
}

// Alias is the (alias, collection) pair returned by alias APIs.
//
// Compatible with Milvus's `entity.Alias` (we drop the `DbName` field since
// VexaDb currently uses a single default DB).
type Alias struct {
	Alias      string `json:"alias"`
	Collection string `json:"collection"`
}

// Shard / ReplicaInfo mirror Milvus's [entity.Shard] / [entity.ReplicaInfo].
// VexaDb has one logical replica per shard (Raft group), so these are
// derived from the cluster status endpoint by the SDK.
type Shard struct {
	ChannelName string  `json:"channel_name"`
	ShardNodes  []int64 `json:"shard_nodes"`
	ShardLeader int64   `json:"shard_leader"`
}

type ReplicaInfo struct {
	ReplicaID         int64            `json:"replica_id"`
	Shards            []*Shard         `json:"shards"`
	Nodes             []int64          `json:"nodes"`
	ResourceGroupName string           `json:"resource_group_name,omitempty"`
	NumOutboundNode   map[string]int32 `json:"num_outbound_node,omitempty"`
}
