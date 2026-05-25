package vexaclient

// InsertResult is returned by Insert / Upsert.
type InsertResult struct {
	// IDs of affected rows (same order as input when provided).
	IDs []string
	// Upserted count from the server.
	Upserted uint64
}

// DeleteResult is returned by Delete.
type DeleteResult struct {
	Deleted uint64
}

// SearchResult is one ANN hit (convenience view; use ResultSet for columns).
type SearchResult struct {
	ID      string
	Score   float32
	Payload map[string]any
	Vector  []float32
}

// CollectionStats describes a collection (GET /stats).
type CollectionStats struct {
	Name               string `json:"name"`
	VectorCount        uint64 `json:"vector_count"`
	Dimension          uint32 `json:"dimension"`
	Metric             string `json:"metric"`
	SparseEnabled      bool   `json:"sparse_enabled"`
	BM25TextField      string `json:"bm25_text_field"`
	PayloadIndexCount  uint32 `json:"payload_index_count"`
	ScalarQuantization bool   `json:"scalar_quantization"`
}

// DescribeCollectionResult from describe + stats merge.
type DescribeCollectionResult struct {
	Name        string
	Dimension   int
	VectorCount uint64
	Metric      string
}

// RebalanceResult from admin rebalance.
type RebalanceResult struct {
	Moved      uint64 `json:"moved"`
	Kept       uint64 `json:"kept"`
	Failed     uint64 `json:"failed"`
	DurationMs uint64 `json:"duration_ms"`
	DryRun     bool   `json:"dry_run"`
}

// ClusterNode describes one node in cluster status.
type ClusterNode struct {
	ID               string   `json:"id"`
	GRPC             string   `json:"grpc"`
	ShardIDs         []uint32 `json:"shard_ids"`
	Healthy          bool     `json:"healthy"`
	Ready            bool     `json:"ready"`
	IsLeader         bool     `json:"is_leader"`
	Source           string   `json:"source"`
	PrimaryForShards []uint32 `json:"primary_for_shards"`
}

// ClusterStatus from admin cluster endpoint.
type ClusterStatus struct {
	ShardCount        uint32        `json:"shard_count"`
	ReplicationFactor uint32        `json:"replication_factor"`
	Nodes             []ClusterNode `json:"nodes"`
}

// Snapshot metadata.
type Snapshot struct {
	ID          string `json:"id"`
	CreatedAtMs uint64 `json:"created_at_ms"`
	Path        string `json:"path"`
}
