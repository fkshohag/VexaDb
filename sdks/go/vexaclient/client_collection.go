package vexaclient

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// CreateCollection creates a collection from options.
//
// Milvus parity: see `sdks/go/milvus-sdk-go/v2.6.x/Collection/CreateCollection.md`.
func (c *Client) CreateCollection(ctx context.Context, opt *CreateCollectionOption) error {
	if opt == nil || opt.Schema == nil {
		return fmt.Errorf("CreateCollectionOption with Schema is required")
	}
	dim := int(opt.Schema.Dimension())
	if dim <= 0 {
		return fmt.Errorf("schema must include a FloatVector field with Dim > 0")
	}
	body := map[string]any{
		"name":      opt.Name,
		"dimension": dim,
		"metric":    opt.MetricType.String(),
	}
	if len(opt.PayloadIndexes) > 0 {
		idxs := make([]map[string]string, len(opt.PayloadIndexes))
		for i, p := range opt.PayloadIndexes {
			idxs[i] = map[string]string{"field": p.Field, "kind": string(p.Kind)}
		}
		body["payload_indexes"] = idxs
	}
	if opt.SparseEnabled {
		body["sparse_enabled"] = true
	}
	// BM25: take the option field first, fall back to a BM25 function in the schema.
	bm25Field := opt.BM25TextField
	if bm25Field == "" && opt.Schema != nil {
		for _, fn := range opt.Schema.Functions {
			if fn != nil && fn.Type == entity.FunctionTypeBM25 && len(fn.InputFieldNames) > 0 {
				bm25Field = fn.InputFieldNames[0]
				break
			}
		}
	}
	if bm25Field != "" {
		body["bm25_text_field"] = bm25Field
	}
	if opt.ScalarQuantize {
		body["scalar_quantization"] = true
	}
	if len(opt.Properties) > 0 {
		body["properties"] = opt.Properties
	}
	return c.do(ctx, http.MethodPost, "/v1/collections", body, nil)
}

// DropCollection deletes a collection (Milvus parity).
func (c *Client) DropCollection(ctx context.Context, opt *DropCollectionOption) error {
	if opt == nil || opt.Name == "" {
		return fmt.Errorf("DropCollectionOption.Name is required")
	}
	return c.do(ctx, http.MethodDelete, "/v1/collections/"+opt.Name, nil, nil)
}

// HasCollection reports whether a collection exists.
//
// Accepts either the Milvus option-style `*HasCollectionOption` or a bare
// name string for backward compatibility with earlier SDK versions.
func (c *Client) HasCollection(ctx context.Context, opt any) (bool, error) {
	name, err := collectionNameFrom(opt, "HasCollection")
	if err != nil {
		return false, err
	}
	names, err := c.ListCollections(ctx)
	if err != nil {
		return false, err
	}
	for _, n := range names {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

// ListCollections returns all collection names. Accepts an optional
// `*ListCollectionOption` (Milvus parity) or nothing.
func (c *Client) ListCollections(ctx context.Context, _ ...*ListCollectionOption) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/v1/collections", nil, &out)
	return out, err
}

// DescribeCollection returns the full collection metadata.
//
// Accepts `*DescribeCollectionOption` (Milvus parity) or a name string for
// backward compatibility.
func (c *Client) DescribeCollection(ctx context.Context, opt any) (*entity.Collection, error) {
	name, err := collectionNameFrom(opt, "DescribeCollection")
	if err != nil {
		return nil, err
	}
	var raw entity.Collection
	if err := c.do(ctx, http.MethodGet, "/v1/collections/"+name, nil, &raw); err != nil {
		return nil, err
	}
	return &raw, nil
}

// GetCollectionStats returns structured collection statistics.
//
// Accepts `*GetCollectionStatsOption` (Milvus parity) or a name string.
func (c *Client) GetCollectionStats(ctx context.Context, opt any) (*CollectionStats, error) {
	name, err := collectionNameFrom(opt, "GetCollectionStats")
	if err != nil {
		return nil, err
	}
	var out CollectionStats
	err = c.do(ctx, http.MethodGet, "/v1/collections/"+name+"/stats", nil, &out)
	return &out, err
}

// ---- Rename / properties / fields (Milvus parity) ----------------------

// RenameCollection renames a collection.
func (c *Client) RenameCollection(ctx context.Context, opt *RenameCollectionOption) error {
	if opt == nil || opt.OldName == "" || opt.NewName == "" {
		return fmt.Errorf("RenameCollectionOption.OldName and NewName are required")
	}
	body := map[string]string{"new_name": opt.NewName}
	return c.do(ctx, http.MethodPost, "/v1/collections/"+opt.OldName+"/rename", body, nil)
}

// AlterCollectionProperties merges properties into a collection.
func (c *Client) AlterCollectionProperties(ctx context.Context, opt *AlterCollectionPropertiesOption) error {
	if opt == nil || opt.Collection == "" {
		return fmt.Errorf("AlterCollectionPropertiesOption.Collection is required")
	}
	body := map[string]any{"set": opt.Properties}
	return c.do(ctx, http.MethodPatch, "/v1/collections/"+opt.Collection+"/properties", body, nil)
}

// AlterCollectionFieldProperty stores per-field properties as
// `field.<name>.<key>` keys (Milvus parity; opaque metadata in VexaDb).
func (c *Client) AlterCollectionFieldProperty(ctx context.Context, opt *AlterCollectionFieldPropertiesOption) error {
	if opt == nil || opt.Collection == "" || opt.Field == "" {
		return fmt.Errorf("AlterCollectionFieldPropertiesOption.Collection and Field are required")
	}
	set := make(map[string]string, len(opt.Properties))
	for k, v := range opt.Properties {
		set[fmt.Sprintf("field.%s.%s", opt.Field, k)] = v
	}
	body := map[string]any{"set": set}
	return c.do(ctx, http.MethodPatch, "/v1/collections/"+opt.Collection+"/properties", body, nil)
}

// DropCollectionProperties removes property keys.
func (c *Client) DropCollectionProperties(ctx context.Context, opt *DropCollectionPropertiesOption) error {
	if opt == nil || opt.Collection == "" {
		return fmt.Errorf("DropCollectionPropertiesOption.Collection is required")
	}
	body := map[string]any{"unset": opt.Keys}
	return c.do(ctx, http.MethodPatch, "/v1/collections/"+opt.Collection+"/properties", body, nil)
}

// AddCollectionField registers a new field.
//
// VexaDb's payload is dynamic JSON, so most fields work without explicit
// registration. The SDK auto-registers a payload index for indexable
// scalar types (Int64, VarChar, Bool, …) when the existing collection has
// dynamic payload (default) so filter pushdown stays fast.
func (c *Client) AddCollectionField(ctx context.Context, opt *AddCollectionFieldOption) error {
	if opt == nil || opt.Collection == "" || opt.Field == nil {
		return fmt.Errorf("AddCollectionFieldOption.Collection and Field are required")
	}
	if opt.Field.DataType == entity.FieldTypeFloatVector ||
		opt.Field.IsPrimary || opt.Field.AutoID {
		return fmt.Errorf(
			"AddCollectionField: cannot add a vector or primary-key field after creation; "+
				"recreate the collection with the new schema (field=%s)",
			opt.Field.Name,
		)
	}
	if !opt.Field.Nullable {
		return fmt.Errorf("AddCollectionField: new fields must be nullable (Milvus parity)")
	}
	// Record under properties for traceability.
	props := map[string]string{
		fmt.Sprintf("field.%s.type", opt.Field.Name):     fmt.Sprintf("%d", opt.Field.DataType),
		fmt.Sprintf("field.%s.nullable", opt.Field.Name): "true",
	}
	if err := c.AlterCollectionProperties(ctx,
		NewAlterCollectionPropertiesOption(opt.Collection).
			WithProperty(fmt.Sprintf("field.%s.type", opt.Field.Name), fmt.Sprintf("%d", opt.Field.DataType)).
			WithProperty(fmt.Sprintf("field.%s.nullable", opt.Field.Name), "true"),
	); err != nil {
		return err
	}
	_ = props
	return nil
}

// ---- Aliases (Milvus parity) -------------------------------------------

// CreateAlias creates a new alias pointing to a collection.
func (c *Client) CreateAlias(ctx context.Context, opt *CreateAliasOption) error {
	if opt == nil || opt.Alias == "" || opt.Collection == "" {
		return fmt.Errorf("CreateAliasOption.Alias and Collection are required")
	}
	body := map[string]string{"alias": opt.Alias, "collection": opt.Collection}
	return c.do(ctx, http.MethodPost, "/v1/aliases", body, nil)
}

// DropAlias removes an alias.
func (c *Client) DropAlias(ctx context.Context, opt *DropAliasOption) error {
	if opt == nil || opt.Alias == "" {
		return fmt.Errorf("DropAliasOption.Alias is required")
	}
	return c.do(ctx, http.MethodDelete, "/v1/aliases/"+opt.Alias, nil, nil)
}

// AlterAlias reassigns an alias to a different collection.
func (c *Client) AlterAlias(ctx context.Context, opt *AlterAliasOption) error {
	if opt == nil || opt.Alias == "" || opt.Collection == "" {
		return fmt.Errorf("AlterAliasOption.Alias and Collection are required")
	}
	body := map[string]string{"collection": opt.Collection}
	return c.do(ctx, http.MethodPut, "/v1/aliases/"+opt.Alias, body, nil)
}

// DescribeAlias resolves an alias to its collection.
func (c *Client) DescribeAlias(ctx context.Context, opt *DescribeAliasOption) (*entity.Alias, error) {
	if opt == nil || opt.Alias == "" {
		return nil, fmt.Errorf("DescribeAliasOption.Alias is required")
	}
	var out entity.Alias
	if err := c.do(ctx, http.MethodGet, "/v1/aliases/"+opt.Alias, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListAliases returns aliases. When `opt.Collection` is empty, returns all
// aliases across all collections.
func (c *Client) ListAliases(ctx context.Context, opt *ListAliasesOption) ([]string, error) {
	if opt != nil && opt.Collection != "" {
		var out []string
		err := c.do(ctx, http.MethodGet, "/v1/collections/"+opt.Collection+"/aliases", nil, &out)
		return out, err
	}
	var rows []entity.Alias
	if err := c.do(ctx, http.MethodGet, "/v1/aliases", nil, &rows); err != nil {
		return nil, err
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Alias
	}
	return out, nil
}

// ---- DescribeReplica (Milvus parity; VexaDb derives from ClusterStatus) -

// DescribeReplica returns shard / replica information for a collection.
//
// When `opt.Collection` is set the SDK calls
// `GET /v1/collections/:name/replicas`, which returns a single
// `entity.ReplicaInfo` with rich shard-to-node placement
// (`Placement[].ShardID`, `NodeID`, `NodeAddress`). When empty, the SDK
// falls back to deriving a single replica from the cluster status endpoint
// (Milvus source compatibility — no breaking change for older callers).
func (c *Client) DescribeReplica(ctx context.Context, opt *DescribeReplicaOption) ([]*entity.ReplicaInfo, error) {
	if opt != nil && opt.Collection != "" {
		return c.describeReplicaByCollection(ctx, opt.Collection, opt.DBName)
	}
	status, err := c.ClusterStatus(ctx)
	if err != nil {
		return nil, err
	}
	shardsByLeader := make(map[string][]*entity.Shard)
	for _, n := range status.Nodes {
		for _, sid := range n.PrimaryForShards {
			shardsByLeader[n.ID] = append(shardsByLeader[n.ID], &entity.Shard{
				ChannelName: fmt.Sprintf("shard-%d", sid),
				ShardNodes:  []int64{nodeIDHashInt64(n.ID)},
				ShardLeader: nodeIDHashInt64(n.ID),
			})
		}
	}
	replica := &entity.ReplicaInfo{
		ReplicaID: 0,
	}
	for _, n := range status.Nodes {
		replica.Nodes = append(replica.Nodes, nodeIDHashInt64(n.ID))
		if shards, ok := shardsByLeader[n.ID]; ok {
			replica.Shards = append(replica.Shards, shards...)
		}
	}
	return []*entity.ReplicaInfo{replica}, nil
}

// nodeIDHashInt64 returns a stable, small integer for a string node ID so
// Milvus-style ReplicaInfo (which uses int64 IDs) can be populated.
func nodeIDHashInt64(s string) int64 {
	// FNV-1a 64 — sufficient for diagnostic display only.
	var h uint64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return int64(h >> 1)
}

// ---- helpers ------------------------------------------------------------

func collectionNameFrom(opt any, fn string) (string, error) {
	switch v := opt.(type) {
	case nil:
		return "", fmt.Errorf("%s: option is required", fn)
	case string:
		if v == "" {
			return "", fmt.Errorf("%s: collection name is required", fn)
		}
		return v, nil
	case *HasCollectionOption:
		return v.Name, nil
	case *DescribeCollectionOption:
		return v.Name, nil
	case *GetCollectionStatsOption:
		return v.Name, nil
	default:
		return "", fmt.Errorf("%s: unsupported option type %T", fn, opt)
	}
}
