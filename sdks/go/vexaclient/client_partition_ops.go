package vexaclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// CreatePartition creates a new partition inside `option.CollectionName`.
// Returns an error if the collection does not exist or the partition is
// already present. Milvus parity: returns `error`.
func (c *Client) CreatePartition(ctx context.Context, option *CreatePartitionOption) error {
	if option == nil || option.CollectionName == "" || option.PartitionName == "" {
		return fmt.Errorf("CreatePartitionOption requires collection and partition names")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/partitions"
	body := map[string]any{"partition_name": option.PartitionName}
	return c.do(ctx, http.MethodPost, path, body, nil)
}

// DropPartition drops a partition and every point tagged with it. The
// built-in `_default` partition cannot be dropped (server returns 400).
func (c *Client) DropPartition(ctx context.Context, option *DropPartitionOption) error {
	if option == nil || option.CollectionName == "" || option.PartitionName == "" {
		return fmt.Errorf("DropPartitionOption requires collection and partition names")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) +
		"/partitions/" + url.PathEscape(option.PartitionName)
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// HasPartition reports whether the partition exists on the collection.
// Milvus parity: `(bool, error)`.
func (c *Client) HasPartition(ctx context.Context, option *HasPartitionOption) (bool, error) {
	if option == nil || option.CollectionName == "" || option.PartitionName == "" {
		return false, fmt.Errorf("HasPartitionOption requires collection and partition names")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) +
		"/partitions/" + url.PathEscape(option.PartitionName)
	var resp struct {
		Exists bool `json:"exists"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return false, err
	}
	return resp.Exists, nil
}

// ListPartitions returns the partition names declared on the collection.
// Always contains at least `_default`.
func (c *Client) ListPartitions(ctx context.Context, option *ListPartitionsOption) ([]string, error) {
	if option == nil || option.CollectionName == "" {
		return nil, fmt.Errorf("ListPartitionsOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/partitions"
	var resp struct {
		Partitions []string `json:"partitions"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Partitions, nil
}

// GetPartitionStats returns server-reported stats. At minimum the result
// contains `row_count`, `partition_name`, and `collection`. The default
// partition's `row_count` includes any untagged (pre-partition) points so
// upgrading an existing cluster never loses data into a "limbo" partition.
func (c *Client) GetPartitionStats(ctx context.Context, option *GetPartitionStatsOption) (map[string]string, error) {
	if option == nil || option.CollectionName == "" || option.PartitionName == "" {
		return nil, fmt.Errorf("GetPartitionStatsOption requires collection and partition names")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) +
		"/partitions/" + url.PathEscape(option.PartitionName) + "/stats"
	var raw map[string]any
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		switch t := v.(type) {
		case string:
			out[k] = t
		default:
			out[k] = fmt.Sprintf("%v", v)
		}
	}
	return out, nil
}
