package vexaclient

import (
	"context"
	"fmt"
	"net/http"
)

// CreateCollection creates a collection from options.
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
	if opt.BM25TextField != "" {
		body["bm25_text_field"] = opt.BM25TextField
	}
	if opt.ScalarQuantize {
		body["scalar_quantization"] = true
	}
	return c.do(ctx, http.MethodPost, "/v1/collections", body, nil)
}

// DropCollection deletes a collection.
func (c *Client) DropCollection(ctx context.Context, opt *DropCollectionOption) error {
	return c.do(ctx, http.MethodDelete, "/v1/collections/"+opt.Name, nil, nil)
}

// HasCollection reports whether a collection exists.
func (c *Client) HasCollection(ctx context.Context, name string) (bool, error) {
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

// ListCollections returns all collection names.
func (c *Client) ListCollections(ctx context.Context) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/v1/collections", nil, &out)
	return out, err
}

// DescribeCollection returns spec summary and vector count.
func (c *Client) DescribeCollection(ctx context.Context, name string) (*DescribeCollectionResult, error) {
	var raw map[string]any
	if err := c.do(ctx, http.MethodGet, "/v1/collections/"+name, nil, &raw); err != nil {
		return nil, err
	}
	stats, err := c.GetCollectionStats(ctx, name)
	if err != nil {
		return nil, err
	}
	return &DescribeCollectionResult{
		Name:        name,
		Dimension:   int(stats.Dimension),
		VectorCount: stats.VectorCount,
		Metric:      stats.Metric,
	}, nil
}

// GetCollectionStats returns structured collection statistics.
func (c *Client) GetCollectionStats(ctx context.Context, name string) (*CollectionStats, error) {
	var out CollectionStats
	err := c.do(ctx, http.MethodGet, "/v1/collections/"+name+"/stats", nil, &out)
	return &out, err
}
