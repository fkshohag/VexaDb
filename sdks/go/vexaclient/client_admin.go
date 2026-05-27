package vexaclient

import (
	"context"
	"net/http"
)

// Rebalance triggers a manual rebalance sweep.
func (c *Client) Rebalance(ctx context.Context, dryRun bool) (*RebalanceResult, error) {
	var out RebalanceResult
	err := c.do(ctx, http.MethodPost, "/v1/admin/rebalance", map[string]any{"dry_run": dryRun}, &out)
	return &out, err
}

// ClusterStatus returns cluster topology.
func (c *Client) ClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	var out ClusterStatus
	err := c.do(ctx, http.MethodGet, "/v1/admin/cluster", nil, &out)
	return &out, err
}

// CreateSnapshot creates a filesystem snapshot.
func (c *Client) CreateSnapshot(ctx context.Context) (*Snapshot, error) {
	var out struct {
		Snapshot Snapshot `json:"snapshot"`
	}
	// gateway returns snapshot at top level on POST /v1/snapshots
	var snap Snapshot
	if err := c.do(ctx, http.MethodPost, "/v1/snapshots", nil, &snap); err != nil {
		return nil, err
	}
	_ = out
	return &snap, nil
}

// ListSnapshots lists snapshots.
func (c *Client) ListSnapshots(ctx context.Context) ([]Snapshot, error) {
	var out []Snapshot
	err := c.do(ctx, http.MethodGet, "/v1/snapshots", nil, &out)
	return out, err
}

// DeleteSnapshot removes a snapshot by ID.
func (c *Client) DeleteSnapshot(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/snapshots/"+id, nil, nil)
}

// ReindexCollection rebuilds the HNSW index.
func (c *Client) ReindexCollection(ctx context.Context, collection string) (uint64, error) {
	var out struct {
		VectorsReindexed uint64 `json:"vectors_reindexed"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/collections/"+collection+"/reindex", nil, &out)
	return out.VectorsReindexed, err
}

// CompactWal compacts the write-ahead log.
func (c *Client) CompactWal(ctx context.Context, snapshotFirst bool) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodPost, "/v1/admin/compact-wal", map[string]any{"snapshot_first": snapshotFirst}, &out)
	return out, err
}
