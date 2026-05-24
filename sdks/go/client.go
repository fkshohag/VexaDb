// Package vectordb provides a REST client for the VectorDB gateway.
package vectordb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// VectorDbError is returned when the gateway responds with a non-success status.
type VectorDbError struct {
	StatusCode int
	Message    string
}

func (e *VectorDbError) Error() string {
	return fmt.Sprintf("vectordb: %d %s", e.StatusCode, e.Message)
}

// Client talks to vectordb-gateway over HTTP/JSON.
type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// NewClient creates a client with optional API key.
func NewClient(baseURL string, apiKey string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

func (c *Client) auth(req *http.Request) {
	if c.APIKey == "" {
		return
	}
	req.Header.Set("x-api-key", c.APIKey)
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, r)
	if err != nil {
		return err
	}
	c.auth(req)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &VectorDbError{StatusCode: resp.StatusCode, Message: string(data)}
	}
	if out == nil || len(data) == 0 || resp.StatusCode == 204 {
		return nil
	}
	return json.Unmarshal(data, out)
}

// Point is a vector record for upsert.
type Point struct {
	ID      string         `json:"id"`
	Values  []float32      `json:"values"`
	Payload map[string]any `json:"payload,omitempty"`
	Sparse  *SparseVector  `json:"sparse,omitempty"`
}

// SparseVector holds sparse embedding components.
type SparseVector struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

// SearchHit is one search result.
type SearchHit struct {
	ID    string  `json:"id"`
	Score float32 `json:"score"`
}

// PayloadIndex describes a payload field index.
type PayloadIndex struct {
	Field string `json:"field"`
	Kind  string `json:"kind"` // keyword | numeric | bool
}

func (c *Client) Health(ctx context.Context) (map[string]string, error) {
	var out map[string]string
	err := c.do(ctx, http.MethodGet, "/health", nil, &out)
	return out, err
}

func (c *Client) Live(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/live", nil)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

func (c *Client) Ready(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/ready", nil)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

func (c *Client) ListCollections(ctx context.Context) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/v1/collections", nil, &out)
	return out, err
}

func (c *Client) CreateCollection(ctx context.Context, name string, dimension int, opts CreateCollectionOpts) error {
	body := map[string]any{
		"name":                name,
		"dimension":           dimension,
		"metric":              opts.Metric,
		"sparse_enabled":      opts.SparseEnabled,
		"scalar_quantization": opts.ScalarQuantization,
	}
	if body["metric"] == "" {
		body["metric"] = "cosine"
	}
	if len(opts.PayloadIndexes) > 0 {
		body["payload_indexes"] = opts.PayloadIndexes
	}
	if opts.BM25TextField != "" {
		body["bm25_text_field"] = opts.BM25TextField
	}
	return c.do(ctx, http.MethodPost, "/v1/collections", body, nil)
}

// CreateCollectionOpts configures collection creation.
type CreateCollectionOpts struct {
	Metric              string
	PayloadIndexes      []PayloadIndex
	SparseEnabled       bool
	BM25TextField       string
	ScalarQuantization  bool
}

func (c *Client) DeleteCollection(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/collections/"+name, nil, nil)
}

func (c *Client) Upsert(ctx context.Context, collection string, points []Point) (uint64, error) {
	var out struct {
		Upserted uint64 `json:"upserted"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/collections/"+collection+"/upsert", map[string]any{"points": points}, &out)
	return out.Upserted, err
}

func (c *Client) BulkUpsert(ctx context.Context, collection string, points []Point, chunkSize int) (uint64, error) {
	if chunkSize <= 0 {
		chunkSize = 500
	}
	var out struct {
		Upserted uint64 `json:"upserted"`
	}
	body := map[string]any{"points": points, "chunk_size": chunkSize}
	err := c.do(ctx, http.MethodPost, "/v1/collections/"+collection+"/bulk", body, &out)
	return out.Upserted, err
}

// SearchOpts configures vector search.
type SearchOpts struct {
	TopK         int
	Filter       map[string]any
	SparseQuery  *SparseVector
	TextQuery    string
	SearchMode   string
	HybridAlpha  float32
}

func (c *Client) Search(ctx context.Context, collection string, vector []float32, opts SearchOpts) ([]SearchHit, error) {
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	if opts.SearchMode == "" {
		opts.SearchMode = "dense"
	}
	if opts.HybridAlpha == 0 {
		opts.HybridAlpha = 0.5
	}
	body := map[string]any{
		"vector":        vector,
		"top_k":         opts.TopK,
		"search_mode":   opts.SearchMode,
		"hybrid_alpha":  opts.HybridAlpha,
	}
	if opts.Filter != nil {
		body["filter"] = opts.Filter
	}
	if opts.SparseQuery != nil {
		body["sparse_query"] = opts.SparseQuery
	}
	if opts.TextQuery != "" {
		body["text_query"] = opts.TextQuery
	}
	var hits []SearchHit
	err := c.do(ctx, http.MethodPost, "/v1/collections/"+collection+"/search", body, &hits)
	return hits, err
}

func (c *Client) DeletePoints(ctx context.Context, collection string, ids []string) (uint64, error) {
	var out struct {
		Deleted uint64 `json:"deleted"`
	}
	err := c.do(ctx, http.MethodDelete, "/v1/collections/"+collection+"/points", map[string]any{"ids": ids}, &out)
	return out.Deleted, err
}

func (c *Client) GetPoint(ctx context.Context, collection, id string) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodGet, "/v1/collections/"+collection+"/points/"+id, nil, &out)
	if err != nil {
		if e, ok := err.(*VectorDbError); ok && e.StatusCode == 404 {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

func (c *Client) CompactWal(ctx context.Context, snapshotFirst bool) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodPost, "/v1/admin/compact-wal", map[string]any{"snapshot_first": snapshotFirst}, &out)
	return out, err
}

func (c *Client) ReindexCollection(ctx context.Context, collection string) (uint64, error) {
	var out struct {
		VectorsReindexed uint64 `json:"vectors_reindexed"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/collections/"+collection+"/reindex", nil, &out)
	return out.VectorsReindexed, err
}

// RebalanceResult is returned by Rebalance.
type RebalanceResult struct {
	Moved         uint64 `json:"moved"`
	Kept          uint64 `json:"kept"`
	Failed        uint64 `json:"failed"`
	DurationMs    uint64 `json:"duration_ms"`
	DryRun        bool   `json:"dry_run"`
	PerCollection []struct {
		Collection string `json:"collection"`
		Moved      uint64 `json:"moved"`
		Kept       uint64 `json:"kept"`
		Failed     uint64 `json:"failed"`
	} `json:"per_collection"`
}

// Rebalance triggers a manual rebalance sweep. Set dryRun=true to see what
// would move without actually moving anything.
func (c *Client) Rebalance(ctx context.Context, dryRun bool) (*RebalanceResult, error) {
	var out RebalanceResult
	err := c.do(ctx, http.MethodPost, "/v1/admin/rebalance", map[string]any{"dry_run": dryRun}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// RebalanceStatus describes the rebalance coordinator state.
type RebalanceStatus struct {
	Running             bool    `json:"running"`
	Enabled             bool    `json:"enabled"`
	IntervalSecs        uint64  `json:"interval_secs"`
	LastStartedUnixMs   *uint64 `json:"last_started_unix_ms,omitempty"`
	LastFinishedUnixMs  *uint64 `json:"last_finished_unix_ms,omitempty"`
	LastDurationMs      *uint64 `json:"last_duration_ms,omitempty"`
	LastMoved           uint64  `json:"last_moved"`
	LastKept            uint64  `json:"last_kept"`
	LastFailed          uint64  `json:"last_failed"`
	LastError           string  `json:"last_error"`
	TotalMoves          uint64  `json:"total_moves"`
	TotalSweeps         uint64  `json:"total_sweeps"`
}

func (c *Client) RebalanceStatus(ctx context.Context) (*RebalanceStatus, error) {
	var out RebalanceStatus
	err := c.do(ctx, http.MethodGet, "/v1/admin/rebalance", nil, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// ClusterNode describes one data node in the cluster.
type ClusterNode struct {
	ID                string   `json:"id"`
	GRPC              string   `json:"grpc"`
	ShardIDs          []uint32 `json:"shard_ids"`
	Healthy           bool     `json:"healthy"`
	Ready             bool     `json:"ready"`
	IsLeader          bool     `json:"is_leader"`
	Source            string   `json:"source"`
	PrimaryForShards  []uint32 `json:"primary_for_shards"`
}

// ClusterStatus describes the cluster topology.
type ClusterStatus struct {
	ShardCount             uint32        `json:"shard_count"`
	ReplicationFactor      uint32        `json:"replication_factor"`
	LastHealthUnixMs       *uint64       `json:"last_health_unix_ms,omitempty"`
	LastConfigReloadUnixMs *uint64       `json:"last_config_reload_unix_ms,omitempty"`
	Nodes                  []ClusterNode `json:"nodes"`
}

func (c *Client) ClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	var out ClusterStatus
	err := c.do(ctx, http.MethodGet, "/v1/admin/cluster", nil, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Snapshot describes one snapshot artifact.
type Snapshot struct {
	ID          string `json:"id"`
	CreatedAtMs uint64 `json:"created_at_ms"`
	Path        string `json:"path"`
}

func (c *Client) ListSnapshots(ctx context.Context) ([]Snapshot, error) {
	var out []Snapshot
	err := c.do(ctx, http.MethodGet, "/v1/snapshots", nil, &out)
	return out, err
}

func (c *Client) CreateSnapshot(ctx context.Context) (*Snapshot, error) {
	var out Snapshot
	err := c.do(ctx, http.MethodPost, "/v1/snapshots", nil, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) DeleteSnapshot(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/snapshots/"+id, nil, nil)
}
