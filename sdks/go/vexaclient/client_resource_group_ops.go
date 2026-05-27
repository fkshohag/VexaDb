package vexaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// ---- CreateResourceGroup ------------------------------------------------

// CreateResourceGroup mirrors Milvus's `Client.CreateResourceGroup`.
// `WithConfig`, `WithNodeRequest`, `WithNodeLimit` are all honored — when
// `WithConfig` is set it takes priority. Returns an error if the name is
// already in use.
func (c *Client) CreateResourceGroup(ctx context.Context, option *CreateResourceGroupOption) error {
	if option == nil || option.Name == "" {
		return fmt.Errorf("CreateResourceGroupOption.Name is required")
	}
	body := map[string]any{
		"name":   option.Name,
		"config": option.EffectiveConfig(),
	}
	return c.do(ctx, http.MethodPost, "/v1/resource-groups", body, nil)
}

// DropResourceGroup mirrors Milvus's `Client.DropResourceGroup`. The
// built-in `__default_resource_group` cannot be dropped (server returns
// 400).
func (c *Client) DropResourceGroup(ctx context.Context, option *DropResourceGroupOption) error {
	if option == nil || option.Name == "" {
		return fmt.Errorf("DropResourceGroupOption.Name is required")
	}
	path := "/v1/resource-groups/" + url.PathEscape(option.Name)
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// ListResourceGroups mirrors Milvus's `Client.ListResourceGroups`. Always
// returns at least `entity.DefaultResourceGroupName`.
func (c *Client) ListResourceGroups(ctx context.Context, option *ListResourceGroupsOption) ([]string, error) {
	_ = option
	var resp struct {
		ResourceGroups []string `json:"resource_groups"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/resource-groups", nil, &resp); err != nil {
		return nil, err
	}
	return resp.ResourceGroups, nil
}

// DescribeResourceGroup mirrors Milvus's `Client.DescribeResourceGroup`.
func (c *Client) DescribeResourceGroup(ctx context.Context, option *DescribeResourceGroupOption) (*entity.ResourceGroup, error) {
	if option == nil || option.Name == "" {
		return nil, fmt.Errorf("DescribeResourceGroupOption.Name is required")
	}
	path := "/v1/resource-groups/" + url.PathEscape(option.Name)
	var raw struct {
		Name             string           `json:"name"`
		Requests         map[string]any   `json:"requests"`
		Limits           map[string]any   `json:"limits"`
		TransferFrom     []string         `json:"transfer_from"`
		TransferTo       []string         `json:"transfer_to"`
		NodeFilter       map[string]any   `json:"node_filter"`
		NumAvailableNode int32            `json:"num_available_node"`
		NumLoadedReplica map[string]int32 `json:"num_loaded_replica"`
		NumIncomingNode  map[string]int32 `json:"num_incoming_node"`
		NumOutgoingNode  map[string]int32 `json:"num_outgoing_node"`
		CreatedAtMs      uint64           `json:"created_at_ms"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return nil, err
	}
	cfg := entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: nodeNumOf(raw.Requests)},
		Limits:   entity.ResourceGroupLimit{NodeNum: nodeNumOf(raw.Limits)},
	}
	for _, n := range raw.TransferFrom {
		cfg.TransferFrom = append(cfg.TransferFrom, &entity.ResourceGroupTransfer{ResourceGroup: n})
	}
	for _, n := range raw.TransferTo {
		cfg.TransferTo = append(cfg.TransferTo, &entity.ResourceGroupTransfer{ResourceGroup: n})
	}
	if nl, ok := raw.NodeFilter["node_labels"].(map[string]any); ok {
		cfg.NodeFilter.NodeLabels = make(map[string]string, len(nl))
		for k, v := range nl {
			if s, ok := v.(string); ok {
				cfg.NodeFilter.NodeLabels[k] = s
			}
		}
	}
	return &entity.ResourceGroup{
		Name:             raw.Name,
		Config:           cfg,
		NumAvailableNode: raw.NumAvailableNode,
		NumLoadedReplica: raw.NumLoadedReplica,
		NumIncomingNode:  raw.NumIncomingNode,
		NumOutgoingNode:  raw.NumOutgoingNode,
		CreatedTimestamp: raw.CreatedAtMs,
	}, nil
}

func nodeNumOf(m map[string]any) int32 {
	if m == nil {
		return 0
	}
	v, ok := m["node_num"]
	if !ok {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int32(t)
	case int:
		return int32(t)
	case int32:
		return t
	case int64:
		return int32(t)
	}
	return 0
}

// UpdateResourceGroup mirrors Milvus's `Client.UpdateResourceGroup`.
// Replaces the existing config in full.
func (c *Client) UpdateResourceGroup(ctx context.Context, option *UpdateResourceGroupOption) error {
	if option == nil || option.Name == "" {
		return fmt.Errorf("UpdateResourceGroupOption.Name is required")
	}
	cfg := option.Config
	if cfg == nil {
		cfg = &entity.ResourceGroupConfig{}
	}
	path := "/v1/resource-groups/" + url.PathEscape(option.Name)
	body := map[string]any{"name": option.Name, "config": cfg}
	return c.do(ctx, http.MethodPatch, path, body, nil)
}

// ---- TransferReplica ----------------------------------------------------

// TransferReplica mirrors Milvus's `Client.TransferReplica`. VexaDb runs in
// "registry-only" mode: the server validates that both resource groups
// exist and returns success without actually moving shards. Use
// `DescribeReplica` to inspect current shard placement.
func (c *Client) TransferReplica(ctx context.Context, option *TransferReplicaOption) error {
	if option == nil || option.CollectionName == "" {
		return fmt.Errorf("TransferReplicaOption.CollectionName is required")
	}
	if option.SourceGroup == "" || option.TargetGroup == "" {
		return fmt.Errorf("TransferReplicaOption requires both SourceGroup and TargetGroup")
	}
	body := map[string]any{
		"collection":   option.CollectionName,
		"source_group": option.SourceGroup,
		"target_group": option.TargetGroup,
		"replica_num":  option.ReplicaNum,
	}
	if option.DBName != "" {
		body["database"] = option.DBName
	}
	return c.do(ctx, http.MethodPost, "/v1/admin/transfer-replica", body, nil)
}

// describeReplicaByCollection implements the Milvus-parity branch of
// `Client.DescribeReplica` that fetches per-collection shard placement via
// the REST gateway. `Client.DescribeReplica` (in `client_collection.go`)
// is the public entry point; it falls back to the legacy cluster-status
// derivation when the caller passes an empty collection name.
func (c *Client) describeReplicaByCollection(ctx context.Context, collection, dbName string) ([]*entity.ReplicaInfo, error) {
	path := "/v1/collections/" + url.PathEscape(collection) + "/replicas"
	var resp struct {
		Replicas []struct {
			ReplicaID     uint64                `json:"replica_id"`
			Collection    string                `json:"collection"`
			ResourceGroup string                `json:"resource_group"`
			Shards        []entity.ReplicaShard `json:"shards"`
		} `json:"replicas"`
	}
	if err := c.doWithDB(ctx, http.MethodGet, path, dbName, nil, &resp); err != nil {
		return nil, err
	}
	out := make([]*entity.ReplicaInfo, 0, len(resp.Replicas))
	for _, r := range resp.Replicas {
		nodeIDs := make([]string, 0, len(r.Shards))
		for _, s := range r.Shards {
			nodeIDs = append(nodeIDs, s.NodeID)
		}
		out = append(out, &entity.ReplicaInfo{
			ReplicaID:         int64(r.ReplicaID),
			Collection:        r.Collection,
			ResourceGroupName: r.ResourceGroup,
			Placement:         r.Shards,
			NodeIDs:           nodeIDs,
		})
	}
	return out, nil
}

// doWithDB is a thin wrapper around `do` that overrides the `x-vexa-db`
// header for one call. Used by APIs that accept a per-call database
// (Milvus `WithDBName`).
func (c *Client) doWithDB(ctx context.Context, method, path, dbOverride string, body, out any) error {
	if dbOverride == "" {
		return c.do(ctx, method, path, body, out)
	}
	var raw []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		raw = b
	}
	var lastErr error
	for attempt := uint(0); attempt <= c.retry.MaxRetry; attempt++ {
		var r io.Reader
		if raw != nil {
			r = bytes.NewReader(raw)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, r)
		if err != nil {
			return err
		}
		c.auth(req)
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("x-vexa-db", dbOverride)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if !c.shouldRetry(attempt, ctx) {
				return err
			}
			c.sleepBackoff(ctx, attempt)
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusGatewayTimeout {
			lastErr = &VexaError{StatusCode: resp.StatusCode, Message: string(data)}
			if !c.shouldRetry(attempt, ctx) {
				return lastErr
			}
			c.sleepBackoff(ctx, attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return &VexaError{StatusCode: resp.StatusCode, Message: string(data)}
		}
		if out == nil || len(data) == 0 || resp.StatusCode == http.StatusNoContent {
			return nil
		}
		return json.Unmarshal(data, out)
	}
	return lastErr
}
