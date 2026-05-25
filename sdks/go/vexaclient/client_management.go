package vexaclient

import (
	"context"
	"net/http"
)

// ---- Client management (Milvus v2.6 parity) ----
//
// These methods cover the surface documented under
// sdks/go/milvus-sdk-go/v2.6.x/Client/* . [New] and [Client.Close] live in
// client.go; this file holds the remaining helpers plus option builders.

// GetServerVersionOption is a placeholder for forward-compat. Milvus exposes
// the same empty option to keep the option-builder shape uniform.
type GetServerVersionOption struct{}

// NewGetServerVersionOption returns an empty option for [Client.GetServerVersion].
func NewGetServerVersionOption() *GetServerVersionOption { return &GetServerVersionOption{} }

// versionResponse mirrors the gateway's /v1/version body.
type versionResponse struct {
	Version   string `json:"version"`
	Server    string `json:"server,omitempty"`
	GitCommit string `json:"git_commit,omitempty"`
}

// GetServerVersion returns the version reported by the connected gateway.
//
// Equivalent to Milvus's `GetServerVersion`. The result is also cached on
// the client during [New]; see [Client.ServerVersion] for the cached value.
func (c *Client) GetServerVersion(ctx context.Context, _ *GetServerVersionOption) (string, error) {
	var out versionResponse
	if err := c.do(ctx, http.MethodGet, "/v1/version", nil, &out); err != nil {
		return "", err
	}
	return out.Version, nil
}

// ServerInfoOption configures [Client.ServerInfo].
type ServerInfoOption struct{}

// NewServerInfoOption returns an empty ServerInfo option.
func NewServerInfoOption() *ServerInfoOption { return &ServerInfoOption{} }

// ServerInfo bundles the metadata returned by the gateway's /v1/version
// endpoint — useful for ops dashboards.
type ServerInfo struct {
	Version   string `json:"version"`
	Server    string `json:"server,omitempty"`
	GitCommit string `json:"git_commit,omitempty"`
}

// ServerInfo returns the full version block from the gateway. Goes beyond
// Milvus's `GetServerVersion` for callers that want commit hash and server
// identifier alongside the semantic version.
func (c *Client) ServerInfo(ctx context.Context, _ *ServerInfoOption) (*ServerInfo, error) {
	var out ServerInfo
	if err := c.do(ctx, http.MethodGet, "/v1/version", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UsingDatabaseOption switches the active database for subsequent calls.
type UsingDatabaseOption struct{ DBName string }

// NewUsingDatabaseOption builds a UsingDatabase option (Milvus parity).
func NewUsingDatabaseOption(dbName string) *UsingDatabaseOption {
	return &UsingDatabaseOption{DBName: dbName}
}

// UsingDatabase updates the `x-vexa-db` header used on subsequent requests.
// Mirrors Milvus's `UsingDatabase` while VexaDb's multi-DB story matures.
// Pass an empty name to clear it.
func (c *Client) UsingDatabase(_ context.Context, opt *UsingDatabaseOption) error {
	if opt == nil {
		return nil
	}
	c.mu.Lock()
	c.cfg.DBName = opt.DBName
	c.mu.Unlock()
	return nil
}

// DBName returns the currently-active database name (may be empty).
func (c *Client) DBName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg.DBName
}
