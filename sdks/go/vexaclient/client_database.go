package vexaclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// ---- Database management (Milvus v2.6 parity) --------------------------
//
// These methods cover the surface documented under
// sdks/go/milvus-sdk-go/v2.6.x/Database/*.md. They map 1:1 onto the
// VexaDb REST gateway exposed at `/v1/databases/*`.

// CreateDatabase creates a new database (collections will be created
// under it once [Client.UseDatabase] / [Client.UsingDatabase] switches
// the active context).
//
// Equivalent to Milvus's `CreateDatabase`.
func (c *Client) CreateDatabase(ctx context.Context, opt *CreateDatabaseOption) error {
	if opt == nil || opt.Name == "" {
		return fmt.Errorf("CreateDatabaseOption.Name is required")
	}
	body := map[string]any{"name": opt.Name}
	if len(opt.Properties) > 0 {
		body["properties"] = opt.Properties
	}
	return c.do(ctx, http.MethodPost, "/v1/databases", body, nil)
}

// DropDatabase drops a database.
//
// By default the call fails when the database still contains
// collections. Use [DropDatabaseOption.WithForce] to cascade-drop all
// child collections and their aliases — Milvus's semantics map onto
// the `force=true` query parameter in VexaDb's REST gateway.
func (c *Client) DropDatabase(ctx context.Context, opt *DropDatabaseOption) error {
	if opt == nil || opt.Name == "" {
		return fmt.Errorf("DropDatabaseOption.Name is required")
	}
	path := "/v1/databases/" + url.PathEscape(opt.Name)
	if opt.Force {
		path += "?force=true"
	}
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// ListDatabase returns the names of every database in the cluster.
//
// Milvus parity: signature matches `(databaseNames []string, err error)`.
// The bare-name overload is kept identical to the Milvus SDK.
func (c *Client) ListDatabase(ctx context.Context, _ *ListDatabaseOption) ([]string, error) {
	var out []string
	if err := c.do(ctx, http.MethodGet, "/v1/databases", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// DescribeDatabase returns full database metadata (name, properties,
// creation timestamp). Returns an error when the database does not
// exist.
func (c *Client) DescribeDatabase(ctx context.Context, opt *DescribeDatabaseOption) (*entity.Database, error) {
	if opt == nil || opt.Name == "" {
		return nil, fmt.Errorf("DescribeDatabaseOption.Name is required")
	}
	var out entity.Database
	if err := c.do(ctx, http.MethodGet, "/v1/databases/"+url.PathEscape(opt.Name), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// AlterDatabaseProperties merges one or more properties into a database.
// Existing keys are overwritten; other keys are left untouched.
func (c *Client) AlterDatabaseProperties(ctx context.Context, opt *AlterDatabasePropertiesOption) error {
	if opt == nil || opt.Name == "" {
		return fmt.Errorf("AlterDatabasePropertiesOption.Name is required")
	}
	body := map[string]any{"set": opt.Properties}
	return c.do(ctx, http.MethodPatch, "/v1/databases/"+url.PathEscape(opt.Name)+"/properties", body, nil)
}

// DropDatabaseProperties removes property keys from a database.
//
// Mirrors Milvus's `DropDatabaseProperties`. Unknown keys are silently
// ignored so the operation is idempotent.
func (c *Client) DropDatabaseProperties(ctx context.Context, opt *DropDatabasePropertiesOption) error {
	if opt == nil || opt.Name == "" {
		return fmt.Errorf("DropDatabasePropertiesOption.Name is required")
	}
	if len(opt.Keys) == 0 {
		return nil
	}
	body := map[string]any{"keys": opt.Keys}
	return c.do(ctx, http.MethodDelete, "/v1/databases/"+url.PathEscape(opt.Name)+"/properties", body, nil)
}

// UseDatabase switches the active database (Milvus v2.6 name). It is an
// alias for [Client.UsingDatabase] kept here for source compatibility
// with Milvus code.
func (c *Client) UseDatabase(ctx context.Context, opt *UseDatabaseOption) error {
	if opt == nil {
		return nil
	}
	return c.UsingDatabase(ctx, &UsingDatabaseOption{DBName: opt.DBName})
}
