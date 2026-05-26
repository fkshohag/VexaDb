package entity

// Database mirrors Milvus's `entity.Database`: the response returned by
// DescribeDatabase. Field names follow VexaDb's REST shape (JSON tags
// match the `/v1/databases/:name` gateway response).
//
// VexaDb collections are scoped per-database; a database is identified
// solely by its name and may carry an opaque property bag. Properties
// are exposed as a map so callers can use Milvus-style keys verbatim
// (e.g. `database.replica.number`, `database.diskQuota.mb`).
type Database struct {
	// Name is the database identifier (unique within a VexaDb cluster).
	Name string `json:"name"`

	// Properties are user-defined string key/value pairs. Empty when the
	// database was created without any.
	Properties map[string]string `json:"properties,omitempty"`

	// CreatedAtMs is the database creation timestamp in epoch milliseconds.
	// Zero when the server did not persist a creation timestamp.
	CreatedAtMs uint64 `json:"created_at_ms,omitempty"`
}

// Property returns the value of a property key, or "" when missing.
//
// Convenience wrapper that avoids a nil-map check at every call site.
func (d *Database) Property(key string) string {
	if d == nil || d.Properties == nil {
		return ""
	}
	return d.Properties[key]
}
