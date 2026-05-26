package vexaclient

// ---- Database options (Milvus v2.6 parity) -----------------------------
//
// These mirror `milvusclient.NewXxxDatabaseOption` builders so existing
// Milvus codebases can migrate to VexaDb without restructuring call
// sites. See `sdks/go/milvus-sdk-go/v2.6.x/Database/*.md` for the
// reference surface.

// CreateDatabaseOption builds a CreateDatabase request.
type CreateDatabaseOption struct {
	Name       string
	Properties map[string]string
}

// NewCreateDatabaseOption — Milvus parity.
func NewCreateDatabaseOption(name string) *CreateDatabaseOption {
	return &CreateDatabaseOption{Name: name}
}

// WithProperty attaches a custom property to the create request.
//
// Values are stringified using the same rules as collection properties
// so integer/bool literals work transparently
// (e.g. `WithProperty("database.replica.number", 2)`).
func (o *CreateDatabaseOption) WithProperty(key string, value any) *CreateDatabaseOption {
	if o.Properties == nil {
		o.Properties = make(map[string]string)
	}
	o.Properties[key] = stringify(value)
	return o
}

// DropDatabaseOption configures DropDatabase.
//
// VexaDb's drop is non-cascading by default: dropping a database that
// still contains collections returns an error. Call [DropDatabaseOption.WithForce]
// to cascade-drop all child collections and their aliases.
type DropDatabaseOption struct {
	Name  string
	Force bool
}

// NewDropDatabaseOption — Milvus parity.
func NewDropDatabaseOption(name string) *DropDatabaseOption {
	return &DropDatabaseOption{Name: name}
}

// WithForce enables cascade-drop. Without this flag the server rejects
// a drop of a non-empty database with a 409 Conflict.
func (o *DropDatabaseOption) WithForce(on bool) *DropDatabaseOption {
	o.Force = on
	return o
}

// ListDatabaseOption is reserved for future filtering (Milvus parity).
type ListDatabaseOption struct{}

// NewListDatabaseOption — Milvus parity.
func NewListDatabaseOption() *ListDatabaseOption { return &ListDatabaseOption{} }

// DescribeDatabaseOption fetches database metadata by name.
type DescribeDatabaseOption struct{ Name string }

// NewDescribeDatabaseOption — Milvus parity.
func NewDescribeDatabaseOption(name string) *DescribeDatabaseOption {
	return &DescribeDatabaseOption{Name: name}
}

// UseDatabaseOption switches the active database for subsequent calls.
//
// Mirrors Milvus's `NewUseDatabaseOption`. VexaDb shares the same
// header-based mechanism as [UsingDatabaseOption]; both builders are
// kept so users coming from Milvus v2.5 and v2.6 codebases can use the
// name they're familiar with.
type UseDatabaseOption struct{ DBName string }

// NewUseDatabaseOption — Milvus parity (v2.6 name).
func NewUseDatabaseOption(dbName string) *UseDatabaseOption {
	return &UseDatabaseOption{DBName: dbName}
}

// AlterDatabasePropertiesOption merges properties into a database.
type AlterDatabasePropertiesOption struct {
	Name       string
	Properties map[string]string
}

// NewAlterDatabasePropertiesOption — Milvus parity.
func NewAlterDatabasePropertiesOption(name string) *AlterDatabasePropertiesOption {
	return &AlterDatabasePropertiesOption{Name: name}
}

// WithProperty sets a property to be added/updated on AlterDatabase.
func (o *AlterDatabasePropertiesOption) WithProperty(key string, value any) *AlterDatabasePropertiesOption {
	if o.Properties == nil {
		o.Properties = make(map[string]string)
	}
	o.Properties[key] = stringify(value)
	return o
}

// DropDatabasePropertiesOption removes property keys from a database.
type DropDatabasePropertiesOption struct {
	Name string
	Keys []string
}

// NewDropDatabasePropertiesOption — Milvus parity. Pass one or more
// property keys to remove.
func NewDropDatabasePropertiesOption(name string, keys ...string) *DropDatabasePropertiesOption {
	return &DropDatabasePropertiesOption{Name: name, Keys: keys}
}
