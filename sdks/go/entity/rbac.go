package entity

// User is one VexaDb user account returned by DescribeUser.
type User struct {
	UserName    string   `json:"name"`
	Disabled    bool     `json:"disabled"`
	CreatedAtMs uint64   `json:"created_at_ms"`
	Roles       []string `json:"roles"`
}

// GrantItem is one privilege grant attached to a role.
type GrantItem struct {
	ObjectType string `json:"object_type"`
	ObjectName string `json:"object_name"`
	Privilege  string `json:"privilege"`
}

// Role is a role and its expanded privilege list.
type Role struct {
	RoleName    string      `json:"name"`
	Description string      `json:"description"`
	Privileges  []GrantItem `json:"grants"`
}

// PrivilegeGroup is a named bundle of privileges.
type PrivilegeGroup struct {
	GroupName  string   `json:"name"`
	Privileges []string `json:"privileges"`
}

// RBACMeta is the BackupRBAC / RestoreRBAC payload.
type RBACMeta struct {
	Users           []RBACUser       `json:"users"`
	Tokens          []RBACToken      `json:"tokens"`
	Roles           []RBACRole       `json:"roles"`
	UserRoles       [][2]string      `json:"user_roles"`
	RoleGrants      []RBACRoleGrants `json:"role_grants"`
	PrivilegeGroups []PrivilegeGroup `json:"privilege_groups"`
}

type RBACUser struct {
	Name         string `json:"name"`
	PasswordHash string `json:"password_hash"`
	CreatedAtMs  uint64 `json:"created_at_ms"`
	Disabled     bool   `json:"disabled"`
}

type RBACToken struct {
	ID          string `json:"id"`
	User        string `json:"user"`
	SecretHash  string `json:"secret_hash"`
	CreatedAtMs uint64 `json:"created_at_ms"`
	Description string `json:"description"`
	Revoked     bool   `json:"revoked"`
}

type RBACRole struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAtMs uint64 `json:"created_at_ms"`
}

type RBACRoleGrants struct {
	Role   string      `json:"role"`
	Grants []GrantItem `json:"grants"`
}

// Built-in role names.
const (
	RoleAdmin     = "admin"
	RoleReadWrite = "read_write"
	RoleReadOnly  = "read_only"
)

// Object types.
const (
	ObjectGlobal     = "Global"
	ObjectCollection = "Collection"
)
