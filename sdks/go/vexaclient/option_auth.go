package vexaclient

import "github.com/vectordb/vectordb/sdks/go/entity"

// CreateUserOption — POST /v1/users
type CreateUserOption struct {
	Name     string
	Password string
	Roles    []string
}

func NewCreateUserOption(name, password string) *CreateUserOption {
	return &CreateUserOption{Name: name, Password: password}
}

func (o *CreateUserOption) WithRoles(roles ...string) *CreateUserOption {
	o.Roles = roles
	return o
}

// UpdatePasswordOption — PATCH /v1/users/:name/password
type UpdatePasswordOption struct {
	Name        string
	OldPassword string
	NewPassword string
}

func NewUpdatePasswordOption(name, oldPassword, newPassword string) *UpdatePasswordOption {
	return &UpdatePasswordOption{Name: name, OldPassword: oldPassword, NewPassword: newPassword}
}

// DropUserOption — DELETE /v1/users/:name
type DropUserOption struct{ Name string }

func NewDropUserOption(name string) *DropUserOption { return &DropUserOption{Name: name} }

// DescribeUserOption — GET /v1/users/:name
type DescribeUserOption struct{ Name string }

func NewDescribeUserOption(name string) *DescribeUserOption { return &DescribeUserOption{Name: name} }

// CreateRoleOption — POST /v1/roles
type CreateRoleOption struct {
	Name        string
	Description string
}

func NewCreateRoleOption(name string) *CreateRoleOption {
	return &CreateRoleOption{Name: name}
}

func (o *CreateRoleOption) WithDescription(d string) *CreateRoleOption {
	o.Description = d
	return o
}

// DropRoleOption — DELETE /v1/roles/:name
type DropRoleOption struct{ Name string }

func NewDropRoleOption(name string) *DropRoleOption { return &DropRoleOption{Name: name} }

// DescribeRoleOption — GET /v1/roles/:name
type DescribeRoleOption struct{ Name string }

func NewDescribeRoleOption(name string) *DescribeRoleOption { return &DescribeRoleOption{Name: name} }

// GrantRoleOption / RevokeRoleOption — assign role to user
type GrantRoleOption struct {
	User string
	Role string
}

func NewGrantRoleOption(user, role string) *GrantRoleOption {
	return &GrantRoleOption{User: user, Role: role}
}

type RevokeRoleOption GrantRoleOption

func NewRevokeRoleOption(user, role string) *RevokeRoleOption {
	return &RevokeRoleOption{User: user, Role: role}
}

// GrantPrivilegeOption — POST /v1/roles/:role/grants
type GrantPrivilegeOption struct {
	Role       string
	ObjectType string
	Privilege  string
	ObjectName string
}

func NewGrantPrivilegeOption(role, objectType, privilege, objectName string) *GrantPrivilegeOption {
	return &GrantPrivilegeOption{
		Role:       role,
		ObjectType: objectType,
		Privilege:  privilege,
		ObjectName: objectName,
	}
}

type RevokePrivilegeOption GrantPrivilegeOption

func NewRevokePrivilegeOption(role, objectType, privilege, objectName string) *RevokePrivilegeOption {
	return &RevokePrivilegeOption{
		Role:       role,
		ObjectType: objectType,
		Privilege:  privilege,
		ObjectName: objectName,
	}
}

// CreatePrivilegeGroupOption — POST /v1/privilege-groups
type CreatePrivilegeGroupOption struct {
	Name       string
	Privileges []string
}

func NewCreatePrivilegeGroupOption(name string) *CreatePrivilegeGroupOption {
	return &CreatePrivilegeGroupOption{Name: name}
}

func (o *CreatePrivilegeGroupOption) WithPrivileges(p ...string) *CreatePrivilegeGroupOption {
	o.Privileges = p
	return o
}

// DropPrivilegeGroupOption
type DropPrivilegeGroupOption struct{ Name string }

func NewDropPrivilegeGroupOption(name string) *DropPrivilegeGroupOption {
	return &DropPrivilegeGroupOption{Name: name}
}

// OperatePrivilegeGroupOption — PATCH /v1/privilege-groups/:name
type OperatePrivilegeGroupOption struct {
	Name   string
	Add    []string
	Remove []string
}

func NewOperatePrivilegeGroupOption(name string) *OperatePrivilegeGroupOption {
	return &OperatePrivilegeGroupOption{Name: name}
}

func (o *OperatePrivilegeGroupOption) WithAdd(p ...string) *OperatePrivilegeGroupOption {
	o.Add = p
	return o
}

func (o *OperatePrivilegeGroupOption) WithRemove(p ...string) *OperatePrivilegeGroupOption {
	o.Remove = p
	return o
}

// TokenInfo is the response from Login / CreateToken.
type TokenInfo struct {
	TokenID string `json:"token_id"`
	Token   string `json:"token"`
	User    string `json:"user"`
}

// LoginOption — POST /v1/auth/login
type LoginOption struct {
	Username string
	Password string
}

func NewLoginOption(username, password string) *LoginOption {
	return &LoginOption{Username: username, Password: password}
}

// CreateTokenOption — POST /v1/auth/tokens
type CreateTokenOption struct {
	Description string
}

func NewCreateTokenOption() *CreateTokenOption { return &CreateTokenOption{} }

func (o *CreateTokenOption) WithDescription(d string) *CreateTokenOption {
	o.Description = d
	return o
}

// RevokeTokenOption — DELETE /v1/auth/tokens/:id
type RevokeTokenOption struct{ ID string }

func NewRevokeTokenOption(id string) *RevokeTokenOption { return &RevokeTokenOption{ID: id} }

// helper so SDK call sites can ignore Object type constants when building grants
var (
	_ = entity.ObjectGlobal
	_ = entity.ObjectCollection
)
