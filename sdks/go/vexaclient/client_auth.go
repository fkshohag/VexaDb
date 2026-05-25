package vexaclient

import (
	"context"
	"net/http"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// ---- Authentication: login / tokens ----

// Login verifies a user/password and mints an API token (and returns it).
// The returned `Token` should be passed back as the `APIKey` for subsequent
// requests. The plaintext secret is shown ONCE.
func (c *Client) Login(ctx context.Context, opt *LoginOption) (*TokenInfo, error) {
	body := map[string]any{"username": opt.Username, "password": opt.Password}
	var out TokenInfo
	if err := c.do(ctx, http.MethodPost, "/v1/auth/login", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateToken mints an API token for the currently authenticated user.
func (c *Client) CreateToken(ctx context.Context, opt *CreateTokenOption) (*TokenInfo, error) {
	body := map[string]any{"description": opt.Description}
	var out TokenInfo
	if err := c.do(ctx, http.MethodPost, "/v1/auth/tokens", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokeToken disables an API token by id.
func (c *Client) RevokeToken(ctx context.Context, opt *RevokeTokenOption) error {
	return c.do(ctx, http.MethodDelete, "/v1/auth/tokens/"+opt.ID, nil, nil)
}

// ---- Users ----

func (c *Client) CreateUser(ctx context.Context, opt *CreateUserOption) error {
	body := map[string]any{"name": opt.Name, "password": opt.Password}
	if len(opt.Roles) > 0 {
		body["roles"] = opt.Roles
	}
	return c.do(ctx, http.MethodPost, "/v1/users", body, nil)
}

func (c *Client) UpdatePassword(ctx context.Context, opt *UpdatePasswordOption) error {
	body := map[string]any{"old": opt.OldPassword, "new": opt.NewPassword}
	return c.do(ctx, http.MethodPatch, "/v1/users/"+opt.Name+"/password", body, nil)
}

func (c *Client) DropUser(ctx context.Context, opt *DropUserOption) error {
	return c.do(ctx, http.MethodDelete, "/v1/users/"+opt.Name, nil, nil)
}

func (c *Client) ListUsers(ctx context.Context) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/v1/users", nil, &out)
	return out, err
}

func (c *Client) DescribeUser(ctx context.Context, opt *DescribeUserOption) (*entity.User, error) {
	var out entity.User
	if err := c.do(ctx, http.MethodGet, "/v1/users/"+opt.Name, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---- Roles ----

func (c *Client) CreateRole(ctx context.Context, opt *CreateRoleOption) error {
	body := map[string]any{"name": opt.Name, "description": opt.Description}
	return c.do(ctx, http.MethodPost, "/v1/roles", body, nil)
}

func (c *Client) DropRole(ctx context.Context, opt *DropRoleOption) error {
	return c.do(ctx, http.MethodDelete, "/v1/roles/"+opt.Name, nil, nil)
}

func (c *Client) ListRoles(ctx context.Context) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/v1/roles", nil, &out)
	return out, err
}

func (c *Client) DescribeRole(ctx context.Context, opt *DescribeRoleOption) (*entity.Role, error) {
	var out entity.Role
	if err := c.do(ctx, http.MethodGet, "/v1/roles/"+opt.Name, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GrantRole(ctx context.Context, opt *GrantRoleOption) error {
	return c.do(ctx, http.MethodPost, "/v1/users/"+opt.User+"/roles/"+opt.Role, nil, nil)
}

func (c *Client) RevokeRole(ctx context.Context, opt *RevokeRoleOption) error {
	return c.do(ctx, http.MethodDelete, "/v1/users/"+opt.User+"/roles/"+opt.Role, nil, nil)
}

// ---- Privileges ----

func (c *Client) GrantPrivilege(ctx context.Context, opt *GrantPrivilegeOption) error {
	body := map[string]any{
		"object_type": opt.ObjectType,
		"object_name": opt.ObjectName,
		"privilege":   opt.Privilege,
	}
	return c.do(ctx, http.MethodPost, "/v1/roles/"+opt.Role+"/grants", body, nil)
}

func (c *Client) RevokePrivilege(ctx context.Context, opt *RevokePrivilegeOption) error {
	body := map[string]any{
		"object_type": opt.ObjectType,
		"object_name": opt.ObjectName,
		"privilege":   opt.Privilege,
	}
	return c.do(ctx, http.MethodDelete, "/v1/roles/"+opt.Role+"/grants", body, nil)
}

// Milvus-style aliases.
func (c *Client) GrantPrivilegeV2(ctx context.Context, opt *GrantPrivilegeOption) error {
	return c.GrantPrivilege(ctx, opt)
}
func (c *Client) RevokePrivilegeV2(ctx context.Context, opt *RevokePrivilegeOption) error {
	return c.RevokePrivilege(ctx, opt)
}

// ---- Privilege groups ----

func (c *Client) CreatePrivilegeGroup(ctx context.Context, opt *CreatePrivilegeGroupOption) error {
	body := map[string]any{"name": opt.Name, "privileges": opt.Privileges}
	return c.do(ctx, http.MethodPost, "/v1/privilege-groups", body, nil)
}

func (c *Client) DropPrivilegeGroup(ctx context.Context, opt *DropPrivilegeGroupOption) error {
	return c.do(ctx, http.MethodDelete, "/v1/privilege-groups/"+opt.Name, nil, nil)
}

func (c *Client) ListPrivilegeGroups(ctx context.Context) ([]entity.PrivilegeGroup, error) {
	var out []entity.PrivilegeGroup
	err := c.do(ctx, http.MethodGet, "/v1/privilege-groups", nil, &out)
	return out, err
}

func (c *Client) OperatePrivilegeGroup(ctx context.Context, opt *OperatePrivilegeGroupOption) error {
	body := map[string]any{}
	if len(opt.Add) > 0 {
		body["add"] = opt.Add
	}
	if len(opt.Remove) > 0 {
		body["remove"] = opt.Remove
	}
	return c.do(ctx, http.MethodPatch, "/v1/privilege-groups/"+opt.Name, body, nil)
}

// Convenience helpers matching Milvus method names.
func (c *Client) AddPrivilegesToGroup(ctx context.Context, name string, privileges ...string) error {
	return c.OperatePrivilegeGroup(ctx, NewOperatePrivilegeGroupOption(name).WithAdd(privileges...))
}

func (c *Client) RemovePrivilegesFromGroup(ctx context.Context, name string, privileges ...string) error {
	return c.OperatePrivilegeGroup(ctx, NewOperatePrivilegeGroupOption(name).WithRemove(privileges...))
}

// ---- RBAC backup / restore ----

// BackupRBAC returns the full RBAC snapshot.
func (c *Client) BackupRBAC(ctx context.Context) (*entity.RBACMeta, error) {
	var out entity.RBACMeta
	if err := c.do(ctx, http.MethodPost, "/v1/admin/rbac/backup", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RestoreRBAC applies a previously exported RBAC snapshot.
func (c *Client) RestoreRBAC(ctx context.Context, meta *entity.RBACMeta) error {
	return c.do(ctx, http.MethodPost, "/v1/admin/rbac/restore", meta, nil)
}
