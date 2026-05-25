use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::model::{
    GrantItem, ObjectType, PrivilegeGroup, Role, RoleGrants, Token, User, BUILTIN_ADMIN,
    BUILTIN_READ_ONLY, BUILTIN_READ_WRITE,
};
use crate::ops::RbacOp;
use crate::password::verify_secret;

#[derive(Debug, Error)]
pub enum RbacError {
    #[error("user already exists: {0}")]
    UserExists(String),
    #[error("user not found: {0}")]
    UserNotFound(String),
    #[error("role already exists: {0}")]
    RoleExists(String),
    #[error("role not found: {0}")]
    RoleNotFound(String),
    #[error("token already exists: {0}")]
    TokenExists(String),
    #[error("token not found: {0}")]
    TokenNotFound(String),
    #[error("privilege group already exists: {0}")]
    GroupExists(String),
    #[error("privilege group not found: {0}")]
    GroupNotFound(String),
    #[error("cannot drop built-in role: {0}")]
    BuiltinRole(String),
    #[error("cannot drop last admin user")]
    LastAdmin,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthzError {
    #[error("authentication required")]
    Unauthenticated,
    #[error("user is disabled")]
    Disabled,
    #[error("no privilege {privilege} on {object_type:?}:{object_name}")]
    Forbidden {
        privilege: String,
        object_type: ObjectType,
        object_name: String,
    },
}

/// Resolved identity for a request.
#[derive(Debug, Clone)]
pub struct Principal {
    pub user: String,
    pub roles: BTreeSet<String>,
    pub is_superuser: bool,
}

impl Principal {
    /// Synthetic principal for `VECTORDB_API_KEYS` users (legacy compat).
    pub fn superuser_token(name: impl Into<String>) -> Self {
        Self {
            user: name.into(),
            roles: BTreeSet::new(),
            is_superuser: true,
        }
    }
}

/// Snapshot of the full RBAC state — used by BackupRBAC / RestoreRBAC.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RbacSnapshot {
    pub users: Vec<User>,
    pub tokens: Vec<Token>,
    pub roles: Vec<Role>,
    pub user_roles: Vec<(String, String)>,
    pub role_grants: Vec<RoleGrants>,
    pub privilege_groups: Vec<PrivilegeGroup>,
}

/// In-memory authoritative state.
#[derive(Debug, Default)]
pub struct RbacState {
    users: BTreeMap<String, User>,
    tokens: BTreeMap<String, Token>,
    roles: BTreeMap<String, Role>,
    /// user → set of role names
    user_roles: BTreeMap<String, BTreeSet<String>>,
    /// role → set of grant items
    role_grants: BTreeMap<String, BTreeSet<GrantItem>>,
    privilege_groups: BTreeMap<String, PrivilegeGroup>,
}

impl RbacState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed the built-in roles (idempotent). Call after construction or after
    /// rehydrating from snapshot to make sure `admin`, `read_write`, and
    /// `read_only` always exist.
    pub fn ensure_builtin_roles(&mut self, now_ms: u64) {
        for name in [BUILTIN_ADMIN, BUILTIN_READ_WRITE, BUILTIN_READ_ONLY] {
            self.roles
                .entry(name.to_string())
                .or_insert_with(|| Role {
                    name: name.to_string(),
                    description: format!("built-in {name} role"),
                    created_at_ms: now_ms,
                });
        }

        let admin_grants = self.role_grants.entry(BUILTIN_ADMIN.to_string()).or_default();
        admin_grants.insert(GrantItem::new(ObjectType::Global, "*", "*"));
        admin_grants.insert(GrantItem::new(ObjectType::Collection, "*", "*"));

        let rw = self.role_grants.entry(BUILTIN_READ_WRITE.to_string()).or_default();
        for p in [
            "DescribeCollection",
            "CollectionStats",
            "ListCollections",
            "Search",
            "Query",
            "Insert",
            "Upsert",
            "Delete",
            "Get",
        ] {
            let obj = match p {
                "ListCollections" => ObjectType::Global,
                _ => ObjectType::Collection,
            };
            rw.insert(GrantItem::new(obj, "*", p));
        }

        let ro = self.role_grants.entry(BUILTIN_READ_ONLY.to_string()).or_default();
        for p in [
            "DescribeCollection",
            "CollectionStats",
            "ListCollections",
            "Search",
            "Query",
            "Get",
        ] {
            let obj = match p {
                "ListCollections" => ObjectType::Global,
                _ => ObjectType::Collection,
            };
            ro.insert(GrantItem::new(obj, "*", p));
        }
    }

    /// Apply one mutation. Idempotent where it makes sense (re-grant of a
    /// privilege the role already has is a no-op).
    pub fn apply(&mut self, op: RbacOp) -> Result<(), RbacError> {
        match op {
            RbacOp::CreateUser { name, password_hash, created_at_ms } => {
                if self.users.contains_key(&name) {
                    return Err(RbacError::UserExists(name));
                }
                self.users.insert(
                    name.clone(),
                    User { name, password_hash, created_at_ms, disabled: false },
                );
            }
            RbacOp::UpdatePassword { name, password_hash } => {
                let u = self.users.get_mut(&name).ok_or(RbacError::UserNotFound(name.clone()))?;
                u.password_hash = password_hash;
            }
            RbacOp::DropUser { name } => {
                if self.is_only_admin(&name) {
                    return Err(RbacError::LastAdmin);
                }
                self.users.remove(&name).ok_or(RbacError::UserNotFound(name.clone()))?;
                self.user_roles.remove(&name);
                self.tokens.retain(|_, t| t.user != name);
            }
            RbacOp::SetUserDisabled { name, disabled } => {
                if disabled && self.is_only_admin(&name) {
                    return Err(RbacError::LastAdmin);
                }
                let u = self.users.get_mut(&name).ok_or(RbacError::UserNotFound(name.clone()))?;
                u.disabled = disabled;
            }
            RbacOp::CreateToken { id, user, secret_hash, created_at_ms, description } => {
                if !self.users.contains_key(&user) {
                    return Err(RbacError::UserNotFound(user));
                }
                if self.tokens.contains_key(&id) {
                    return Err(RbacError::TokenExists(id));
                }
                self.tokens.insert(
                    id.clone(),
                    Token { id, user, secret_hash, created_at_ms, description, revoked: false },
                );
            }
            RbacOp::RevokeToken { id } => {
                let t = self.tokens.get_mut(&id).ok_or(RbacError::TokenNotFound(id.clone()))?;
                t.revoked = true;
            }
            RbacOp::CreateRole { name, description, created_at_ms } => {
                if self.roles.contains_key(&name) {
                    return Err(RbacError::RoleExists(name));
                }
                self.roles.insert(
                    name.clone(),
                    Role { name, description, created_at_ms },
                );
            }
            RbacOp::DropRole { name } => {
                if matches!(name.as_str(), BUILTIN_ADMIN | BUILTIN_READ_WRITE | BUILTIN_READ_ONLY) {
                    return Err(RbacError::BuiltinRole(name));
                }
                self.roles.remove(&name).ok_or(RbacError::RoleNotFound(name.clone()))?;
                self.role_grants.remove(&name);
                for roles in self.user_roles.values_mut() {
                    roles.remove(&name);
                }
            }
            RbacOp::GrantRole { user, role } => {
                if !self.users.contains_key(&user) {
                    return Err(RbacError::UserNotFound(user));
                }
                if !self.roles.contains_key(&role) {
                    return Err(RbacError::RoleNotFound(role));
                }
                self.user_roles.entry(user).or_default().insert(role);
            }
            RbacOp::RevokeRole { user, role } => {
                if let Some(roles) = self.user_roles.get_mut(&user) {
                    roles.remove(&role);
                }
            }
            RbacOp::GrantPrivilege { role, grant } => {
                if !self.roles.contains_key(&role) {
                    return Err(RbacError::RoleNotFound(role));
                }
                self.role_grants.entry(role).or_default().insert(grant);
            }
            RbacOp::RevokePrivilege { role, grant } => {
                if let Some(g) = self.role_grants.get_mut(&role) {
                    g.remove(&grant);
                }
            }
            RbacOp::CreatePrivilegeGroup { name, privileges } => {
                if self.privilege_groups.contains_key(&name) {
                    return Err(RbacError::GroupExists(name));
                }
                self.privilege_groups.insert(
                    name.clone(),
                    PrivilegeGroup { name, privileges: privileges.into_iter().collect() },
                );
            }
            RbacOp::DropPrivilegeGroup { name } => {
                self.privilege_groups.remove(&name).ok_or(RbacError::GroupNotFound(name))?;
            }
            RbacOp::AddPrivilegesToGroup { name, privileges } => {
                let g = self.privilege_groups.get_mut(&name).ok_or(RbacError::GroupNotFound(name.clone()))?;
                g.privileges.extend(privileges);
            }
            RbacOp::RemovePrivilegesFromGroup { name, privileges } => {
                let g = self.privilege_groups.get_mut(&name).ok_or(RbacError::GroupNotFound(name.clone()))?;
                for p in privileges {
                    g.privileges.remove(&p);
                }
            }
            RbacOp::GrantPrivilegeGroup { role, group, object_type, object_name } => {
                if !self.roles.contains_key(&role) {
                    return Err(RbacError::RoleNotFound(role));
                }
                let g = self.privilege_groups.get(&group).ok_or(RbacError::GroupNotFound(group.clone()))?;
                let entry = self.role_grants.entry(role).or_default();
                for p in &g.privileges {
                    entry.insert(GrantItem::new(object_type, object_name.clone(), p.clone()));
                }
            }
        }
        Ok(())
    }

    fn is_only_admin(&self, name: &str) -> bool {
        let mut admin_count = 0usize;
        for (user, roles) in &self.user_roles {
            if roles.contains(BUILTIN_ADMIN) {
                if user != name {
                    return false;
                }
                admin_count += 1;
            }
        }
        admin_count > 0
    }

    /// Look up a user. Returns `None` if not present.
    pub fn get_user(&self, name: &str) -> Option<&User> {
        self.users.get(name)
    }

    pub fn list_users(&self) -> Vec<String> {
        self.users.keys().cloned().collect()
    }

    pub fn list_roles(&self) -> Vec<String> {
        self.roles.keys().cloned().collect()
    }

    pub fn list_tokens(&self, user: &str) -> Vec<&Token> {
        self.tokens.values().filter(|t| t.user == user && !t.revoked).collect()
    }

    pub fn list_all_tokens(&self) -> Vec<&Token> {
        self.tokens.values().collect()
    }

    pub fn user_roles(&self, user: &str) -> BTreeSet<String> {
        self.user_roles.get(user).cloned().unwrap_or_default()
    }

    pub fn role_grants(&self, role: &str) -> Vec<GrantItem> {
        self.role_grants
            .get(role)
            .map(|g| g.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn role(&self, name: &str) -> Option<&Role> {
        self.roles.get(name)
    }

    pub fn token(&self, id: &str) -> Option<&Token> {
        self.tokens.get(id)
    }

    pub fn list_privilege_groups(&self) -> Vec<&PrivilegeGroup> {
        self.privilege_groups.values().collect()
    }

    pub fn privilege_group(&self, name: &str) -> Option<&PrivilegeGroup> {
        self.privilege_groups.get(name)
    }

    /// Verify a username + password and resolve a [`Principal`].
    pub fn authenticate_password(&self, user: &str, password: &str) -> Result<Principal, AuthzError> {
        let u = self.users.get(user).ok_or(AuthzError::Unauthenticated)?;
        if u.disabled {
            return Err(AuthzError::Disabled);
        }
        match verify_secret(password, &u.password_hash) {
            Ok(true) => Ok(self.principal_for(user)),
            _ => Err(AuthzError::Unauthenticated),
        }
    }

    /// Verify an API token (`token_id:secret`) and resolve a [`Principal`].
    pub fn authenticate_token(&self, raw: &str) -> Result<Principal, AuthzError> {
        let (id, secret) = raw.split_once(':').ok_or(AuthzError::Unauthenticated)?;
        let t = self.tokens.get(id).ok_or(AuthzError::Unauthenticated)?;
        if t.revoked {
            return Err(AuthzError::Unauthenticated);
        }
        let u = self.users.get(&t.user).ok_or(AuthzError::Unauthenticated)?;
        if u.disabled {
            return Err(AuthzError::Disabled);
        }
        match verify_secret(secret, &t.secret_hash) {
            Ok(true) => Ok(self.principal_for(&t.user)),
            _ => Err(AuthzError::Unauthenticated),
        }
    }

    /// Authorize a request. `is_superuser` bypasses all checks.
    pub fn authorize(
        &self,
        principal: &Principal,
        object_type: ObjectType,
        object_name: &str,
        privilege: &str,
    ) -> Result<(), AuthzError> {
        if principal.is_superuser {
            return Ok(());
        }
        for role in &principal.roles {
            if let Some(grants) = self.role_grants.get(role) {
                if grants.iter().any(|g| g.matches(object_type, object_name, privilege)) {
                    return Ok(());
                }
            }
        }
        Err(AuthzError::Forbidden {
            privilege: privilege.to_string(),
            object_type,
            object_name: object_name.to_string(),
        })
    }

    fn principal_for(&self, user: &str) -> Principal {
        let roles = self.user_roles(user);
        let is_superuser = roles.contains(BUILTIN_ADMIN);
        Principal {
            user: user.to_string(),
            roles,
            is_superuser,
        }
    }

    /// Export the full state.
    pub fn snapshot(&self) -> RbacSnapshot {
        let mut user_roles = Vec::new();
        for (u, roles) in &self.user_roles {
            for r in roles {
                user_roles.push((u.clone(), r.clone()));
            }
        }
        let role_grants = self
            .role_grants
            .iter()
            .map(|(role, grants)| RoleGrants {
                role: role.clone(),
                grants: grants.iter().cloned().collect(),
            })
            .collect();
        RbacSnapshot {
            users: self.users.values().cloned().collect(),
            tokens: self.tokens.values().cloned().collect(),
            roles: self.roles.values().cloned().collect(),
            user_roles,
            role_grants,
            privilege_groups: self.privilege_groups.values().cloned().collect(),
        }
    }

    /// Replace the state with `snap`.
    pub fn restore(&mut self, snap: RbacSnapshot) {
        *self = Self::default();
        for u in snap.users {
            self.users.insert(u.name.clone(), u);
        }
        for t in snap.tokens {
            self.tokens.insert(t.id.clone(), t);
        }
        for r in snap.roles {
            self.roles.insert(r.name.clone(), r);
        }
        for (u, r) in snap.user_roles {
            self.user_roles.entry(u).or_default().insert(r);
        }
        for rg in snap.role_grants {
            let set = self.role_grants.entry(rg.role).or_default();
            for g in rg.grants {
                set.insert(g);
            }
        }
        for g in snap.privilege_groups {
            self.privilege_groups.insert(g.name.clone(), g);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::password::hash_secret;

    fn new_state() -> RbacState {
        let mut s = RbacState::new();
        s.ensure_builtin_roles(1);
        s
    }

    #[test]
    fn create_user_and_authenticate() {
        let mut s = new_state();
        let h = hash_secret("hunter2").unwrap();
        s.apply(RbacOp::CreateUser { name: "alice".into(), password_hash: h, created_at_ms: 1 }).unwrap();
        s.apply(RbacOp::GrantRole { user: "alice".into(), role: BUILTIN_READ_WRITE.into() }).unwrap();

        let p = s.authenticate_password("alice", "hunter2").unwrap();
        assert!(s.authorize(&p, ObjectType::Collection, "books", "Search").is_ok());
        assert!(s.authorize(&p, ObjectType::Global, "*", "CreateCollection").is_err());
    }

    #[test]
    fn admin_bypasses_grants() {
        let mut s = new_state();
        let h = hash_secret("root").unwrap();
        s.apply(RbacOp::CreateUser { name: "root".into(), password_hash: h, created_at_ms: 1 }).unwrap();
        s.apply(RbacOp::GrantRole { user: "root".into(), role: BUILTIN_ADMIN.into() }).unwrap();
        let p = s.authenticate_password("root", "root").unwrap();
        assert!(p.is_superuser);
        assert!(s.authorize(&p, ObjectType::Global, "*", "AnythingGoes").is_ok());
    }

    #[test]
    fn token_auth() {
        let mut s = new_state();
        let h = hash_secret("pw").unwrap();
        s.apply(RbacOp::CreateUser { name: "u".into(), password_hash: h, created_at_ms: 1 }).unwrap();
        s.apply(RbacOp::GrantRole { user: "u".into(), role: BUILTIN_READ_ONLY.into() }).unwrap();
        let tok = hash_secret("tok-secret").unwrap();
        s.apply(RbacOp::CreateToken {
            id: "tok1".into(),
            user: "u".into(),
            secret_hash: tok,
            created_at_ms: 1,
            description: "test".into(),
        }).unwrap();

        let p = s.authenticate_token("tok1:tok-secret").unwrap();
        assert_eq!(p.user, "u");
        assert!(s.authorize(&p, ObjectType::Collection, "x", "Search").is_ok());
        assert!(s.authorize(&p, ObjectType::Collection, "x", "Insert").is_err());

        s.apply(RbacOp::RevokeToken { id: "tok1".into() }).unwrap();
        assert!(s.authenticate_token("tok1:tok-secret").is_err());
    }

    #[test]
    fn last_admin_protected() {
        let mut s = new_state();
        let h = hash_secret("x").unwrap();
        s.apply(RbacOp::CreateUser { name: "root".into(), password_hash: h, created_at_ms: 1 }).unwrap();
        s.apply(RbacOp::GrantRole { user: "root".into(), role: BUILTIN_ADMIN.into() }).unwrap();
        assert!(matches!(s.apply(RbacOp::DropUser { name: "root".into() }), Err(RbacError::LastAdmin)));
    }

    #[test]
    fn snapshot_roundtrip() {
        let mut s = new_state();
        let h = hash_secret("x").unwrap();
        s.apply(RbacOp::CreateUser { name: "a".into(), password_hash: h, created_at_ms: 1 }).unwrap();
        s.apply(RbacOp::CreateRole { name: "auditor".into(), description: "".into(), created_at_ms: 1 }).unwrap();
        s.apply(RbacOp::GrantPrivilege {
            role: "auditor".into(),
            grant: GrantItem::new(ObjectType::Collection, "*", "Search"),
        }).unwrap();
        s.apply(RbacOp::GrantRole { user: "a".into(), role: "auditor".into() }).unwrap();

        let snap = s.snapshot();
        let mut s2 = RbacState::new();
        s2.restore(snap);
        s2.ensure_builtin_roles(1);
        assert!(s2.role("auditor").is_some());
        assert_eq!(s2.user_roles("a").iter().collect::<Vec<_>>(), vec![&"auditor".to_string()]);
    }
}
