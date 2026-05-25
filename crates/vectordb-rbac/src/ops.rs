use serde::{Deserialize, Serialize};

use crate::model::{GrantItem, ObjectType};

/// One mutation against the RBAC state.
///
/// Carried through the existing WAL/Raft so all nodes converge on the same
/// users, tokens, roles, and grants. Variant order matters for bincode WAL
/// compatibility: only append at the end.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RbacOp {
    CreateUser {
        name: String,
        password_hash: String,
        created_at_ms: u64,
    },
    UpdatePassword {
        name: String,
        password_hash: String,
    },
    DropUser {
        name: String,
    },
    SetUserDisabled {
        name: String,
        disabled: bool,
    },
    CreateToken {
        id: String,
        user: String,
        secret_hash: String,
        created_at_ms: u64,
        description: String,
    },
    RevokeToken {
        id: String,
    },
    CreateRole {
        name: String,
        description: String,
        created_at_ms: u64,
    },
    DropRole {
        name: String,
    },
    GrantRole {
        user: String,
        role: String,
    },
    RevokeRole {
        user: String,
        role: String,
    },
    GrantPrivilege {
        role: String,
        grant: GrantItem,
    },
    RevokePrivilege {
        role: String,
        grant: GrantItem,
    },
    CreatePrivilegeGroup {
        name: String,
        privileges: Vec<String>,
    },
    DropPrivilegeGroup {
        name: String,
    },
    AddPrivilegesToGroup {
        name: String,
        privileges: Vec<String>,
    },
    RemovePrivilegesFromGroup {
        name: String,
        privileges: Vec<String>,
    },
    /// Grant every privilege in a group to a role (expanded on apply so the
    /// grant set stays explicit and removing the group later doesn't strip
    /// the privileges).
    GrantPrivilegeGroup {
        role: String,
        group: String,
        object_type: ObjectType,
        object_name: String,
    },
}
