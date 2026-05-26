use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// The kind of object a privilege applies to.
///
/// `Global` privileges apply to the whole instance (`object_name = "*"`).
/// `Collection` privileges scope to one collection by name (or `"*"` for all).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ObjectType {
    Global,
    Collection,
}

impl ObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectType::Global => "Global",
            ObjectType::Collection => "Collection",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Global" | "global" => Some(ObjectType::Global),
            "Collection" | "collection" => Some(ObjectType::Collection),
            _ => None,
        }
    }
}

/// A named privilege. New names can be added without breaking on-disk RBAC
/// (privilege strings are stored verbatim); unknown names simply never match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Privilege {
    // Global / admin
    CreateCollection,
    DropCollection,
    ListCollections,
    DescribeCollection,
    CollectionStats,
    Reindex,
    Snapshot,
    Rebalance,
    CompactWal,
    ClusterStatus,
    /// Rename a collection or set/drop collection properties (Milvus parity).
    AlterCollection,
    /// Create/drop/reassign collection aliases (Milvus parity).
    AlterAlias,
    // Database management (Milvus parity). All five are global-scoped.
    CreateDatabase,
    DropDatabase,
    ListDatabases,
    DescribeDatabase,
    AlterDatabase,
    // Data plane
    Search,
    Query,
    Insert,
    Upsert,
    Delete,
    Get,
    // RBAC
    ManageRbac,
}

impl Privilege {
    pub const ALL: &'static [Privilege] = &[
        Privilege::CreateCollection,
        Privilege::DropCollection,
        Privilege::ListCollections,
        Privilege::DescribeCollection,
        Privilege::CollectionStats,
        Privilege::Reindex,
        Privilege::Snapshot,
        Privilege::Rebalance,
        Privilege::CompactWal,
        Privilege::ClusterStatus,
        Privilege::AlterCollection,
        Privilege::AlterAlias,
        Privilege::CreateDatabase,
        Privilege::DropDatabase,
        Privilege::ListDatabases,
        Privilege::DescribeDatabase,
        Privilege::AlterDatabase,
        Privilege::Search,
        Privilege::Query,
        Privilege::Insert,
        Privilege::Upsert,
        Privilege::Delete,
        Privilege::Get,
        Privilege::ManageRbac,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Privilege::CreateCollection => "CreateCollection",
            Privilege::DropCollection => "DropCollection",
            Privilege::ListCollections => "ListCollections",
            Privilege::DescribeCollection => "DescribeCollection",
            Privilege::CollectionStats => "CollectionStats",
            Privilege::Reindex => "Reindex",
            Privilege::Snapshot => "Snapshot",
            Privilege::Rebalance => "Rebalance",
            Privilege::CompactWal => "CompactWal",
            Privilege::ClusterStatus => "ClusterStatus",
            Privilege::AlterCollection => "AlterCollection",
            Privilege::AlterAlias => "AlterAlias",
            Privilege::CreateDatabase => "CreateDatabase",
            Privilege::DropDatabase => "DropDatabase",
            Privilege::ListDatabases => "ListDatabases",
            Privilege::DescribeDatabase => "DescribeDatabase",
            Privilege::AlterDatabase => "AlterDatabase",
            Privilege::Search => "Search",
            Privilege::Query => "Query",
            Privilege::Insert => "Insert",
            Privilege::Upsert => "Upsert",
            Privilege::Delete => "Delete",
            Privilege::Get => "Get",
            Privilege::ManageRbac => "ManageRbac",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|p| p.as_str() == s)
    }

    /// Default object type the privilege applies to. Used by the gateway when
    /// no explicit object is provided on a grant.
    pub fn default_object(&self) -> ObjectType {
        match self {
            Privilege::Search
            | Privilege::Query
            | Privilege::Insert
            | Privilege::Upsert
            | Privilege::Delete
            | Privilege::Get
            | Privilege::DescribeCollection
            | Privilege::CollectionStats
            | Privilege::Reindex
            | Privilege::AlterCollection
            | Privilege::AlterAlias => ObjectType::Collection,
            _ => ObjectType::Global,
        }
    }
}

/// A user account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    /// Argon2id PHC hash of the password (empty when only token auth is used).
    #[serde(default)]
    pub password_hash: String,
    pub created_at_ms: u64,
    #[serde(default)]
    pub disabled: bool,
}

/// An API token bound to a user.
///
/// The plaintext token is shown once at creation; only `secret_hash` is
/// persisted. `id` is a stable, short identifier safe to display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub id: String,
    pub user: String,
    pub secret_hash: String,
    pub created_at_ms: u64,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub revoked: bool,
}

/// A role definition. The list of privileges is stored separately in the
/// state as grant rows (see [`GrantItem`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub created_at_ms: u64,
}

/// One grant row: this role can perform `privilege` on `(object_type,
/// object_name)`. `object_name = "*"` matches any object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GrantItem {
    pub object_type: ObjectType,
    pub object_name: String,
    /// Stored as a string so unknown privileges round-trip on backup/restore.
    pub privilege: String,
}

impl GrantItem {
    pub fn new(object_type: ObjectType, object_name: impl Into<String>, privilege: impl Into<String>) -> Self {
        Self {
            object_type,
            object_name: object_name.into(),
            privilege: privilege.into(),
        }
    }

    /// True when this grant authorizes the requested (object, privilege).
    pub fn matches(&self, object: ObjectType, name: &str, privilege: &str) -> bool {
        if self.object_type != object {
            return false;
        }
        if self.object_name != "*" && self.object_name != name {
            return false;
        }
        self.privilege == privilege || self.privilege == "*"
    }
}

/// A view: a role and its expanded grant list (used in BackupRBAC output).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleGrants {
    pub role: String,
    pub grants: Vec<GrantItem>,
}

/// A named bundle of privileges that can be granted as a unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivilegeGroup {
    pub name: String,
    pub privileges: BTreeSet<String>,
}

pub const BUILTIN_ADMIN: &str = "admin";
pub const BUILTIN_READ_WRITE: &str = "read_write";
pub const BUILTIN_READ_ONLY: &str = "read_only";
