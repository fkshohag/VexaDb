//! RBAC primitives for VexaDb.
//!
//! - [`RbacState`] is the in-memory authoritative state (users, tokens, roles,
//!   role assignments, grants, privilege groups). It is constructed empty
//!   and mutated by applying [`RbacOp`]s. The same op stream can be persisted
//!   (WAL) and replicated (Raft) without RBAC knowing about either.
//! - [`Principal`] is the resolved identity for a request, used by
//!   [`RbacState::authorize`] to allow/deny one (object, privilege).
//! - Passwords and API tokens are stored as Argon2id PHC strings; raw secrets
//!   are never persisted.

mod model;
mod ops;
mod password;
mod state;

pub mod grpc;

pub use model::{
    GrantItem, ObjectType, Privilege, PrivilegeGroup, Role, RoleGrants, Token, User, BUILTIN_ADMIN,
    BUILTIN_READ_ONLY, BUILTIN_READ_WRITE,
};
pub use ops::RbacOp;
pub use password::{hash_secret, verify_secret, PasswordError};
pub use grpc::{require_collection, method_priv, MethodPriv, RbacCache, RbacInterceptor};
pub use state::{AuthzError, Principal, RbacError, RbacSnapshot, RbacState};
