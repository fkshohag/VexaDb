//! gRPC authentication interceptor and per-request authorization helpers.
//!
//! Shared by data nodes and routers. State is held in [`RbacCache`]; nodes
//! with a local engine refresh it directly, routers pull snapshots over gRPC.

use std::sync::Arc;

use base64::Engine as _;
use parking_lot::RwLock;
use tonic::{Request, Status};

use vectordb_auth::{HEADER_API_KEY, HEADER_AUTHORIZATION};

use crate::{
    AuthzError, ObjectType, Principal, Privilege, RbacSnapshot, RbacState,
};

/// In-memory mirror of the cluster RBAC state (cheap to clone).
#[derive(Clone)]
pub struct RbacCache {
    state: Arc<RwLock<RbacState>>,
    legacy_keys: Arc<Vec<String>>,
    enabled: Arc<RwLock<bool>>,
}

impl RbacCache {
    pub fn new(legacy_keys: Vec<String>) -> Self {
        let enabled = !legacy_keys.is_empty();
        Self {
            state: Arc::new(RwLock::new(RbacState::new())),
            legacy_keys: Arc::new(legacy_keys),
            enabled: Arc::new(RwLock::new(enabled)),
        }
    }

    pub fn enabled(&self) -> bool {
        *self.enabled.read()
    }

    fn refresh_enabled(&self, has_users: bool) {
        let mut e = self.enabled.write();
        if !*e && (has_users || !self.legacy_keys.is_empty()) {
            *e = true;
        }
    }

    pub fn install(&self, snap: RbacSnapshot) {
        let has_users = !snap.users.is_empty();
        self.state.write().restore(snap);
        self.refresh_enabled(has_users);
    }

    fn legacy_match(&self, raw: &str) -> bool {
        self.legacy_keys.iter().any(|k| k == raw)
    }

    pub fn resolve(&self, md: &tonic::metadata::MetadataMap) -> Result<Option<Principal>, AuthzError> {
        let api_key = md.get(HEADER_API_KEY).and_then(|v| v.to_str().ok());
        let authz = md.get(HEADER_AUTHORIZATION).and_then(|v| v.to_str().ok());

        if let Some(raw) = api_key.filter(|s| !s.is_empty()) {
            return self.resolve_token_or_legacy(raw).map(Some);
        }

        if let Some(v) = authz {
            let v = v.trim();
            if let Some(b64) = v.strip_prefix("Basic ") {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(b64.trim())
                    .map_err(|_| AuthzError::Unauthenticated)?;
                let s = std::str::from_utf8(&decoded).map_err(|_| AuthzError::Unauthenticated)?;
                let (user, pw) = s.split_once(':').ok_or(AuthzError::Unauthenticated)?;
                return self.state.read().authenticate_password(user, pw).map(Some);
            }
            if let Some(rest) = v.strip_prefix("Bearer ") {
                return self.resolve_token_or_legacy(rest.trim()).map(Some);
            }
        }
        Ok(None)
    }

    pub fn resolve_token_or_legacy(&self, raw: &str) -> Result<Principal, AuthzError> {
        if self.legacy_match(raw) {
            return Ok(Principal::superuser_token("legacy-api-key"));
        }
        self.state.read().authenticate_token(raw)
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        object_type: ObjectType,
        object_name: &str,
        privilege: &str,
    ) -> Result<(), AuthzError> {
        self.state
            .read()
            .authorize(principal, object_type, object_name, privilege)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MethodPriv {
    pub privilege: Privilege,
    pub global_only: bool,
}

pub fn method_priv(method: &str) -> Option<MethodPriv> {
    use Privilege::*;
    let p = |privilege: Privilege, global_only: bool| Some(MethodPriv { privilege, global_only });
    match method {
        "Health" => None,
        "CreateCollection" => p(CreateCollection, true),
        "ListCollections" => p(ListCollections, true),
        "CreateSnapshot" | "ListSnapshots" | "DeleteSnapshot" => p(Snapshot, true),
        "CompactWal" => p(CompactWal, true),
        "Rebalance" | "RebalanceStatus" => p(Rebalance, true),
        "RegisterNode" | "ClusterStatus" => p(ClusterStatus, true),
        "ApplyRbac" | "GetRbacSnapshot" => p(ManageRbac, true),
        "DeleteCollection" => p(DropCollection, false),
        "DescribeCollection" => p(DescribeCollection, false),
        "Stats" => p(CollectionStats, false),
        "ReindexCollection" => p(Reindex, false),
        "Upsert" => p(Upsert, false),
        "BulkUpsert" | "ImportStream" => p(Insert, false),
        "Search" => p(Search, false),
        "Query" | "Scroll" => p(Query, false),
        "Delete" => p(Delete, false),
        "Get" => p(Get, false),
        // Collection-meta RPCs do their own per-op authorization in the
        // server handler (the affected collection depends on the op kind),
        // so the global interceptor only checks that the user is
        // authenticated.
        "MutateCollectionMeta" => None,
        "ListAliases" | "DescribeAlias" => p(DescribeCollection, true),
        // Database management — all global-scoped privileges (per project
        // decision; per-database object grants can be added later).
        "CreateDatabase" => p(CreateDatabase, true),
        "DropDatabase" => p(DropDatabase, true),
        "ListDatabases" => p(ListDatabases, true),
        "DescribeDatabase" => p(DescribeDatabase, true),
        "AlterDatabase" => p(AlterDatabase, true),
        // Management: indexes / flush / compact / segments (Milvus parity).
        // Index ops do per-collection RBAC checks in the server handler
        // since the affected collection is encoded inside the MetaOp JSON.
        "AddPayloadIndex" | "DropPayloadIndex" => None,
        "FlushCollection" => p(Flush, false),
        "CompactCollection" => p(Compact, false),
        // GetCompactionState targets a job ID (not a collection), so the
        // privilege is global to mirror CompactWal.
        "GetCompactionState" => p(GetCompactionState, true),
        "ListPersistentSegments" => p(GetPersistentSegmentInfo, false),
        // Partitions: Create/Drop encode the collection inside the MetaOp JSON
        // so the server handler resolves the privilege per-call. Read-side RPCs
        // resolve directly off `collection` in the request.
        "CreatePartition" | "DropPartition" => None,
        "HasPartition" => p(DescribePartition, false),
        "ListPartitions" => p(ShowPartitions, false),
        "GetPartitionStats" => p(GetPartitionStatistics, false),
        // Resource groups (Milvus parity, all global).
        "CreateResourceGroup" => p(CreateResourceGroup, true),
        "DropResourceGroup" => p(DropResourceGroup, true),
        "UpdateResourceGroup" => p(UpdateResourceGroup, true),
        "ListResourceGroups" => p(ListResourceGroups, true),
        "DescribeResourceGroup" => p(DescribeResourceGroup, true),
        "DescribeReplica" => p(DescribeReplica, true),
        "TransferReplica" => p(TransferReplica, true),
        _ => None,
    }
}

#[derive(Clone)]
pub struct RbacInterceptor {
    cache: RbacCache,
    exempt_health: bool,
}

impl RbacInterceptor {
    pub fn new(cache: RbacCache, exempt_health: bool) -> Self {
        Self { cache, exempt_health }
    }
}

impl tonic::service::Interceptor for RbacInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let method = request
            .extensions()
            .get::<tonic::GrpcMethod<'_>>()
            .map(|m| m.method().to_string())
            .unwrap_or_default();

        if self.exempt_health && method == "Health" {
            return Ok(request);
        }

        if !self.cache.enabled() {
            return Ok(request);
        }

        let principal = match self.cache.resolve(request.metadata()) {
            Ok(Some(p)) => p,
            _ => return Err(Status::unauthenticated("missing or invalid credentials")),
        };

        if let Some(rp) = method_priv(&method) {
            if rp.global_only {
                self.cache
                    .authorize(&principal, ObjectType::Global, "*", rp.privilege.as_str())
                    .map_err(|_| {
                        Status::permission_denied(format!(
                            "no {} privilege",
                            rp.privilege.as_str()
                        ))
                    })?;
            }
        }

        request.extensions_mut().insert(principal);
        Ok(request)
    }
}

pub fn require_collection<T>(
    cache: &RbacCache,
    request: &Request<T>,
    collection: &str,
    privilege: Privilege,
) -> Result<(), Status> {
    if !cache.enabled() {
        return Ok(());
    }
    let principal = request
        .extensions()
        .get::<Principal>()
        .ok_or_else(|| Status::unauthenticated("principal missing"))?;
    cache
        .authorize(principal, ObjectType::Collection, collection, privilege.as_str())
        .map_err(|_| {
            Status::permission_denied(format!(
                "no {} privilege on collection {collection}",
                privilege.as_str()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{hash_secret, GrantItem, RbacOp};

    fn cache_with(state: RbacState) -> RbacCache {
        let c = RbacCache::new(vec![]);
        c.install(state.snapshot());
        c
    }

    #[test]
    fn legacy_key_superuser() {
        let c = RbacCache::new(vec!["secret".into()]);
        assert!(c.enabled());
        let p = c.resolve_token_or_legacy("secret").unwrap();
        assert!(p.is_superuser);
    }

    #[test]
    fn token_resolves_and_authorizes() {
        let mut s = RbacState::new();
        s.ensure_builtin_roles(1);
        let h = hash_secret("pw").unwrap();
        s.apply(RbacOp::CreateUser {
            name: "alice".into(),
            password_hash: h,
            created_at_ms: 1,
        })
        .unwrap();
        s.apply(RbacOp::GrantRole {
            user: "alice".into(),
            role: crate::BUILTIN_READ_ONLY.into(),
        })
        .unwrap();
        let tok = hash_secret("xyz").unwrap();
        s.apply(RbacOp::CreateToken {
            id: "t1".into(),
            user: "alice".into(),
            secret_hash: tok,
            created_at_ms: 1,
            description: "".into(),
        })
        .unwrap();

        let c = cache_with(s);
        let p = c.resolve_token_or_legacy("t1:xyz").unwrap();
        assert!(c.authorize(&p, ObjectType::Collection, "books", "Search").is_ok());
        assert!(c.authorize(&p, ObjectType::Collection, "books", "Insert").is_err());
    }
}
