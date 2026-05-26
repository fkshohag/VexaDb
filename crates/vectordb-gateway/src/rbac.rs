//! Gateway-side RBAC: cached state, principal resolution, REST handlers.
//!
//! The gateway caches an [`RbacState`] in memory for fast authorization. It
//! refreshes from the cluster at startup, after every mutation it forwards,
//! and periodically in the background. Mutations themselves are sent to the
//! cluster as JSON-encoded [`RbacOp`]s over the existing gRPC surface.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use axum::Json;
use base64::Engine as _;
use parking_lot::RwLock;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;
use vectordb_auth::{HEADER_API_KEY, HEADER_AUTHORIZATION};
use vectordb_client::VectorDbClient;
use vectordb_rbac::{
    hash_secret, AuthzError, GrantItem, ObjectType, Principal, Privilege, RbacOp, RbacSnapshot,
    RbacState, BUILTIN_ADMIN, BUILTIN_READ_ONLY, BUILTIN_READ_WRITE,
};

use crate::AppState;

/// Shared, refreshable RBAC cache.
#[derive(Clone)]
pub struct RbacContext {
    state: Arc<RwLock<RbacState>>,
    /// `Some` when the legacy `VECTORDB_API_KEYS` env is set; these keys grant
    /// superuser access for backward compatibility.
    legacy_keys: Arc<BTreeSet<String>>,
    /// True when RBAC is enabled (any auth mechanism configured).
    pub enabled: bool,
    /// gRPC client used to forward mutations.
    pub client: Arc<Mutex<VectorDbClient>>,
}

impl RbacContext {
    pub fn new(client: Arc<Mutex<VectorDbClient>>, legacy_keys: Vec<String>, enabled: bool) -> Self {
        Self {
            state: Arc::new(RwLock::new(RbacState::new())),
            legacy_keys: Arc::new(legacy_keys.into_iter().collect()),
            enabled,
            client,
        }
    }

    /// Refresh the cache from the cluster.
    pub async fn refresh(&self) -> anyhow::Result<()> {
        let mut client = self.client.lock().await;
        let bytes = client
            .get_rbac_snapshot()
            .await
            .map_err(|e| anyhow::anyhow!("get_rbac_snapshot: {e}"))?;
        if bytes.is_empty() {
            return Ok(());
        }
        let snap: RbacSnapshot = serde_json::from_slice(&bytes)?;
        let mut s = self.state.write();
        s.restore(snap);
        Ok(())
    }

    /// Apply one RBAC mutation through the cluster, then refresh the cache.
    pub async fn apply(&self, op: RbacOp) -> Result<(), StatusCode> {
        let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let mut client = self.client.lock().await;
        client
            .apply_rbac(bytes)
            .await
            .map_err(|s| match s.code() {
                tonic::Code::AlreadyExists => StatusCode::CONFLICT,
                tonic::Code::NotFound => StatusCode::NOT_FOUND,
                tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
                tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
                _ => StatusCode::BAD_GATEWAY,
            })?;
        drop(client);
        let _ = self.refresh().await;
        Ok(())
    }

    pub fn snapshot(&self) -> RbacSnapshot {
        self.state.read().snapshot()
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn random_token(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

/// Seed built-in roles + (optionally) a root user. Idempotent: each step is
/// skipped if the cached snapshot shows the target already exists.
pub async fn bootstrap(ctx: &RbacContext, root_password: Option<&str>) -> anyhow::Result<()> {
    ctx.refresh().await.ok(); // best-effort

    let snap = ctx.snapshot();
    let has_role = |name: &str| snap.roles.iter().any(|r| r.name == name);

    if !has_role(BUILTIN_ADMIN) {
        ctx.apply(RbacOp::CreateRole {
            name: BUILTIN_ADMIN.into(),
            description: "built-in superuser".into(),
            created_at_ms: now_ms(),
        }).await.ok();
        ctx.apply(RbacOp::GrantPrivilege {
            role: BUILTIN_ADMIN.into(),
            grant: GrantItem::new(ObjectType::Global, "*", "*"),
        }).await.ok();
        ctx.apply(RbacOp::GrantPrivilege {
            role: BUILTIN_ADMIN.into(),
            grant: GrantItem::new(ObjectType::Collection, "*", "*"),
        }).await.ok();
    }

    let seed = |role: &str, privs: &[(ObjectType, &str)]| {
        let role = role.to_string();
        let privs = privs.iter().map(|(o, p)| (*o, p.to_string())).collect::<Vec<_>>();
        async move {
            ctx.apply(RbacOp::CreateRole {
                name: role.clone(),
                description: format!("built-in {role}"),
                created_at_ms: now_ms(),
            }).await.ok();
            for (obj, p) in privs {
                ctx.apply(RbacOp::GrantPrivilege {
                    role: role.clone(),
                    grant: GrantItem::new(obj, "*", p),
                }).await.ok();
            }
        }
    };

    if !has_role(BUILTIN_READ_WRITE) {
        seed(
            BUILTIN_READ_WRITE,
            &[
                (ObjectType::Global, "ListCollections"),
                (ObjectType::Global, "ListDatabases"),
                (ObjectType::Global, "DescribeDatabase"),
                (ObjectType::Collection, "DescribeCollection"),
                (ObjectType::Collection, "CollectionStats"),
                (ObjectType::Collection, "Search"),
                (ObjectType::Collection, "Query"),
                (ObjectType::Collection, "Insert"),
                (ObjectType::Collection, "Upsert"),
                (ObjectType::Collection, "Delete"),
                (ObjectType::Collection, "Get"),
                // Management (Milvus parity).
                (ObjectType::Collection, "CreateIndex"),
                (ObjectType::Collection, "DropIndex"),
                (ObjectType::Collection, "DescribeIndex"),
                (ObjectType::Collection, "ListIndexes"),
                (ObjectType::Collection, "AlterIndex"),
                (ObjectType::Collection, "LoadCollection"),
                (ObjectType::Collection, "ReleaseCollection"),
                (ObjectType::Collection, "GetLoadState"),
                (ObjectType::Collection, "Flush"),
                (ObjectType::Collection, "Compact"),
                (ObjectType::Collection, "GetPersistentSegmentInfo"),
                // Partitions (Milvus parity, collection-scoped).
                (ObjectType::Collection, "CreatePartition"),
                (ObjectType::Collection, "DropPartition"),
                (ObjectType::Collection, "DescribePartition"),
                (ObjectType::Collection, "ShowPartitions"),
                (ObjectType::Collection, "GetPartitionStatistics"),
            ],
        ).await;
    }
    if !has_role(BUILTIN_READ_ONLY) {
        seed(
            BUILTIN_READ_ONLY,
            &[
                (ObjectType::Global, "ListCollections"),
                (ObjectType::Global, "ListDatabases"),
                (ObjectType::Global, "DescribeDatabase"),
                (ObjectType::Collection, "DescribeCollection"),
                (ObjectType::Collection, "CollectionStats"),
                (ObjectType::Collection, "Search"),
                (ObjectType::Collection, "Query"),
                (ObjectType::Collection, "Get"),
                (ObjectType::Collection, "DescribeIndex"),
                (ObjectType::Collection, "ListIndexes"),
                (ObjectType::Collection, "GetLoadState"),
                (ObjectType::Collection, "GetPersistentSegmentInfo"),
                // Partitions — read-side only.
                (ObjectType::Collection, "DescribePartition"),
                (ObjectType::Collection, "ShowPartitions"),
                (ObjectType::Collection, "GetPartitionStatistics"),
            ],
        ).await;
    }

    let snap = ctx.snapshot();
    if snap.users.is_empty() {
        if let Some(pw) = root_password {
            let hash = hash_secret(pw).map_err(|e| anyhow::anyhow!("hash root: {e}"))?;
            ctx.apply(RbacOp::CreateUser {
                name: "root".into(),
                password_hash: hash,
                created_at_ms: now_ms(),
            }).await.ok();
            ctx.apply(RbacOp::GrantRole {
                user: "root".into(),
                role: BUILTIN_ADMIN.into(),
            }).await.ok();
            tracing::info!("RBAC: bootstrapped 'root' superuser");
        } else {
            tracing::warn!(
                "RBAC enabled but no users exist; set VECTORDB_ROOT_PASSWORD to create a root user"
            );
        }
    }
    Ok(())
}

/// Try to resolve a [`Principal`] from request headers.
///
/// Recognized:
/// - `Authorization: Basic <base64(user:pass)>`
/// - `Authorization: Bearer <token>` (legacy key or `tokenid:secret`)
/// - `x-api-key: <token>` (legacy key or `tokenid:secret`)
///
/// `Ok(None)` means no credential provided.
pub fn resolve_principal(ctx: &RbacContext, headers: &axum::http::HeaderMap) -> Result<Option<Principal>, AuthzError> {
    let api_key = headers.get(HEADER_API_KEY).and_then(|v| v.to_str().ok());
    let authz = headers.get(HEADER_AUTHORIZATION).and_then(|v| v.to_str().ok());

    if let Some(raw) = api_key.filter(|s| !s.is_empty()) {
        return resolve_token_or_legacy(ctx, raw).map(Some);
    }

    if let Some(v) = authz {
        let v = v.trim();
        if let Some(b64) = v.strip_prefix("Basic ") {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(b64.trim())
                .map_err(|_| AuthzError::Unauthenticated)?;
            let s = std::str::from_utf8(&decoded).map_err(|_| AuthzError::Unauthenticated)?;
            let (user, pw) = s.split_once(':').ok_or(AuthzError::Unauthenticated)?;
            let state = ctx.state.read();
            return state.authenticate_password(user, pw).map(Some);
        }
        if let Some(rest) = v.strip_prefix("Bearer ") {
            return resolve_token_or_legacy(ctx, rest.trim()).map(Some);
        }
    }
    Ok(None)
}

fn resolve_token_or_legacy(ctx: &RbacContext, raw: &str) -> Result<Principal, AuthzError> {
    if ctx.legacy_keys.contains(raw) {
        return Ok(Principal::superuser_token("legacy-api-key"));
    }
    let state = ctx.state.read();
    state.authenticate_token(raw)
}

/// Authorization required for one request.
#[derive(Debug, Clone, Copy)]
pub struct RoutePriv {
    pub object_type: ObjectType,
    pub privilege: Privilege,
    /// Index into the URI path segments holding the object name, if any.
    /// `None` = wildcard.
    pub name_segment: Option<usize>,
}

/// Map (METHOD, PATH) → (privilege, object). `None` = no auth check needed
/// (probes, login).
pub fn route_priv(method: &str, path: &str) -> Option<RoutePriv> {
    use ObjectType::*;
    use Privilege::*;

    // Public probes
    if matches!(path, "/health" | "/live" | "/ready" | "/metrics") {
        return None;
    }
    // Login is intentionally unauthenticated; it produces credentials.
    if path == "/v1/auth/login" && method == "POST" {
        return None;
    }

    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let m = method.to_ascii_uppercase();

    let g = |p: Privilege| Some(RoutePriv { object_type: Global, privilege: p, name_segment: None });
    let c = |p: Privilege, seg: usize| Some(RoutePriv { object_type: Collection, privilege: p, name_segment: Some(seg) });

    match (m.as_str(), segs.as_slice()) {
        ("GET", ["v1", "collections"]) => g(ListCollections),
        ("POST", ["v1", "collections"]) => g(CreateCollection),
        ("GET", ["v1", "collections", _]) => c(DescribeCollection, 2),
        ("DELETE", ["v1", "collections", _]) => c(DropCollection, 2),
        ("GET", ["v1", "collections", _, "stats"]) => c(CollectionStats, 2),
        ("POST", ["v1", "collections", _, "upsert"]) => c(Upsert, 2),
        ("POST", ["v1", "collections", _, "bulk"]) => c(Insert, 2),
        ("POST", ["v1", "collections", _, "search"]) => c(Search, 2),
        ("POST", ["v1", "collections", _, "hybrid-search"]) => c(Search, 2),
        ("POST", ["v1", "collections", _, "query"]) => c(Query, 2),
        ("POST", ["v1", "collections", _, "scroll"]) => c(Query, 2),
        ("POST", ["v1", "admin", "analyze"]) => g(Query),
        ("POST", ["v1", "collections", _, "reindex"]) => c(Reindex, 2),
        ("DELETE", ["v1", "collections", _, "points"]) => c(Delete, 2),
        ("GET", ["v1", "collections", _, "points", _]) => c(Get, 2),
        // ---- Database management (Milvus parity) ------------------------
        ("GET", ["v1", "databases"]) => g(ListDatabases),
        ("POST", ["v1", "databases"]) => g(CreateDatabase),
        ("GET", ["v1", "databases", _]) => g(DescribeDatabase),
        ("DELETE", ["v1", "databases", _]) => g(DropDatabase),
        ("PATCH", ["v1", "databases", _, "properties"]) => g(AlterDatabase),
        ("GET", ["v1", "snapshots"]) => g(Snapshot),
        ("POST", ["v1", "snapshots"]) => g(Snapshot),
        ("DELETE", ["v1", "snapshots", _]) => g(Snapshot),
        ("POST", ["v1", "admin", "rebalance"]) => g(Rebalance),
        ("GET", ["v1", "admin", "rebalance"]) => g(Rebalance),
        ("POST", ["v1", "admin", "compact-wal"]) => g(CompactWal),
        ("GET", ["v1", "admin", "cluster"]) => g(ClusterStatus),
        // ---- Management (Milvus parity) ----------------------------------
        ("GET", ["v1", "collections", _, "indexes"]) => c(ListIndexes, 2),
        ("POST", ["v1", "collections", _, "indexes"]) => c(CreateIndex, 2),
        ("GET", ["v1", "collections", _, "indexes", _]) => c(DescribeIndex, 2),
        ("DELETE", ["v1", "collections", _, "indexes", _]) => c(DropIndex, 2),
        ("PATCH", ["v1", "collections", _, "indexes", _, "properties"]) => c(AlterIndex, 2),
        ("DELETE", ["v1", "collections", _, "indexes", _, "properties"]) => c(AlterIndex, 2),
        ("POST", ["v1", "collections", _, "load"]) => c(LoadCollection, 2),
        ("POST", ["v1", "collections", _, "release"]) => c(ReleaseCollection, 2),
        ("GET", ["v1", "collections", _, "load-state"]) => c(GetLoadState, 2),
        ("POST", ["v1", "collections", _, "refresh-load"]) => c(LoadCollection, 2),
        ("POST", ["v1", "collections", _, "flush"]) => c(Flush, 2),
        ("POST", ["v1", "collections", _, "compact"]) => c(Compact, 2),
        ("GET", ["v1", "compactions", _]) => g(GetCompactionState),
        ("GET", ["v1", "collections", _, "segments"]) => c(GetPersistentSegmentInfo, 2),
        // Partitions (collection-scoped).
        ("GET", ["v1", "collections", _, "partitions"]) => c(ShowPartitions, 2),
        ("POST", ["v1", "collections", _, "partitions"]) => c(CreatePartition, 2),
        ("GET", ["v1", "collections", _, "partitions", _]) => c(DescribePartition, 2),
        ("DELETE", ["v1", "collections", _, "partitions", _]) => c(DropPartition, 2),
        ("GET", ["v1", "collections", _, "partitions", _, "stats"]) => {
            c(GetPartitionStatistics, 2)
        }
        // Resource groups (Milvus parity, global).
        ("GET", ["v1", "resource-groups"]) => g(ListResourceGroups),
        ("POST", ["v1", "resource-groups"]) => g(CreateResourceGroup),
        ("GET", ["v1", "resource-groups", _]) => g(DescribeResourceGroup),
        ("PATCH", ["v1", "resource-groups", _]) => g(UpdateResourceGroup),
        ("DELETE", ["v1", "resource-groups", _]) => g(DropResourceGroup),
        ("GET", ["v1", "collections", _, "replicas"]) => g(DescribeReplica),
        ("POST", ["v1", "admin", "transfer-replica"]) => g(TransferReplica),
        // RBAC management (everything under /v1/auth, /v1/users, /v1/roles,
        // /v1/privilege-groups, /v1/admin/rbac).
        ("POST", ["v1", "auth", "tokens"])
        | ("DELETE", ["v1", "auth", "tokens", _])
        | ("POST", ["v1", "users"])
        | ("GET", ["v1", "users"])
        | ("GET", ["v1", "users", _])
        | ("DELETE", ["v1", "users", _])
        | ("PATCH", ["v1", "users", _, "password"])
        | ("POST", ["v1", "users", _, "roles", _])
        | ("DELETE", ["v1", "users", _, "roles", _])
        | ("POST", ["v1", "roles"])
        | ("GET", ["v1", "roles"])
        | ("GET", ["v1", "roles", _])
        | ("DELETE", ["v1", "roles", _])
        | ("POST", ["v1", "roles", _, "grants"])
        | ("DELETE", ["v1", "roles", _, "grants"])
        | ("POST", ["v1", "privilege-groups"])
        | ("GET", ["v1", "privilege-groups"])
        | ("DELETE", ["v1", "privilege-groups", _])
        | ("PATCH", ["v1", "privilege-groups", _])
        | ("POST", ["v1", "admin", "rbac", "backup"])
        | ("POST", ["v1", "admin", "rbac", "restore"]) => g(ManageRbac),
        _ => g(ListCollections), // unknown route, fail-closed at a low privilege
    }
}

/// Middleware: resolve principal, enforce privilege, attach principal.
pub async fn rbac_middleware(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = request.uri().path().to_string();
    let method = request.method().clone();

    if !state.rbac.enabled {
        return Ok(next.run(request).await);
    }

    let route = route_priv(method.as_str(), &path);
    let Some(rp) = route else {
        return Ok(next.run(request).await);
    };

    let principal = match resolve_principal(&state.rbac, &headers) {
        Ok(Some(p)) => p,
        _ => return Err(StatusCode::UNAUTHORIZED),
    };

    let object_name = match rp.name_segment {
        None => "*".to_string(),
        Some(idx) => path
            .split('/')
            .filter(|s| !s.is_empty())
            .nth(idx)
            .unwrap_or("*")
            .to_string(),
    };

    let allowed = state
        .rbac
        .state
        .read()
        .authorize(&principal, rp.object_type, &object_name, rp.privilege.as_str())
        .is_ok();
    if !allowed {
        return Err(StatusCode::FORBIDDEN);
    }

    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

// ---------- REST handlers ----------

#[derive(Deserialize)]
pub struct LoginBody {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token_id: String,
    pub token: String,
    pub user: String,
}

/// `POST /v1/auth/login` — verify password and mint a new API token.
pub async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginBody>,
) -> Result<Json<LoginResponse>, StatusCode> {
    if !state.rbac.enabled {
        return Err(StatusCode::NOT_FOUND);
    }
    {
        let s = state.rbac.state.read();
        s.authenticate_password(&body.username, &body.password)
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
    }
    issue_token(&state.rbac, &body.username, "login").await
}

#[derive(Deserialize)]
pub struct TokenBody {
    #[serde(default)]
    pub description: String,
}

/// `POST /v1/auth/tokens` — mint a new API token for the authenticated user.
pub async fn create_token(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<LoginResponse>, StatusCode> {
    let principal = request.extensions().get::<Principal>().cloned().ok_or(StatusCode::UNAUTHORIZED)?;
    let body = read_json::<TokenBody>(request).await.unwrap_or_default();
    issue_token(&state.rbac, &principal.user, &body.description).await
}

async fn issue_token(ctx: &RbacContext, user: &str, description: &str) -> Result<Json<LoginResponse>, StatusCode> {
    let id = Uuid::new_v4().simple().to_string()[..16].to_string();
    let secret = random_token(40);
    let hash = hash_secret(&secret).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    ctx.apply(RbacOp::CreateToken {
        id: id.clone(),
        user: user.to_string(),
        secret_hash: hash,
        created_at_ms: now_ms(),
        description: description.to_string(),
    })
    .await?;
    Ok(Json(LoginResponse {
        token_id: id.clone(),
        token: format!("{id}:{secret}"),
        user: user.to_string(),
    }))
}

/// `DELETE /v1/auth/tokens/:id`
pub async fn revoke_token(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::RevokeToken { id }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct CreateUserBody {
    pub name: String,
    pub password: String,
    #[serde(default)]
    pub roles: Vec<String>,
}

/// `POST /v1/users`
pub async fn create_user(
    State(state): State<AppState>,
    Json(body): Json<CreateUserBody>,
) -> Result<StatusCode, StatusCode> {
    let hash = hash_secret(&body.password).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.rbac.apply(RbacOp::CreateUser {
        name: body.name.clone(),
        password_hash: hash,
        created_at_ms: now_ms(),
    }).await?;
    for r in body.roles {
        state.rbac.apply(RbacOp::GrantRole {
            user: body.name.clone(),
            role: r,
        }).await?;
    }
    Ok(StatusCode::CREATED)
}

/// `GET /v1/users`
pub async fn list_users(State(state): State<AppState>) -> Json<Vec<String>> {
    Json(state.rbac.state.read().list_users())
}

#[derive(Serialize)]
pub struct UserView {
    pub name: String,
    pub disabled: bool,
    pub created_at_ms: u64,
    pub roles: Vec<String>,
}

/// `GET /v1/users/:name`
pub async fn describe_user(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<UserView>, StatusCode> {
    let s = state.rbac.state.read();
    let u = s.get_user(&name).ok_or(StatusCode::NOT_FOUND)?.clone();
    let roles = s.user_roles(&name).into_iter().collect();
    Ok(Json(UserView {
        name: u.name,
        disabled: u.disabled,
        created_at_ms: u.created_at_ms,
        roles,
    }))
}

/// `DELETE /v1/users/:name`
pub async fn drop_user(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::DropUser { name }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct UpdatePasswordBody {
    pub old: String,
    pub new: String,
}

/// `PATCH /v1/users/:name/password`
pub async fn update_password(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<UpdatePasswordBody>,
) -> Result<StatusCode, StatusCode> {
    {
        let s = state.rbac.state.read();
        s.authenticate_password(&name, &body.old).map_err(|_| StatusCode::UNAUTHORIZED)?;
    }
    let hash = hash_secret(&body.new).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.rbac.apply(RbacOp::UpdatePassword { name, password_hash: hash }).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/users/:name/roles/:role`
pub async fn grant_role(
    State(state): State<AppState>,
    Path((name, role)): Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::GrantRole { user: name, role }).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /v1/users/:name/roles/:role`
pub async fn revoke_role(
    State(state): State<AppState>,
    Path((name, role)): Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::RevokeRole { user: name, role }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct CreateRoleBody {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

/// `POST /v1/roles`
pub async fn create_role(
    State(state): State<AppState>,
    Json(body): Json<CreateRoleBody>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::CreateRole {
        name: body.name,
        description: body.description,
        created_at_ms: now_ms(),
    }).await?;
    Ok(StatusCode::CREATED)
}

/// `GET /v1/roles`
pub async fn list_roles(State(state): State<AppState>) -> Json<Vec<String>> {
    Json(state.rbac.state.read().list_roles())
}

#[derive(Serialize)]
pub struct RoleView {
    pub name: String,
    pub description: String,
    pub grants: Vec<GrantItem>,
}

/// `GET /v1/roles/:name`
pub async fn describe_role(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<RoleView>, StatusCode> {
    let s = state.rbac.state.read();
    let r = s.role(&name).ok_or(StatusCode::NOT_FOUND)?.clone();
    let grants = s.role_grants(&name);
    Ok(Json(RoleView { name: r.name, description: r.description, grants }))
}

/// `DELETE /v1/roles/:name`
pub async fn drop_role(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::DropRole { name }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct GrantBody {
    pub object_type: String,
    pub object_name: String,
    pub privilege: String,
}

fn parse_object_type(s: &str) -> Result<ObjectType, StatusCode> {
    ObjectType::parse(s).ok_or(StatusCode::BAD_REQUEST)
}

/// `POST /v1/roles/:role/grants`
pub async fn grant_privilege(
    State(state): State<AppState>,
    Path(role): Path<String>,
    Json(body): Json<GrantBody>,
) -> Result<StatusCode, StatusCode> {
    let object_type = parse_object_type(&body.object_type)?;
    state.rbac.apply(RbacOp::GrantPrivilege {
        role,
        grant: GrantItem::new(object_type, body.object_name, body.privilege),
    }).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /v1/roles/:role/grants`
pub async fn revoke_privilege(
    State(state): State<AppState>,
    Path(role): Path<String>,
    Json(body): Json<GrantBody>,
) -> Result<StatusCode, StatusCode> {
    let object_type = parse_object_type(&body.object_type)?;
    state.rbac.apply(RbacOp::RevokePrivilege {
        role,
        grant: GrantItem::new(object_type, body.object_name, body.privilege),
    }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct PrivilegeGroupBody {
    pub name: String,
    #[serde(default)]
    pub privileges: Vec<String>,
}

/// `POST /v1/privilege-groups`
pub async fn create_privilege_group(
    State(state): State<AppState>,
    Json(body): Json<PrivilegeGroupBody>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::CreatePrivilegeGroup {
        name: body.name,
        privileges: body.privileges,
    }).await?;
    Ok(StatusCode::CREATED)
}

/// `GET /v1/privilege-groups`
pub async fn list_privilege_groups(State(state): State<AppState>) -> Json<Vec<vectordb_rbac::PrivilegeGroup>> {
    let s = state.rbac.state.read();
    Json(s.list_privilege_groups().into_iter().cloned().collect())
}

/// `DELETE /v1/privilege-groups/:name`
pub async fn drop_privilege_group(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, StatusCode> {
    state.rbac.apply(RbacOp::DropPrivilegeGroup { name }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct GroupPatchBody {
    #[serde(default)]
    pub add: Vec<String>,
    #[serde(default)]
    pub remove: Vec<String>,
}

/// `PATCH /v1/privilege-groups/:name`
pub async fn patch_privilege_group(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<GroupPatchBody>,
) -> Result<StatusCode, StatusCode> {
    if !body.add.is_empty() {
        state.rbac.apply(RbacOp::AddPrivilegesToGroup {
            name: name.clone(),
            privileges: body.add,
        }).await?;
    }
    if !body.remove.is_empty() {
        state.rbac.apply(RbacOp::RemovePrivilegesFromGroup {
            name,
            privileges: body.remove,
        }).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/admin/rbac/backup` — returns the full snapshot.
pub async fn backup_rbac(State(state): State<AppState>) -> Json<RbacSnapshot> {
    Json(state.rbac.snapshot())
}

/// `POST /v1/admin/rbac/restore` — applies a snapshot as a sequence of ops.
///
/// Idempotent re-create: existing users/roles/grants are skipped (best-effort).
pub async fn restore_rbac(
    State(state): State<AppState>,
    Json(snap): Json<RbacSnapshot>,
) -> Result<StatusCode, StatusCode> {
    for u in snap.users {
        let _ = state.rbac.apply(RbacOp::CreateUser {
            name: u.name,
            password_hash: u.password_hash,
            created_at_ms: u.created_at_ms,
        }).await;
    }
    for r in snap.roles {
        let _ = state.rbac.apply(RbacOp::CreateRole {
            name: r.name,
            description: r.description,
            created_at_ms: r.created_at_ms,
        }).await;
    }
    for (u, r) in snap.user_roles {
        let _ = state.rbac.apply(RbacOp::GrantRole { user: u, role: r }).await;
    }
    for rg in snap.role_grants {
        for g in rg.grants {
            let _ = state.rbac.apply(RbacOp::GrantPrivilege {
                role: rg.role.clone(),
                grant: g,
            }).await;
        }
    }
    for g in snap.privilege_groups {
        let _ = state.rbac.apply(RbacOp::CreatePrivilegeGroup {
            name: g.name,
            privileges: g.privileges.into_iter().collect(),
        }).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Spawn a background task that refreshes the cache every `period`.
pub fn start_refresh_loop(ctx: RbacContext, period: Duration) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        // Skip the first immediate tick — bootstrap already refreshed.
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(e) = ctx.refresh().await {
                tracing::debug!(error = %e, "RBAC refresh failed");
            }
        }
    });
}

async fn read_json<T: serde::de::DeserializeOwned + Default>(request: Request) -> Option<T> {
    use axum::body::to_bytes;
    let body = request.into_body();
    let bytes = to_bytes(body, 1024 * 1024).await.ok()?;
    if bytes.is_empty() {
        return Some(T::default());
    }
    serde_json::from_slice(&bytes).ok()
}

impl Default for TokenBody {
    fn default() -> Self {
        Self { description: String::new() }
    }
}
