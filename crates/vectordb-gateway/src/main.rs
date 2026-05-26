mod rbac;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    middleware::from_fn_with_state,
    routing::{delete, get, patch, post},
    Json, Router,
};
use clap::Parser;
use lazy_static::lazy_static;
use prometheus::{Encoder, IntCounter, TextEncoder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;
use vectordb_client::{cosine_collection, VectorDbClient};
use vectordb_proto::vectordb::v1::{
    DistanceMetric, PayloadFieldIndex, PayloadIndexKind, VectorPoint,
};

pub use rbac::RbacContext;

#[derive(Parser, Debug)]
#[command(name = "vectordb-gateway", about = "HTTP/JSON gateway for VectorDB")]
struct Cli {
    #[arg(long, env = "VECTORDB_GRPC", default_value = "http://127.0.0.1:6334")]
    grpc: String,

    #[arg(long, env = "VECTORDB_HTTP", default_value = "0.0.0.0:8080")]
    listen: String,

    /// Comma-separated legacy API keys. Each key is treated as a superuser
    /// credential (backward compat with the pre-RBAC gateway).
    #[arg(long, env = "VECTORDB_API_KEYS")]
    api_keys: Option<String>,

    /// Bootstrap a `root` superuser with this password if no users exist.
    /// RBAC stays enabled across restarts once any user is present.
    #[arg(long, env = "VECTORDB_ROOT_PASSWORD")]
    root_password: Option<String>,

    /// How often to refresh the RBAC snapshot from the cluster.
    #[arg(long, env = "VECTORDB_RBAC_REFRESH_SECS", default_value = "15")]
    rbac_refresh_secs: u64,

    #[arg(long, env = "VECTORDB_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    #[arg(long, env = "VECTORDB_TLS_KEY")]
    tls_key: Option<PathBuf>,
}

lazy_static! {
    static ref HTTP_REQUESTS: IntCounter =
        prometheus::register_int_counter!("vectordb_http_requests_total", "HTTP requests").unwrap();
}

#[derive(Clone)]
pub struct AppState {
    pub client: Arc<Mutex<VectorDbClient>>,
    pub rbac: RbacContext,
}

#[derive(Serialize)]
struct HealthBody {
    status: String,
}

#[derive(Deserialize)]
struct CreateCollectionBody {
    name: String,
    dimension: u32,
    #[serde(default)]
    metric: Option<String>,
    #[serde(default)]
    payload_indexes: Vec<PayloadFieldIndexBody>,
    #[serde(default)]
    sparse_enabled: Option<bool>,
    #[serde(default)]
    bm25_text_field: Option<String>,
    #[serde(default)]
    scalar_quantization: Option<bool>,
    /// Opaque user-defined properties (Milvus parity: TTL, mmap.enabled, …).
    #[serde(default)]
    properties: std::collections::HashMap<String, String>,
}

#[derive(Deserialize)]
struct PayloadFieldIndexBody {
    field: String,
    /// "keyword" | "numeric" | "bool"
    kind: String,
}

#[derive(Deserialize)]
struct UpsertBody {
    points: Vec<PointBody>,
}

#[derive(Deserialize)]
struct PointBody {
    id: String,
    values: Vec<f32>,
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    sparse: Option<SparseQueryBody>,
}

#[derive(Deserialize)]
struct SearchBody {
    vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    top_k: u32,
    /// Filter expression (Filter DSL). Forwarded as JSON.
    #[serde(default)]
    filter: Value,
    #[serde(default)]
    sparse_query: Option<SparseQueryBody>,
    #[serde(default)]
    text_query: Option<String>,
    /// dense | sparse | bm25 | hybrid_rrf | hybrid_weighted
    #[serde(default)]
    search_mode: Option<String>,
    #[serde(default)]
    hybrid_alpha: Option<f32>,
    /// Top-level payload keys to return. Empty + with_payload=true → all keys.
    #[serde(default)]
    output_fields: Vec<String>,
    #[serde(default)]
    with_payload: bool,
    #[serde(default)]
    with_vector: bool,
}

/// Filter-only retrieval body (Milvus-style `Query`).
#[derive(Deserialize)]
struct QueryBody {
    /// JSON object **or** string expression (`category == 'books'`).
    #[serde(default)]
    filter: Value,
    #[serde(default)]
    ids: Vec<String>,
    #[serde(default = "default_query_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
    #[serde(default)]
    output_fields: Vec<String>,
    #[serde(default = "default_with_payload")]
    with_payload: bool,
    #[serde(default)]
    with_vector: bool,
}

fn default_query_limit() -> u32 {
    100
}

fn default_with_payload() -> bool {
    true
}

#[derive(Deserialize, Default)]
struct SparseQueryBody {
    #[serde(default)]
    indices: Vec<u32>,
    #[serde(default)]
    values: Vec<f32>,
}

fn default_top_k() -> u32 {
    10
}

#[derive(Serialize)]
struct SearchHit {
    id: String,
    score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vector: Option<Vec<f32>>,
}

#[derive(Deserialize)]
struct DeletePointsBody {
    ids: Vec<String>,
}

#[derive(Deserialize)]
struct BulkUpsertBody {
    points: Vec<PointBody>,
    #[serde(default)]
    chunk_size: Option<u32>,
}

#[derive(Deserialize)]
struct CompactWalBody {
    #[serde(default)]
    snapshot_first: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("vectordb=info".parse()?))
        .init();

    let cli = Cli::parse();

    let legacy_keys: Vec<String> = cli
        .api_keys
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let grpc_api_key = legacy_keys.first().cloned();
    let grpc_client = Arc::new(Mutex::new(
        VectorDbClient::connect_with(&cli.grpc, grpc_api_key).await?,
    ));

    let rbac_enabled = !legacy_keys.is_empty() || cli.root_password.is_some();
    let mut rbac_ctx = RbacContext::new(grpc_client.clone(), legacy_keys.clone(), rbac_enabled);

    if let Err(e) = rbac::bootstrap(&rbac_ctx, cli.root_password.as_deref()).await {
        tracing::warn!(error = %e, "RBAC bootstrap failed (will retry on next mutation)");
    }
    // Re-evaluate: if the cluster already has users, auto-enable RBAC even if
    // no env var was set (operator restored from a snapshot, for example).
    if !rbac_ctx.enabled && !rbac_ctx.snapshot().users.is_empty() {
        rbac_ctx.enabled = true;
    }
    let final_enabled = rbac_ctx.enabled;
    rbac::start_refresh_loop(rbac_ctx.clone(), Duration::from_secs(cli.rbac_refresh_secs.max(1)));

    let state = AppState {
        client: grpc_client,
        rbac: rbac_ctx,
    };

    // axum's default body limit is 2MB which is too small for real bulk
    // ingest at modern dimensions (768-dim vectors are ~6KB each in JSON,
    // so even a few hundred points exceed 2MB). Lift the cap on write paths
    // so /bulk and /upsert can accept fat batches; reads stay on the default.
    const WRITE_BODY_LIMIT: usize = 64 * 1024 * 1024; // 64 MB

    let app = Router::new()
        .route("/health", get(health))
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(http_metrics))
        .route("/v1/version", get(version))
        .route("/v1/collections", get(list_collections).post(create_collection))
        .route(
            "/v1/collections/:name",
            get(describe_collection).delete(delete_collection),
        )
        .route(
            "/v1/collections/:name/upsert",
            post(upsert).layer(DefaultBodyLimit::max(WRITE_BODY_LIMIT)),
        )
        .route(
            "/v1/collections/:name/bulk",
            post(bulk_upsert).layer(DefaultBodyLimit::max(WRITE_BODY_LIMIT)),
        )
        .route("/v1/collections/:name/reindex", post(reindex_collection))
        .route("/v1/collections/:name/rename", post(rename_collection))
        .route("/v1/collections/:name/properties", patch(alter_properties))
        .route("/v1/collections/:name/aliases", get(list_aliases_for))
        .route("/v1/collections/:name/search", post(search))
        .route("/v1/collections/:name/query", post(query_points))
        .route("/v1/collections/:name/stats", get(collection_stats))
        .route("/v1/aliases", get(list_aliases).post(create_alias))
        .route(
            "/v1/aliases/:alias",
            get(describe_alias).put(alter_alias).delete(drop_alias),
        )
        // ---- Database management (Milvus parity) -----------------------
        .route("/v1/databases", get(list_databases).post(create_database))
        .route(
            "/v1/databases/:name",
            get(describe_database).delete(drop_database),
        )
        .route(
            "/v1/databases/:name/properties",
            patch(alter_database_properties).delete(drop_database_properties),
        )
        // ---- Management (Milvus parity) ---------------------------------
        .route(
            "/v1/collections/:name/indexes",
            get(list_indexes).post(create_index),
        )
        .route(
            "/v1/collections/:name/indexes/:field",
            get(describe_index).delete(drop_index),
        )
        .route(
            "/v1/collections/:name/indexes/:field/properties",
            patch(alter_index_properties).delete(drop_index_properties),
        )
        .route("/v1/collections/:name/load", post(load_collection))
        .route("/v1/collections/:name/release", post(release_collection))
        .route("/v1/collections/:name/load-state", get(get_load_state))
        .route("/v1/collections/:name/refresh-load", post(refresh_load))
        .route("/v1/collections/:name/flush", post(flush_collection_route))
        .route("/v1/collections/:name/compact", post(compact_collection_route))
        .route("/v1/compactions/:id", get(get_compaction_state_route))
        .route("/v1/collections/:name/segments", get(list_segments_route))
        // ---- Partitions (Milvus parity) -----------------------------------
        .route(
            "/v1/collections/:name/partitions",
            get(list_partitions_route).post(create_partition_route),
        )
        .route(
            "/v1/collections/:name/partitions/:partition",
            get(has_partition_route).delete(drop_partition_route),
        )
        .route(
            "/v1/collections/:name/partitions/:partition/stats",
            get(get_partition_stats_route),
        )
        .route("/v1/admin/compact-wal", post(compact_wal))
        .route("/v1/admin/rebalance", post(trigger_rebalance).get(rebalance_status))
        .route("/v1/admin/cluster", get(cluster_status))
        .route("/v1/collections/:name/points", delete(delete_points))
        .route("/v1/collections/:name/points/:id", get(get_point))
        .route("/v1/snapshots", get(list_snapshots).post(create_snapshot))
        .route("/v1/snapshots/:id", delete(delete_snapshot))
        // ---- Auth & RBAC management ----
        .route("/v1/auth/login", post(rbac::login))
        .route("/v1/auth/tokens", post(rbac::create_token))
        .route("/v1/auth/tokens/:id", delete(rbac::revoke_token))
        .route("/v1/users", post(rbac::create_user).get(rbac::list_users))
        .route("/v1/users/:name", get(rbac::describe_user).delete(rbac::drop_user))
        .route("/v1/users/:name/password", patch(rbac::update_password))
        .route("/v1/users/:name/roles/:role", post(rbac::grant_role).delete(rbac::revoke_role))
        .route("/v1/roles", post(rbac::create_role).get(rbac::list_roles))
        .route("/v1/roles/:name", get(rbac::describe_role).delete(rbac::drop_role))
        .route("/v1/roles/:role/grants", post(rbac::grant_privilege).delete(rbac::revoke_privilege))
        .route("/v1/privilege-groups", post(rbac::create_privilege_group).get(rbac::list_privilege_groups))
        .route("/v1/privilege-groups/:name", delete(rbac::drop_privilege_group).patch(rbac::patch_privilege_group))
        .route("/v1/admin/rbac/backup", post(rbac::backup_rbac))
        .route("/v1/admin/rbac/restore", post(rbac::restore_rbac))
        .layer(from_fn_with_state(state.clone(), rbac::rbac_middleware))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: SocketAddr = cli.listen.parse()?;
    tracing::info!(
        http = %cli.listen,
        grpc = %cli.grpc,
        rbac = final_enabled,
        legacy_keys = !legacy_keys.is_empty(),
        tls = cli.tls_cert.is_some(),
        "vectordb-gateway listening"
    );

    match (&cli.tls_cert, &cli.tls_key) {
        (Some(cert), Some(key)) => {
            let cfg = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
            axum_server::bind_rustls(addr, cfg).serve(app.into_make_service()).await?;
        }
        (None, None) => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
        _ => anyhow::bail!("both VECTORDB_TLS_CERT and VECTORDB_TLS_KEY are required for TLS"),
    }
    Ok(())
}


/// Header used by clients to scope requests to a logical database (Milvus
/// parity). The gateway translates `(x-vexa-db: <db>, /v1/collections/foo)`
/// into the fully-qualified `<db>/foo` collection identifier before
/// forwarding to gRPC. Missing or empty header → built-in `default` database.
const HEADER_DB: &str = "x-vexa-db";

/// Extract the active database from request headers, falling back to the
/// built-in `default` database.
fn current_db(headers: &HeaderMap) -> String {
    headers
        .get(HEADER_DB)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(vectordb_core::DEFAULT_DATABASE)
        .to_string()
}

/// Build a fully-qualified collection / alias identifier from `(db, name)`.
fn fq(db: &str, name: &str) -> String {
    format!("{db}/{name}")
}

async fn live() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(state): State<AppState>) -> Result<StatusCode, StatusCode> {
    let mut client = state.client.lock().await;
    let h = client.health_detail().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if h.ready {
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

async fn http_metrics() -> Result<String, StatusCode> {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(String::from_utf8(buffer).unwrap_or_default())
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthBody>, StatusCode> {
    let mut client = state.client.lock().await;
    let status = client.health().await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(HealthBody { status }))
}

#[derive(Serialize)]
struct VersionBody {
    /// Semantic version of the gateway crate (matches the cluster).
    version: &'static str,
    /// Free-form server identifier — useful when multiple VexaDb clusters
    /// share a client codebase. Set via `VECTORDB_SERVER_NAME` env.
    server: String,
    /// Optional git commit, populated at build time when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    git_commit: Option<&'static str>,
}

/// `GET /v1/version` — used by SDKs for connection probes (Milvus parity).
async fn version() -> Json<VersionBody> {
    Json(VersionBody {
        version: env!("CARGO_PKG_VERSION"),
        server: std::env::var("VECTORDB_SERVER_NAME").unwrap_or_else(|_| "vexadb".to_string()),
        git_commit: option_env!("VERGEN_GIT_SHA"),
    })
}

async fn list_collections(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<String>>, StatusCode> {
    let db = current_db(&headers);
    let mut client = state.client.lock().await;
    let names = client
        .list_collections()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let prefix = format!("{db}/");
    // Only return collections that belong to the active database, stripping
    // the `<db>/` prefix so callers see simple names.
    let filtered: Vec<String> = names
        .into_iter()
        .filter_map(|fqn| fqn.strip_prefix(&prefix).map(str::to_string))
        .collect();
    Ok(Json(filtered))
}

async fn create_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateCollectionBody>,
) -> Result<StatusCode, StatusCode> {
    let metric = match body.metric.as_deref().unwrap_or("cosine") {
        "cosine" => DistanceMetric::Cosine,
        "euclidean" => DistanceMetric::Euclidean,
        "dot" | "dot_product" => DistanceMetric::DotProduct,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let payload_indexes = body
        .payload_indexes
        .into_iter()
        .map(|p| {
            let kind = match p.kind.as_str() {
                "keyword" => PayloadIndexKind::Keyword,
                "numeric" => PayloadIndexKind::Numeric,
                "bool" => PayloadIndexKind::Bool,
                _ => PayloadIndexKind::Unspecified,
            };
            PayloadFieldIndex {
                field: p.field,
                kind: kind as i32,
            }
        })
        .collect::<Vec<_>>();
    let mut spec = cosine_collection(&body.name, body.dimension);
    spec.database = current_db(&headers);
    spec.metric = metric as i32;
    spec.payload_indexes = payload_indexes;
    spec.sparse_enabled = body.sparse_enabled.unwrap_or(false);
    spec.bm25_text_field = body.bm25_text_field.unwrap_or_default();
    spec.scalar_quantization = body.scalar_quantization.unwrap_or(false);
    spec.properties = body.properties;

    let mut client = state.client.lock().await;
    client
        .create_collection(spec)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(StatusCode::CREATED)
}

async fn describe_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let (spec, count, aliases) = client
        .describe_collection_full(&fqn)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    // Strip the `db/` prefix from aliases for response clarity — clients
    // working within the active database see just simple names.
    let prefix = format!("{db}/");
    let aliases: Vec<String> = aliases
        .into_iter()
        .map(|a| {
            a.strip_prefix(&prefix)
                .map(str::to_string)
                .unwrap_or(a)
        })
        .collect();
    Ok(Json(serde_json::json!({
        "name": spec.name,
        "database": if spec.database.is_empty() { db.clone() } else { spec.database },
        "dimension": spec.dimension,
        "metric": metric_to_string(spec.metric),
        "m": spec.m,
        "ef_construction": spec.ef_construction,
        "ef_search": spec.ef_search,
        "payload_indexes": spec.payload_indexes.iter().map(|p| serde_json::json!({
            "field": p.field,
            "kind": index_kind_to_string(p.kind),
        })).collect::<Vec<_>>(),
        "sparse_enabled": spec.sparse_enabled,
        "bm25_text_field": spec.bm25_text_field,
        "scalar_quantization": spec.scalar_quantization,
        "properties": spec.properties,
        "aliases": aliases,
        "vector_count": count,
    })))
}

fn metric_to_string(m: i32) -> &'static str {
    match DistanceMetric::try_from(m).unwrap_or(DistanceMetric::Unspecified) {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Euclidean => "euclidean",
        DistanceMetric::DotProduct => "dot_product",
        DistanceMetric::Unspecified => "unspecified",
    }
}

fn index_kind_to_string(k: i32) -> &'static str {
    match PayloadIndexKind::try_from(k).unwrap_or(PayloadIndexKind::Unspecified) {
        PayloadIndexKind::Keyword => "keyword",
        PayloadIndexKind::Numeric => "numeric",
        PayloadIndexKind::Bool => "bool",
        PayloadIndexKind::Unspecified => "unspecified",
    }
}

async fn delete_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let mut client = state.client.lock().await;
    client
        .delete_collection(&fqn)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Collection meta: rename, aliases, properties (Milvus parity) -------

#[derive(Deserialize)]
struct RenameCollectionBody {
    new_name: String,
}

async fn rename_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<RenameCollectionBody>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    let op = serde_json::json!({
        "RenameCollection": { "old": name, "new": body.new_name, "database": db }
    });
    forward_meta_op(&state, op).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct AlterPropertiesBody {
    /// Keys to set / overwrite.
    #[serde(default)]
    set: std::collections::BTreeMap<String, String>,
    /// Keys to remove.
    #[serde(default)]
    unset: Vec<String>,
}

async fn alter_properties(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<AlterPropertiesBody>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    let op = serde_json::json!({
        "AlterCollectionProperties": {
            "name": name,
            "database": db,
            "set": body.set,
            "unset": body.unset,
        }
    });
    forward_meta_op(&state, op).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct CreateAliasBody {
    alias: String,
    collection: String,
}

async fn create_alias(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateAliasBody>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    let op = serde_json::json!({
        "CreateAlias": {
            "alias": body.alias,
            "collection": body.collection,
            "database": db,
        }
    });
    forward_meta_op(&state, op).await?;
    Ok(StatusCode::CREATED)
}

#[derive(Deserialize)]
struct AlterAliasBody {
    collection: String,
}

async fn alter_alias(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(alias): Path<String>,
    Json(body): Json<AlterAliasBody>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    let op = serde_json::json!({
        "AlterAlias": {
            "alias": alias,
            "collection": body.collection,
            "database": db,
        }
    });
    forward_meta_op(&state, op).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn drop_alias(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(alias): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    let op = serde_json::json!({
        "DropAlias": { "alias": alias, "database": db }
    });
    forward_meta_op(&state, op).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct AliasRow {
    alias: String,
    collection: String,
}

async fn list_aliases(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AliasRow>>, StatusCode> {
    let db = current_db(&headers);
    let mut client = state.client.lock().await;
    let rows = client
        .list_aliases("")
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let prefix = format!("{db}/");
    Ok(Json(
        rows.into_iter()
            .filter_map(|(fq_alias, fq_collection)| {
                let alias = fq_alias.strip_prefix(&prefix)?.to_string();
                let collection = fq_collection
                    .strip_prefix(&prefix)
                    .map(str::to_string)
                    .unwrap_or(fq_collection);
                Some(AliasRow { alias, collection })
            })
            .collect(),
    ))
}

async fn list_aliases_for(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let rows = client
        .list_aliases(&fqn)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let prefix = format!("{db}/");
    Ok(Json(
        rows.into_iter()
            .filter_map(|(a, _)| a.strip_prefix(&prefix).map(str::to_string))
            .collect(),
    ))
}

#[derive(Serialize)]
struct AliasDetail {
    alias: String,
    collection: String,
}

async fn describe_alias(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(alias): Path<String>,
) -> Result<Json<AliasDetail>, StatusCode> {
    let db = current_db(&headers);
    let fq_alias = fq(&db, &alias);
    let mut client = state.client.lock().await;
    let fq_collection = client
        .describe_alias(&fq_alias)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let prefix = format!("{db}/");
    let collection = fq_collection
        .strip_prefix(&prefix)
        .map(str::to_string)
        .unwrap_or(fq_collection);
    Ok(Json(AliasDetail { alias, collection }))
}

// ---- Database management (Milvus parity) -------------------------------

#[derive(Deserialize)]
struct CreateDatabaseBody {
    name: String,
    #[serde(default)]
    properties: std::collections::BTreeMap<String, String>,
}

async fn create_database(
    State(state): State<AppState>,
    Json(body): Json<CreateDatabaseBody>,
) -> Result<StatusCode, StatusCode> {
    if body.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let op = serde_json::json!({
        "CreateDatabase": {
            "name": body.name,
            "properties": body.properties,
            "created_at_ms": now_ms(),
        }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut client = state.client.lock().await;
    client
        .create_database(bytes)
        .await
        .map_err(map_db_grpc_err)?;
    Ok(StatusCode::CREATED)
}

#[derive(Deserialize)]
struct DropDatabaseQuery {
    #[serde(default)]
    force: bool,
}

async fn drop_database(
    State(state): State<AppState>,
    Path(name): Path<String>,
    axum::extract::Query(q): axum::extract::Query<DropDatabaseQuery>,
) -> Result<StatusCode, StatusCode> {
    let op = serde_json::json!({
        "DropDatabase": { "name": name, "force": q.force }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut client = state.client.lock().await;
    client
        .drop_database(bytes)
        .await
        .map_err(map_db_grpc_err)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct AlterDatabaseBody {
    #[serde(default)]
    set: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    unset: Vec<String>,
}

async fn alter_database_properties(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<AlterDatabaseBody>,
) -> Result<StatusCode, StatusCode> {
    let op = serde_json::json!({
        "AlterDatabaseProperties": {
            "name": name,
            "set": body.set,
            "unset": body.unset,
        }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut client = state.client.lock().await;
    client
        .alter_database(bytes)
        .await
        .map_err(map_db_grpc_err)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct DropDatabasePropsBody {
    keys: Vec<String>,
}

/// Convenience endpoint mirroring Milvus's `DropDatabaseProperties`. Forwards
/// as a single `AlterDatabaseProperties` with an `unset` list.
async fn drop_database_properties(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<DropDatabasePropsBody>,
) -> Result<StatusCode, StatusCode> {
    let op = serde_json::json!({
        "AlterDatabaseProperties": {
            "name": name,
            "set": {},
            "unset": body.keys,
        }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut client = state.client.lock().await;
    client
        .alter_database(bytes)
        .await
        .map_err(map_db_grpc_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_databases(State(state): State<AppState>) -> Result<Json<Vec<String>>, StatusCode> {
    let mut client = state.client.lock().await;
    client
        .list_databases()
        .await
        .map(Json)
        .map_err(|_| StatusCode::BAD_GATEWAY)
}

#[derive(Serialize)]
struct DatabaseDetail {
    name: String,
    properties: std::collections::HashMap<String, String>,
    created_at_ms: u64,
}

async fn describe_database(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<DatabaseDetail>, StatusCode> {
    let mut client = state.client.lock().await;
    let info = client
        .describe_database(&name)
        .await
        .map_err(map_db_grpc_err)?;
    Ok(Json(DatabaseDetail {
        name: info.name,
        properties: info.properties,
        created_at_ms: info.created_at_ms,
    }))
}

// ---- Management (Milvus parity) ------------------------------------------

#[derive(Deserialize)]
struct CreateIndexBody {
    /// Field name to index.
    field: String,
    /// One of: keyword | numeric | bool | hnsw (vector — triggers reindex).
    /// `hnsw` is a Milvus-style shortcut for "rebuild the vector index".
    #[serde(default = "default_index_kind")]
    kind: String,
    /// Optional override for the index name (defaults to the field name).
    #[serde(default)]
    index_name: Option<String>,
    /// Opaque key/value metadata stored alongside the collection.
    /// `index.<name>.params.<k>` = v. Forward-compatible with Milvus's
    /// index params surface even though the engine ignores them today.
    #[serde(default)]
    params: std::collections::BTreeMap<String, String>,
    /// Distance metric for the vector index (`cosine|euclidean|dot_product`).
    /// Ignored for non-vector indexes.
    #[serde(default)]
    metric: Option<String>,
}

fn default_index_kind() -> String {
    "keyword".into()
}

async fn create_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<CreateIndexBody>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let kind = body.kind.to_lowercase();
    let index_name = body.index_name.clone().unwrap_or_else(|| body.field.clone());

    // Vector index: Milvus's CreateIndex on a vector field is logically
    // "build the HNSW index". VexaDb auto-builds at create_collection time,
    // so we trigger a reindex (the only way to refresh in place).
    if kind == "hnsw" || kind == "auto" || kind == "autoindex" || kind == "vector" {
        let mut client = state.client.lock().await;
        client
            .reindex_collection(&fqn)
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        // Store index params as opaque collection properties for parity.
        if !body.params.is_empty() || body.metric.is_some() {
            let mut set = std::collections::BTreeMap::new();
            for (k, v) in &body.params {
                set.insert(format!("index.{index_name}.params.{k}"), v.clone());
            }
            if let Some(m) = body.metric.clone() {
                set.insert(format!("index.{index_name}.metric"), m);
            }
            set.insert(format!("index.{index_name}.kind"), kind.clone());
            set.insert(format!("index.{index_name}.field"), body.field.clone());
            let op = serde_json::json!({
                "AlterCollectionProperties": {
                    "name": name,
                    "database": db,
                    "set": set,
                    "unset": [],
                }
            });
            forward_meta_op(&state, op).await?;
        }
        return Ok(Json(serde_json::json!({
            "name": index_name,
            "field": body.field,
            "kind": "hnsw",
            "state": "Finished",
        })));
    }

    // Scalar / payload index: route through MetaOp::AddPayloadIndex.
    let op = serde_json::json!({
        "AddPayloadIndex": {
            "collection": name,
            "database": db,
            "field": body.field,
            "kind": kind,
        }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    {
        let mut client = state.client.lock().await;
        client
            .add_payload_index(bytes)
            .await
            .map_err(map_index_grpc_err)?;
    }
    // Store custom params + index name as collection properties.
    if body.index_name.is_some() || !body.params.is_empty() {
        let mut set = std::collections::BTreeMap::new();
        set.insert(format!("index.{index_name}.field"), body.field.clone());
        set.insert(format!("index.{index_name}.kind"), kind.clone());
        for (k, v) in &body.params {
            set.insert(format!("index.{index_name}.params.{k}"), v.clone());
        }
        let alter = serde_json::json!({
            "AlterCollectionProperties": {
                "name": name,
                "database": db,
                "set": set,
                "unset": [],
            }
        });
        forward_meta_op(&state, alter).await?;
    }
    Ok(Json(serde_json::json!({
        "name": index_name,
        "field": body.field,
        "kind": kind,
        "state": "Finished",
    })))
}

async fn drop_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, field)): Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    // Drop the payload index (no-op when the field has no index).
    let op = serde_json::json!({
        "DropPayloadIndex": {
            "collection": name,
            "database": db,
            "field": field,
        }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    {
        let mut client = state.client.lock().await;
        client
            .drop_payload_index(bytes)
            .await
            .map_err(map_index_grpc_err)?;
    }
    // Also strip the opaque `index.<field>.*` metadata, best-effort.
    let mut unset = vec![
        format!("index.{field}.field"),
        format!("index.{field}.kind"),
        format!("index.{field}.metric"),
    ];
    // Wildcards aren't supported in unset; the explicit keys above cover the
    // common case. Custom param keys will linger but never leak meaning.
    unset.sort();
    unset.dedup();
    let alter = serde_json::json!({
        "AlterCollectionProperties": {
            "name": name,
            "database": db,
            "set": {},
            "unset": unset,
        }
    });
    let _ = forward_meta_op(&state, alter).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_indexes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let (spec, _count) = client
        .describe_collection(&fqn)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let mut out: Vec<Value> = spec
        .payload_indexes
        .iter()
        .map(|p| {
            serde_json::json!({
                "name": p.field,
                "field": p.field,
                "kind": index_kind_to_string(p.kind),
                "scope": "scalar",
            })
        })
        .collect();
    out.push(serde_json::json!({
        "name": "vector",
        "field": "vector",
        "kind": "hnsw",
        "scope": "vector",
        "metric": metric_to_string(spec.metric),
        "m": spec.m,
        "ef_construction": spec.ef_construction,
        "ef_search": spec.ef_search,
    }));
    Ok(Json(serde_json::json!(out)))
}

async fn describe_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, field)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let (spec, vector_count) = client
        .describe_collection(&fqn)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let prefix = format!("index.{field}.");
    let mut params = std::collections::BTreeMap::<String, String>::new();
    for (k, v) in spec.properties.iter() {
        if let Some(rest) = k.strip_prefix(&prefix) {
            params.insert(rest.to_string(), v.clone());
        }
    }
    if field == "vector" {
        return Ok(Json(serde_json::json!({
            "name": "vector",
            "field": "vector",
            "kind": "hnsw",
            "scope": "vector",
            "metric": metric_to_string(spec.metric),
            "m": spec.m,
            "ef_construction": spec.ef_construction,
            "ef_search": spec.ef_search,
            "params": params,
            "state": "Finished",
            "total_rows": vector_count,
            "indexed_rows": vector_count,
            "pending_index_rows": 0,
        })));
    }
    let idx = spec
        .payload_indexes
        .iter()
        .find(|p| p.field == field)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "name": field,
        "field": field,
        "kind": index_kind_to_string(idx.kind),
        "scope": "scalar",
        "params": params,
        "state": "Finished",
        "total_rows": vector_count,
        "indexed_rows": vector_count,
        "pending_index_rows": 0,
    })))
}

#[derive(Deserialize)]
struct AlterIndexBody {
    #[serde(default)]
    set: std::collections::BTreeMap<String, String>,
}

async fn alter_index_properties(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, field)): Path<(String, String)>,
    Json(body): Json<AlterIndexBody>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    // Index properties are stored on the collection as `index.<field>.<key>`
    // so VexaDb honours Milvus's "settable knob" surface without growing a
    // dedicated table. Future engine support can swap this without breaking
    // clients.
    let prefix = format!("index.{field}.");
    let mut set = std::collections::BTreeMap::new();
    for (k, v) in body.set {
        set.insert(format!("{prefix}{k}"), v);
    }
    if set.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }
    let op = serde_json::json!({
        "AlterCollectionProperties": {
            "name": name,
            "database": db,
            "set": set,
            "unset": [],
        }
    });
    forward_meta_op(&state, op).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct DropIndexPropsBody {
    #[serde(default)]
    keys: Vec<String>,
}

async fn drop_index_properties(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, field)): Path<(String, String)>,
    body: Option<Json<DropIndexPropsBody>>,
) -> Result<StatusCode, StatusCode> {
    let db = current_db(&headers);
    let body = body.map(|b| b.0).unwrap_or_else(|| DropIndexPropsBody { keys: vec![] });
    if body.keys.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }
    let prefix = format!("index.{field}.");
    let unset: Vec<String> = body.keys.into_iter().map(|k| format!("{prefix}{k}")).collect();
    let op = serde_json::json!({
        "AlterCollectionProperties": {
            "name": name,
            "database": db,
            "set": {},
            "unset": unset,
        }
    });
    forward_meta_op(&state, op).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Load / Release / Refresh -----------------------------------------------

async fn load_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // VexaDb keeps every collection resident in memory (HNSW is always
    // loaded). LoadCollection just verifies the collection exists and
    // returns success — preserves Milvus's API shape for clients that
    // call it before every batch.
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let _ = client
        .describe_collection(&fqn)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "state": "Loaded",
        "progress": 100,
    })))
}

async fn release_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, StatusCode> {
    // ReleaseCollection is a no-op for VexaDb (no manual unload path);
    // we still validate existence so callers see a consistent 404 on
    // unknown names.
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let _ = client
        .describe_collection(&fqn)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_load_state(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let _ = client
        .describe_collection(&fqn)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "state": "Loaded",
        "progress": 100,
    })))
}

async fn refresh_load(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // Milvus's RefreshLoad reloads the collection so newly inserted points
    // become visible. VexaDb writes are already visible on commit, but we
    // map this to a reindex so callers can force the HNSW to re-balance.
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let resp = client
        .reindex_collection(&fqn)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "state": "Loaded",
        "progress": 100,
        "vectors_reindexed": resp.vectors_reindexed,
    })))
}

// ---- Flush / Compact / Segments ---------------------------------------------

async fn flush_collection_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let resp = client
        .flush_collection(&fqn)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "collection": resp.collection,
        "flush_ts_ms": resp.flush_ts_ms,
        "segment_ids": resp.segment_ids,
        "flushed_segment_ids": resp.flushed_segment_ids,
        "wal_entries": resp.wal_entries,
    })))
}

async fn compact_collection_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let id = client
        .compact_collection(&fqn)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "compaction_id": id,
    })))
}

async fn get_compaction_state_route(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let resp = client
        .get_compaction_state(id)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "compaction_id": resp.compaction_id,
        "collection": resp.collection,
        "state": compaction_state_to_string(resp.state),
        "entries_before": resp.entries_before,
        "entries_after": resp.entries_after,
        "started_ms": resp.started_ms,
        "finished_ms": resp.finished_ms,
        "error": if resp.error.is_empty() { Value::Null } else { Value::String(resp.error) },
    })))
}

async fn list_segments_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let resp = client
        .list_persistent_segments(&fqn)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let segments: Vec<Value> = resp
        .segments
        .into_iter()
        .map(|s| {
            serde_json::json!({
                "id": s.id,
                "collection": s.collection,
                "num_rows": s.num_rows,
                "state": segment_state_to_string(s.state),
                "source": s.source,
            })
        })
        .collect();
    Ok(Json(serde_json::json!(segments)))
}

// ---- Partitions (Milvus parity) ----------------------------------------

#[derive(Debug, serde::Deserialize)]
struct CreatePartitionBody {
    partition_name: String,
}

async fn create_partition_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<CreatePartitionBody>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let op = serde_json::json!({
        "CreatePartition": {
            "collection": name,
            "database": db,
            "partition": body.partition_name,
        }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut client = state.client.lock().await;
    client
        .create_partition(bytes)
        .await
        .map_err(map_index_grpc_err)?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn drop_partition_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, partition)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let op = serde_json::json!({
        "DropPartition": {
            "collection": name,
            "database": db,
            "partition": partition,
        }
    });
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut client = state.client.lock().await;
    client
        .drop_partition(bytes)
        .await
        .map_err(map_index_grpc_err)?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn has_partition_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, partition)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let exists = client
        .has_partition(&fqn, &partition)
        .await
        .map_err(map_index_grpc_err)?;
    Ok(Json(serde_json::json!({ "exists": exists })))
}

async fn list_partitions_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let parts = client
        .list_partitions(&fqn)
        .await
        .map_err(map_index_grpc_err)?;
    Ok(Json(serde_json::json!({ "partitions": parts })))
}

async fn get_partition_stats_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, partition)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let db = current_db(&headers);
    let fqn = fq(&db, &name);
    let mut client = state.client.lock().await;
    let stats = client
        .get_partition_stats(&fqn, &partition)
        .await
        .map_err(map_index_grpc_err)?;
    let map: serde_json::Map<String, Value> = stats
        .into_iter()
        .map(|(k, v)| (k, Value::String(v)))
        .collect();
    Ok(Json(Value::Object(map)))
}

fn map_index_grpc_err(e: anyhow::Error) -> StatusCode {
    let msg = e.to_string();
    if msg.contains("collection not found") {
        StatusCode::NOT_FOUND
    } else if msg.contains("InvalidArgument") || msg.contains("invalid meta") {
        StatusCode::BAD_REQUEST
    } else if msg.contains("PermissionDenied") {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::BAD_GATEWAY
    }
}

fn compaction_state_to_string(s: i32) -> &'static str {
    use vectordb_proto::vectordb::v1::CompactionState;
    match CompactionState::try_from(s).unwrap_or(CompactionState::Unspecified) {
        CompactionState::Running => "Running",
        CompactionState::Completed => "Completed",
        CompactionState::Failed => "Failed",
        CompactionState::Unspecified => "Unspecified",
    }
}

fn segment_state_to_string(s: i32) -> &'static str {
    use vectordb_proto::vectordb::v1::SegmentState;
    match SegmentState::try_from(s).unwrap_or(SegmentState::Unspecified) {
        SegmentState::Growing => "Growing",
        SegmentState::Sealed => "Sealed",
        SegmentState::Flushed => "Flushed",
        SegmentState::Unspecified => "Unspecified",
    }
}

fn map_db_grpc_err(e: anyhow::Error) -> StatusCode {
    let msg = e.to_string();
    if msg.contains("database not found") {
        StatusCode::NOT_FOUND
    } else if msg.contains("database exists") || msg.contains("AlreadyExists") {
        StatusCode::CONFLICT
    } else if msg.contains("not empty") || msg.contains("FailedPrecondition") {
        StatusCode::CONFLICT
    } else if msg.contains("InvalidArgument") || msg.contains("cannot be dropped") {
        StatusCode::BAD_REQUEST
    } else if msg.contains("PermissionDenied") {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::BAD_GATEWAY
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn forward_meta_op(state: &AppState, op: serde_json::Value) -> Result<(), StatusCode> {
    let bytes = serde_json::to_vec(&op).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut client = state.client.lock().await;
    client
        .mutate_collection_meta(bytes)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            // Map a few well-known gRPC errors back to HTTP statuses.
            if msg.contains("alias not found") || msg.contains("collection not found") {
                StatusCode::NOT_FOUND
            } else if msg.contains("alias exists")
                || msg.contains("collection exists")
                || msg.contains("AlreadyExists")
            {
                StatusCode::CONFLICT
            } else if msg.contains("invalid")
                || msg.contains("InvalidArgument")
                || msg.contains("collides")
            {
                StatusCode::BAD_REQUEST
            } else if msg.contains("PermissionDenied") {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_GATEWAY
            }
        })
}

async fn upsert(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<Value>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let points = points_from_body(body.points);
    let mut client = state.client.lock().await;
    let n = client
        .upsert(&fqn, points)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({ "upserted": n })))
}

fn filter_value_to_json(filter: &Value) -> Result<String, StatusCode> {
    if filter.is_null() {
        return Ok(String::new());
    }
    if let Some(expr) = filter.as_str() {
        let parsed = vectordb_core::parse_filter_input(expr)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        return match parsed {
            Some(f) => Ok(serde_json::to_string(&f).unwrap_or_default()),
            None => Ok(String::new()),
        };
    }
    Ok(filter.to_string())
}

fn payload_bytes_to_value(bytes: &[u8]) -> Option<Value> {
    if bytes.is_empty() {
        return None;
    }
    serde_json::from_slice(bytes).ok()
}

async fn search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<SearchBody>,
) -> Result<Json<Vec<SearchHit>>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let filter_json = filter_value_to_json(&body.filter)?;
    let sparse_query = body.sparse_query.map(|s| vectordb_proto::vectordb::v1::SparseVector {
        indices: s.indices,
        values: s.values,
    });
    let search_mode = body.search_mode.as_deref().unwrap_or("dense");
    let hybrid_alpha = body.hybrid_alpha.unwrap_or(0.5);
    let mut client = state.client.lock().await;
    let hits = client
        .search_hybrid(
            &fqn,
            body.vector,
            body.top_k,
            vec![],
            filter_json,
            sparse_query,
            body.text_query,
            search_mode,
            hybrid_alpha,
            body.output_fields,
            body.with_payload,
            body.with_vector,
        )
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(
        hits.into_iter()
            .map(|h| SearchHit {
                id: h.id,
                score: h.score,
                payload: if body.with_payload {
                    payload_bytes_to_value(&h.payload)
                } else {
                    None
                },
                vector: if body.with_vector && !h.vector.is_empty() {
                    Some(h.vector)
                } else {
                    None
                },
            })
            .collect(),
    ))
}

async fn query_points(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<QueryBody>,
) -> Result<Json<Value>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let filter_json = filter_value_to_json(&body.filter)?;
    let mut client = state.client.lock().await;
    let resp = client
        .query(
            &fqn,
            filter_json,
            body.ids,
            body.limit,
            body.offset,
            body.output_fields,
            body.with_payload,
            body.with_vector,
        )
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let points: Vec<Value> = resp
        .points
        .into_iter()
        .map(|p| {
            let payload = payload_bytes_to_value(&p.payload);
            serde_json::json!({
                "id": p.id,
                "values": if body.with_vector && !p.values.is_empty() { Some(p.values) } else { None },
                "payload": payload,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "points": points })))
}

async fn collection_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let mut client = state.client.lock().await;
    let s = client.stats(&fqn).await.map_err(|e| {
        if e.to_string().contains("not found") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::BAD_GATEWAY
        }
    })?;
    let metric = match vectordb_proto::vectordb::v1::DistanceMetric::try_from(s.metric) {
        Ok(vectordb_proto::vectordb::v1::DistanceMetric::Cosine)
        | Ok(vectordb_proto::vectordb::v1::DistanceMetric::Unspecified) => "cosine",
        Ok(vectordb_proto::vectordb::v1::DistanceMetric::Euclidean) => "euclidean",
        Ok(vectordb_proto::vectordb::v1::DistanceMetric::DotProduct) => "dot",
        _ => "cosine",
    };
    Ok(Json(serde_json::json!({
        "name": s.name,
        "vector_count": s.vector_count,
        "dimension": s.dimension,
        "metric": metric,
        "sparse_enabled": s.sparse_enabled,
        "bm25_text_field": s.bm25_text_field,
        "payload_index_count": s.payload_index_count,
        "scalar_quantization": s.scalar_quantization,
    })))
}

fn points_from_body(points: Vec<PointBody>) -> Vec<VectorPoint> {
    points
        .into_iter()
        .map(|p| {
            let sparse = p
                .sparse
                .map(|s| vectordb_proto::vectordb::v1::SparseVector {
                    indices: s.indices,
                    values: s.values,
                });
            VectorPoint {
                id: p.id,
                values: p.values,
                payload: if p.payload.is_null() {
                    Vec::new()
                } else {
                    serde_json::to_vec(&p.payload).unwrap_or_default()
                },
                sparse,
            }
        })
        .collect()
}

async fn bulk_upsert(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<BulkUpsertBody>,
) -> Result<Json<Value>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let points = points_from_body(body.points);
    let chunk_size = body.chunk_size.unwrap_or(500);
    let mut client = state.client.lock().await;
    let n = client
        .bulk_upsert(&fqn, points, chunk_size)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({ "upserted": n })))
}

async fn reindex_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let mut client = state.client.lock().await;
    let resp = client
        .reindex_collection(&fqn)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "vectors_reindexed": resp.vectors_reindexed,
    })))
}

#[derive(Deserialize, Default)]
struct RebalanceBody {
    #[serde(default)]
    dry_run: bool,
}

async fn trigger_rebalance(
    State(state): State<AppState>,
    body: Option<Json<RebalanceBody>>,
) -> Result<Json<Value>, StatusCode> {
    let dry_run = body.map(|b| b.0.dry_run).unwrap_or(false);
    let mut client = state.client.lock().await;
    let resp = client.rebalance(dry_run).await.map_err(|e| {
        // 409 when a sweep is already running, so callers can retry.
        let msg = e.to_string();
        if msg.contains("already in progress") {
            StatusCode::CONFLICT
        } else {
            StatusCode::BAD_GATEWAY
        }
    })?;
    Ok(Json(serde_json::json!({
        "moved": resp.moved,
        "kept": resp.kept,
        "failed": resp.failed,
        "duration_ms": resp.duration_ms,
        "per_collection": resp.per_collection.into_iter().map(|c| serde_json::json!({
            "collection": c.collection,
            "moved": c.moved,
            "kept": c.kept,
            "failed": c.failed,
        })).collect::<Vec<_>>(),
        "dry_run": dry_run,
    })))
}

async fn rebalance_status(
    State(state): State<AppState>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let s = client
        .rebalance_status()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "running": s.running,
        "enabled": s.enabled,
        "interval_secs": s.interval_secs,
        "last_started_unix_ms": s.last_started_unix_ms,
        "last_finished_unix_ms": s.last_finished_unix_ms,
        "last_duration_ms": s.last_duration_ms,
        "last_moved": s.last_moved,
        "last_kept": s.last_kept,
        "last_failed": s.last_failed,
        "last_error": s.last_error,
        "total_moves": s.total_moves,
        "total_sweeps": s.total_sweeps,
    })))
}

async fn cluster_status(
    State(state): State<AppState>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let s = client
        .cluster_status()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "shard_count": s.shard_count,
        "replication_factor": s.replication_factor,
        "last_health_unix_ms": s.last_health_unix_ms,
        "last_config_reload_unix_ms": s.last_config_reload_unix_ms,
        "nodes": s.nodes.into_iter().map(|n| serde_json::json!({
            "id": n.id,
            "grpc": n.grpc,
            "shard_ids": n.shard_ids,
            "healthy": n.healthy,
            "ready": n.ready,
            "is_leader": n.is_leader,
            "source": n.source,
            "primary_for_shards": n.primary_for_shards,
        })).collect::<Vec<_>>(),
    })))
}

async fn compact_wal(
    State(state): State<AppState>,
    Json(body): Json<CompactWalBody>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let resp = client
        .compact_wal(body.snapshot_first)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "entries_before": resp.entries_before,
        "entries_after": resp.entries_after,
        "snapshot": resp.snapshot.map(|s| serde_json::json!({
            "id": s.id,
            "path": s.path,
        })),
    })))
}

async fn delete_points(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<DeletePointsBody>,
) -> Result<Json<Value>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let mut client = state.client.lock().await;
    let n = client
        .delete(&fqn, body.ids)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({ "deleted": n })))
}

async fn get_point(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let fqn = fq(&current_db(&headers), &name);
    let mut client = state.client.lock().await;
    let point = client.get(&fqn, &id).await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    match point {
        Some(p) => {
            let payload: Value = if p.payload.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&p.payload).unwrap_or(Value::Null)
            };
            Ok(Json(serde_json::json!({
                "id": p.id,
                "values": p.values,
                "payload": payload,
            })))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn list_snapshots(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let snaps = client
        .list_snapshots()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!(snaps
        .into_iter()
        .map(|s| serde_json::json!({
            "id": s.id,
            "created_at_ms": s.created_at_ms,
            "path": s.path,
        }))
        .collect::<Vec<_>>())))
}

async fn create_snapshot(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let snap = client
        .create_snapshot()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "id": snap.id,
        "created_at_ms": snap.created_at_ms,
        "path": snap.path,
    })))
}

async fn delete_snapshot(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let mut client = state.client.lock().await;
    client
        .delete_snapshot(&id)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(StatusCode::NO_CONTENT)
}
