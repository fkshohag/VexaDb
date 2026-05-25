mod rbac;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
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
        .route("/v1/collections/:name/search", post(search))
        .route("/v1/collections/:name/query", post(query_points))
        .route("/v1/collections/:name/stats", get(collection_stats))
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

async fn list_collections(State(state): State<AppState>) -> Result<Json<Vec<String>>, StatusCode> {
    let mut client = state.client.lock().await;
    client
        .list_collections()
        .await
        .map(Json)
        .map_err(|_| StatusCode::BAD_GATEWAY)
}

async fn create_collection(
    State(state): State<AppState>,
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
    spec.metric = metric as i32;
    spec.payload_indexes = payload_indexes;
    spec.sparse_enabled = body.sparse_enabled.unwrap_or(false);
    spec.bm25_text_field = body.bm25_text_field.unwrap_or_default();
    spec.scalar_quantization = body.scalar_quantization.unwrap_or(false);

    let mut client = state.client.lock().await;
    client
        .create_collection(spec)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(StatusCode::CREATED)
}

async fn describe_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let (spec, count) = client
        .describe_collection(&name)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "spec": format!("{spec:?}"),
        "vector_count": count,
    })))
}

async fn delete_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let mut client = state.client.lock().await;
    client
        .delete_collection(&name)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn upsert(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<Value>, StatusCode> {
    let points = points_from_body(body.points);
    let mut client = state.client.lock().await;
    let n = client
        .upsert(&name, points)
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
    Path(name): Path<String>,
    Json(body): Json<SearchBody>,
) -> Result<Json<Vec<SearchHit>>, StatusCode> {
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
            &name,
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
    Path(name): Path<String>,
    Json(body): Json<QueryBody>,
) -> Result<Json<Value>, StatusCode> {
    let filter_json = filter_value_to_json(&body.filter)?;
    let mut client = state.client.lock().await;
    let resp = client
        .query(
            &name,
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
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let s = client.stats(&name).await.map_err(|e| {
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
    Path(name): Path<String>,
    Json(body): Json<BulkUpsertBody>,
) -> Result<Json<Value>, StatusCode> {
    let points = points_from_body(body.points);
    let chunk_size = body.chunk_size.unwrap_or(500);
    let mut client = state.client.lock().await;
    let n = client
        .bulk_upsert(&name, points, chunk_size)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({ "upserted": n })))
}

async fn reindex_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let resp = client
        .reindex_collection(&name)
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
    Path(name): Path<String>,
    Json(body): Json<DeletePointsBody>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let n = client
        .delete(&name, body.ids)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({ "deleted": n })))
}

async fn get_point(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let point = client.get(&name, &id).await.map_err(|_| StatusCode::BAD_GATEWAY)?;
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
