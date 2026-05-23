use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::Response,
    routing::{delete, get, post},
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
use vectordb_auth::{AuthConfig, AuthError, HEADER_API_KEY, HEADER_AUTHORIZATION};
use vectordb_client::{cosine_collection, VectorDbClient};
use vectordb_proto::vectordb::v1::{
    DistanceMetric, PayloadFieldIndex, PayloadIndexKind, VectorPoint,
};

#[derive(Parser, Debug)]
#[command(name = "vectordb-gateway", about = "HTTP/JSON gateway for VectorDB")]
struct Cli {
    #[arg(long, env = "VECTORDB_GRPC", default_value = "http://127.0.0.1:6334")]
    grpc: String,

    #[arg(long, env = "VECTORDB_HTTP", default_value = "0.0.0.0:8080")]
    listen: String,

    /// Comma-separated API keys (enables auth when set).
    #[arg(long, env = "VECTORDB_API_KEYS")]
    api_keys: Option<String>,

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
struct AppState {
    client: Arc<Mutex<VectorDbClient>>,
    auth: Arc<AuthConfig>,
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
    let mut auth = AuthConfig::default();
    if let Some(keys) = &cli.api_keys {
        auth.keys = keys
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        auth.required = true;
    }
    let api_key = auth.keys.first().cloned();
    let client = VectorDbClient::connect_with(&cli.grpc, api_key).await?;
    let state = AppState {
        client: Arc::new(Mutex::new(client)),
        auth: Arc::new(auth),
    };

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
        .route("/v1/collections/:name/upsert", post(upsert))
        .route("/v1/collections/:name/bulk", post(bulk_upsert))
        .route("/v1/collections/:name/reindex", post(reindex_collection))
        .route("/v1/collections/:name/search", post(search))
        .route("/v1/admin/compact-wal", post(compact_wal))
        .route("/v1/collections/:name/points", delete(delete_points))
        .route("/v1/collections/:name/points/:id", get(get_point))
        .route("/v1/snapshots", get(list_snapshots).post(create_snapshot))
        .route("/v1/snapshots/:id", delete(delete_snapshot))
        .layer(from_fn_with_state(state.clone(), auth_middleware))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: SocketAddr = cli.listen.parse()?;
    tracing::info!(
        http = %cli.listen,
        grpc = %cli.grpc,
        auth = cli.api_keys.is_some(),
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

async fn auth_middleware(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = request.uri().path();
    if matches!(path, "/health" | "/live" | "/ready" | "/metrics") {
        return Ok(next.run(request).await);
    }
    HTTP_REQUESTS.inc();
    let authorization = headers
        .get(HEADER_AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let api_key = headers.get(HEADER_API_KEY).and_then(|v| v.to_str().ok());
    state
        .auth
        .check_headers(authorization, api_key)
        .map_err(|e| match e {
            AuthError::MissingKey | AuthError::InvalidKey => StatusCode::UNAUTHORIZED,
        })?;
    Ok(next.run(request).await)
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

async fn search(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<SearchBody>,
) -> Result<Json<Vec<SearchHit>>, StatusCode> {
    let filter_json = if body.filter.is_null() {
        String::new()
    } else {
        body.filter.to_string()
    };
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
        )
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(
        hits.into_iter()
            .map(|h| SearchHit {
                id: h.id,
                score: h.score,
            })
            .collect(),
    ))
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
                })
                .unwrap_or_default();
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
