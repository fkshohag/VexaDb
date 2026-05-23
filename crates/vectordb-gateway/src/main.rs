use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;
use vectordb_client::{cosine_collection, VectorDbClient};
use vectordb_proto::vectordb::v1::VectorPoint;

#[derive(Parser, Debug)]
#[command(name = "vectordb-gateway", about = "HTTP/JSON gateway for VectorDB")]
struct Cli {
    #[arg(long, env = "VECTORDB_GRPC", default_value = "http://127.0.0.1:6334")]
    grpc: String,

    #[arg(long, env = "VECTORDB_HTTP", default_value = "0.0.0.0:8080")]
    listen: String,
}

#[derive(Clone)]
struct AppState {
    client: Arc<Mutex<VectorDbClient>>,
}

#[derive(Serialize)]
struct HealthBody {
    status: String,
}

#[derive(Deserialize)]
struct CreateCollectionBody {
    name: String,
    dimension: u32,
}

#[derive(Deserialize)]
struct UpsertBody {
    points: Vec<PointBody>,
}

#[derive(Deserialize, Serialize, Clone)]
struct PointBody {
    id: String,
    values: Vec<f32>,
    #[serde(default)]
    payload: Option<String>,
}

#[derive(Deserialize)]
struct SearchBody {
    vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    top_k: u32,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("vectordb=info".parse()?))
        .init();

    let cli = Cli::parse();
    let client = VectorDbClient::connect(&cli.grpc).await?;
    let state = AppState {
        client: Arc::new(Mutex::new(client)),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/collections", get(list_collections).post(create_collection))
        .route(
            "/v1/collections/:name",
            get(describe_collection).delete(delete_collection),
        )
        .route("/v1/collections/:name/upsert", post(upsert))
        .route("/v1/collections/:name/search", post(search))
        .route("/v1/collections/:name/points", delete(delete_points))
        .route("/v1/collections/:name/points/:id", get(get_point))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
    tracing::info!(http = %cli.listen, grpc = %cli.grpc, "vectordb-gateway listening");
    axum::serve(listener, app).await?;
    Ok(())
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
    let mut client = state.client.lock().await;
    client
        .create_collection(cosine_collection(&body.name, body.dimension))
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(StatusCode::CREATED)
}

async fn describe_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let (spec, count) = client
        .describe_collection(&name)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "spec": spec,
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
) -> Result<Json<serde_json::Value>, StatusCode> {
    let points: Vec<VectorPoint> = body
        .points
        .into_iter()
        .map(|p| VectorPoint {
            id: p.id,
            values: p.values,
            payload: p.payload.unwrap_or_default().into_bytes(),
        })
        .collect();
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
    let mut client = state.client.lock().await;
    let hits = client
        .search(&name, body.vector, body.top_k)
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

async fn delete_points(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<DeletePointsBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
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
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut client = state.client.lock().await;
    let point = client.get(&name, &id).await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    match point {
        Some(p) => Ok(Json(serde_json::json!({
            "id": p.id,
            "values": p.values,
            "payload": String::from_utf8_lossy(&p.payload),
        }))),
        None => Err(StatusCode::NOT_FOUND),
    }
}
