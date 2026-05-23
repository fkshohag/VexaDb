//! Prometheus metrics registry and HTTP scrape endpoint.

use std::net::SocketAddr;
use std::time::Instant;

use axum::{routing::get, Router};
use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, Encoder, HistogramVec, IntCounterVec,
    TextEncoder,
};

lazy_static! {
    pub static ref RPC_REQUESTS: IntCounterVec = register_int_counter_vec!(
        "vectordb_rpc_requests_total",
        "Total gRPC requests",
        &["rpc", "status"]
    )
    .unwrap();
    pub static ref RPC_DURATION: HistogramVec = register_histogram_vec!(
        "vectordb_rpc_duration_seconds",
        "gRPC request duration",
        &["rpc"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
    )
    .unwrap();
}

pub struct RpcTimer {
    rpc: &'static str,
    start: Instant,
}

impl RpcTimer {
    pub fn start(rpc: &'static str) -> Self {
        Self {
            rpc,
            start: Instant::now(),
        }
    }

    pub fn finish(self, ok: bool) {
        let status = if ok { "ok" } else { "error" };
        RPC_REQUESTS.with_label_values(&[self.rpc, status]).inc();
        RPC_DURATION
            .with_label_values(&[self.rpc])
            .observe(self.start.elapsed().as_secs_f64());
    }
}

async fn scrape_handler() -> String {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .unwrap_or_default();
    String::from_utf8(buffer).unwrap_or_default()
}

/// Spawn a background HTTP server exposing `GET /metrics`.
pub async fn spawn_metrics_server(listen: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new().route("/metrics", get(scrape_handler));
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "prometheus metrics listening");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "metrics server stopped");
        }
    });
    Ok(())
}
