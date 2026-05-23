mod auth_interceptor;
mod config;
mod leader;
mod metrics;
mod replication;
mod service;
mod tls;

use std::net::SocketAddr;

use anyhow::Context;
use clap::Parser;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use vectordb_proto::VectorServiceServer;
use vectordb_router::RouterService;

use crate::auth_interceptor::ApiKeyInterceptor;
use crate::config::{load_config, ServerConfig};
use crate::service::VectorServiceImpl;

#[derive(Parser, Debug)]
#[command(name = "vectordb-server", about = "Production vector database node")]
struct Cli {
    #[arg(short, long, env = "VECTORDB_CONFIG")]
    config: Option<std::path::PathBuf>,

    #[arg(long, env = "VECTORDB_LISTEN")]
    listen: Option<String>,

    #[arg(long, env = "VECTORDB_DATA_DIR")]
    data_dir: Option<std::path::PathBuf>,

    #[arg(long, env = "VECTORDB_NODE_ID")]
    node_id: Option<String>,

    /// Comma-separated API keys (overrides config when set).
    #[arg(long, env = "VECTORDB_API_KEYS")]
    api_keys: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("vectordb=info".parse()?))
        .json()
        .init();

    let cli = Cli::parse();
    let mut cfg: ServerConfig = if let Some(path) = cli.config {
        load_config(&path)?
    } else {
        ServerConfig::default_local()
    };

    if let Some(listen) = cli.listen {
        cfg.server.listen = listen;
    }
    if let Some(data_dir) = cli.data_dir {
        cfg.storage.data_dir = data_dir;
    }
    if let Some(node_id) = cli.node_id {
        cfg.cluster.node_id = node_id;
    }
    if let Some(keys) = cli.api_keys {
        cfg.auth.keys = keys
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        cfg.auth.required = true;
    }

    if let Some(metrics) = &cfg.metrics {
        let listen: SocketAddr = metrics.listen.parse().context("invalid metrics.listen")?;
        metrics::spawn_metrics_server(listen).await?;
    }

    let addr = cfg.server.listen.parse()?;
    let cluster = cfg.cluster_config();
    let auth = ApiKeyInterceptor::new(cfg.auth.clone(), true);
    let mut server = Server::builder();

    if let Some(tls) = &cfg.tls {
        server = server.tls_config(tls.server_tls_config()?)?;
        tracing::info!("gRPC TLS enabled");
    }

    if cfg.is_router() {
        tracing::info!(
            node_id = %cfg.cluster.node_id,
            listen = %cfg.server.listen,
            shards = cfg.cluster.shard_count,
            role = "router",
            auth = cfg.auth.is_enabled(),
            "starting VectorDB router"
        );
        let router = RouterService::new(
            cfg.cluster.node_id.clone(),
            &cluster,
            cfg.cluster.shard_count,
        );
        server
            .layer(tonic::service::interceptor(auth))
            .add_service(VectorServiceServer::new(router))
            .serve(addr)
            .await?;
        return Ok(());
    }

    let svc = VectorServiceImpl::new(cfg.clone())
        .await
        .context("failed to initialize VectorDB engine")?;

    tracing::info!(
        node_id = %cfg.cluster.node_id,
        listen = %cfg.server.listen,
        shards = cfg.cluster.shard_count,
        role = ?cfg.cluster.role,
        auth = cfg.auth.is_enabled(),
        raft = cfg.raft_enabled(),
        "starting VectorDB data node"
    );

    server
        .layer(tonic::service::interceptor(auth))
        .add_service(VectorServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
