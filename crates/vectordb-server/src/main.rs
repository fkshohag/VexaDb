mod config;
mod service;

use anyhow::Context;
use clap::Parser;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use vectordb_proto::VectorServiceServer;

use crate::config::{load_config, ServerConfig};
use crate::service::VectorServiceImpl;

#[derive(Parser, Debug)]
#[command(name = "vectordb-server", about = "Production vector database node")]
struct Cli {
    /// Path to TOML config file
    #[arg(short, long, env = "VECTORDB_CONFIG")]
    config: Option<std::path::PathBuf>,

    /// gRPC listen address (overrides config)
    #[arg(long, env = "VECTORDB_LISTEN")]
    listen: Option<String>,

    /// Data directory (overrides config)
    #[arg(long, env = "VECTORDB_DATA_DIR")]
    data_dir: Option<std::path::PathBuf>,

    /// Node ID (overrides config)
    #[arg(long, env = "VECTORDB_NODE_ID")]
    node_id: Option<String>,
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

    let svc = VectorServiceImpl::new(cfg.clone())
        .context("failed to initialize VectorDB engine")?;

    let addr = cfg.server.listen.parse()?;
    tracing::info!(
        node_id = %cfg.cluster.node_id,
        listen = %cfg.server.listen,
        shards = cfg.cluster.shard_count,
        "starting VectorDB server"
    );

    Server::builder()
        .add_service(VectorServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
