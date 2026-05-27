use std::net::SocketAddr;

use anyhow::Context;
use clap::Parser;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use vectordb_proto::VectorServiceServer;
use vectordb_router::{RebalanceCoordinator, RouterService};

use vectordb_rbac::{GrpcMethodLayer, RbacCache, RbacInterceptor};
use vectordb_server::authz::spawn_remote_refresh;
use vectordb_server::config::{load_config, ServerConfig};
use vectordb_server::metrics;
use vectordb_server::service::VectorServiceImpl;

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
    let config_path = cli.config.clone();
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
            auto_rebalance = cfg.rebalance.enabled,
            auto_topology = true,
            "starting VectorDB router"
        );
        let rbac_cache = RbacCache::new(cfg.auth.keys.clone());
        let auth = RbacInterceptor::new(rbac_cache.clone(), true);
        let (mut router, topology) = RouterService::with_topology(
            cfg.cluster.node_id.clone(),
            &cluster,
            cfg.cluster.shard_count,
            cfg.rebalance.clone(),
            cfg.topology.clone(),
            config_path.clone(),
        );
        router.set_rbac_cache(rbac_cache.clone());
        // Pull RBAC snapshot from the first configured peer (or self via pool later).
        let refresh_ep = cluster
            .nodes
            .first()
            .map(|n| n.advertise_addr.clone())
            .unwrap_or_else(|| cfg.vector_endpoint());
        spawn_remote_refresh(
            rbac_cache.clone(),
            refresh_ep,
            cfg.auth.keys.first().cloned(),
            std::time::Duration::from_secs(5),
        );
        topology.spawn_background();

        if let Some(path) = config_path.clone() {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let topo = topology.clone();
                let path_sighup = path.clone();
                tokio::spawn(async move {
                    if let Ok(mut stream) = signal(SignalKind::hangup()) {
                        while stream.recv().await.is_some() {
                            tracing::info!("SIGHUP: reloading router config");
                            if let Ok(cfg) = load_config(&path_sighup) {
                                let cluster = cfg.cluster_config();
                                topo.apply_static(cluster.nodes, cfg.cluster.shard_count);
                                topo.mark_config_reloaded();
                            }
                        }
                    }
                });
            }

            let reload_secs = cfg.topology.config_reload_interval_secs;
            if reload_secs > 0 {
                let topo = topology.clone();
                tokio::spawn(async move {
                    let every = std::time::Duration::from_secs(reload_secs.max(10));
                    let mut tick = tokio::time::interval(every);
                    tick.tick().await;
                    loop {
                        tick.tick().await;
                        if topo.config_file_changed(&path) {
                            if let Ok(cfg) = load_config(&path) {
                                let cluster = cfg.cluster_config();
                                topo.apply_static(cluster.nodes, cfg.cluster.shard_count);
                                topo.mark_config_reloaded();
                            }
                        }
                    }
                });
            }
        }

        let coordinator: RebalanceCoordinator = router.rebalance();
        coordinator.spawn_loop();
        server
            // `GrpcMethodLayer` captures the gRPC method name from the
            // HTTP URI path so `RbacInterceptor` can read it (tonic 0.12
            // does not insert `tonic::GrpcMethod` on the server side).
            .layer(GrpcMethodLayer)
            .layer(tonic::service::interceptor(auth))
            .add_service(VectorServiceServer::new(router))
            .serve(addr)
            .await?;
        return Ok(());
    }

    let svc = VectorServiceImpl::new(cfg.clone())
        .await
        .context("failed to initialize VectorDB engine")?;

    // Use the service's cache (already seeded from engine) for the interceptor.
    let auth = RbacInterceptor::new(svc.rbac_cache(), true);

    // Replica bootstrap runs in the background so cold start never blocks
    // gRPC for `peer_retry_secs` (default 10 min) while siblings are still
    // WAL-replaying. The engine is already open; WAL replay happened in
    // `VectorServiceImpl::new`. If the local WAL is non-empty, bootstrap
    // returns immediately in the background task anyway.
    if cfg.bootstrap.enabled {
        let bootstrap_cfg = cfg.bootstrap.clone();
        let raft = cfg.raft.clone();
        let bootstrap_api_key = cfg.auth.keys.first().cloned();
        let engine = svc.engine_handle();
        tokio::spawn(async move {
            match vectordb_server::replica_bootstrap::bootstrap_from_peer(
                &bootstrap_cfg,
                raft.as_ref(),
                bootstrap_api_key,
                engine,
            )
            .await
            {
                Ok(report) if report.points_copied > 0 => {
                    tracing::info!(
                        source = %report.source_endpoint.clone().unwrap_or_default(),
                        collections_created = report.collections_created,
                        points_copied = report.points_copied,
                        elapsed_secs = report.elapsed_secs,
                        "replica bootstrap installed historical state"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(error = %e, "background replica bootstrap failed");
                }
            }
        });
    }

    if cfg.snapshot.interval_secs > 0 {
        spawn_snapshot_loop(svc.engine_handle(), cfg.snapshot.clone());
    }

    tracing::info!(
        node_id = %cfg.cluster.node_id,
        listen = %cfg.server.listen,
        shards = cfg.cluster.shard_count,
        role = ?cfg.cluster.role,
        auth = cfg.auth.is_enabled(),
        raft = cfg.raft_enabled(),
        snapshot_interval_secs = cfg.snapshot.interval_secs,
        bootstrap_enabled = cfg.bootstrap.enabled,
        "starting VectorDB data node"
    );

    server
        .layer(GrpcMethodLayer)
        .layer(tonic::service::interceptor(auth))
        .add_service(VectorServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}

fn spawn_snapshot_loop(
    engine: std::sync::Arc<vectordb_storage::CollectionEngine>,
    cfg: vectordb_server::config::SnapshotSection,
) {
    let interval = std::time::Duration::from_secs(cfg.interval_secs);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        // Skip the immediate fire on startup — we just replayed WAL.
        tick.tick().await;
        loop {
            tick.tick().await;
            let cfg = cfg.clone();
            let engine = engine.clone();
            // Snapshot is sync + heavy; run on blocking pool.
            let result = tokio::task::spawn_blocking(move || {
                // Skip if WAL is small — avoids churn on idle nodes.
                let entry_count = engine.replay_wal_count().unwrap_or(usize::MAX);
                if entry_count < cfg.min_wal_entries {
                    return Ok::<_, vectordb_storage::EngineError>(None);
                }
                if cfg.compact_wal {
                    let (snap, stats) = engine.snapshot_and_compact_wal()?;
                    Ok(Some((snap.id, Some(stats))))
                } else {
                    let snap = engine.snapshot_manager().create()?;
                    Ok(Some((snap.id, None)))
                }
            })
            .await;
            match result {
                Ok(Ok(Some((id, Some(stats))))) => tracing::info!(
                    snapshot_id = %id,
                    wal_before = stats.before,
                    wal_after = stats.after,
                    "auto-snapshot complete (WAL compacted)"
                ),
                Ok(Ok(Some((id, None)))) => tracing::info!(
                    snapshot_id = %id,
                    "auto-snapshot complete"
                ),
                Ok(Ok(None)) => tracing::debug!("auto-snapshot skipped (WAL below threshold)"),
                Ok(Err(e)) => tracing::warn!(error = %e, "auto-snapshot failed"),
                Err(e) => tracing::warn!(error = %e, "auto-snapshot task panicked"),
            }
        }
    });
}
