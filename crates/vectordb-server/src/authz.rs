//! Server-specific RBAC cache refresh helpers (engine-local and remote).

use std::sync::Arc;
use std::time::Duration;

pub use vectordb_rbac::{require_collection, RbacCache, RbacInterceptor};
use vectordb_storage::CollectionEngine;

/// Copy RBAC state from the local engine into the cache.
pub fn refresh_from_engine(cache: &RbacCache, engine: &CollectionEngine) {
    let snap = engine.rbac().snapshot();
    cache.install(snap);
}

/// Background refresh from a local engine (data nodes).
pub fn spawn_engine_refresh(cache: RbacCache, engine: Arc<CollectionEngine>, period: Duration) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.tick().await;
        loop {
            tick.tick().await;
            refresh_from_engine(&cache, &engine);
        }
    });
}

/// Background refresh from a sibling backend via gRPC (routers).
pub fn spawn_remote_refresh(
    cache: RbacCache,
    endpoint: String,
    api_key: Option<String>,
    period: Duration,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            if let Ok(mut client) =
                vectordb_client::VectorDbClient::connect_with(endpoint.clone(), api_key.clone()).await
            {
                if let Ok(bytes) = client.get_rbac_snapshot().await {
                    if !bytes.is_empty() {
                        if let Ok(snap) = serde_json::from_slice::<vectordb_rbac::RbacSnapshot>(&bytes) {
                            cache.install(snap);
                        }
                    }
                }
            }
        }
    });
}
