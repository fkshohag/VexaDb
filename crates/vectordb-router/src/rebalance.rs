//! Auto-rebalance coordinator.
//!
//! For each collection on each shard, walk every point, recompute its target
//! shard via the consistent-hash ring, and migrate any orphans. Idempotent
//! and crash-safe — re-running after a partial failure reconciles cleanly.
//!
//! The coordinator is owned by the router, which is the only process that
//! knows the *global* topology (the data nodes only know their own peers).
//!
//! Two run modes:
//!   * `RebalanceCoordinator::run_once` — one-shot sweep. Used by manual
//!     triggers (CLI / admin HTTP endpoint).
//!   * `RebalanceCoordinator::spawn_loop` — background task that wakes every
//!     `interval_secs` and runs a sweep if no other rebalance is in flight.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use vectordb_cluster::{shard_for_point, ShardId, ShardRouter};

use crate::pool::ClientPool;

/// Knobs for the auto-rebalance loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceConfig {
    /// Master switch. When false, the loop never runs (manual trigger only).
    #[serde(default)]
    pub enabled: bool,
    /// Sleep between sweeps. Default 60s.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// Scroll page size per shard.
    #[serde(default = "default_page")]
    pub page_size: u32,
    /// Hard cap on points migrated per sweep. 0 = unlimited.
    /// Useful to throttle a large rebalance into multiple windows.
    #[serde(default)]
    pub max_moves_per_sweep: u64,
    /// Skip a sweep when no orphans were detected for this many consecutive
    /// sweeps in a row (back-off when the cluster is balanced).
    #[serde(default = "default_idle_skip")]
    pub idle_skip_after: u32,
}

fn default_interval() -> u64 {
    60
}
fn default_page() -> u32 {
    256
}
fn default_idle_skip() -> u32 {
    5
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_interval(),
            page_size: default_page(),
            max_moves_per_sweep: 0,
            idle_skip_after: default_idle_skip(),
        }
    }
}

/// Last-sweep summary, exposed for `/v1/admin/rebalance/status`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RebalanceStatus {
    pub running: bool,
    pub last_started_unix_ms: Option<u64>,
    pub last_finished_unix_ms: Option<u64>,
    pub last_duration_ms: Option<u64>,
    pub last_moved: u64,
    pub last_kept: u64,
    pub last_failed: u64,
    pub last_error: Option<String>,
    pub total_moves: u64,
    pub total_sweeps: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RebalanceReport {
    pub moved: u64,
    pub kept: u64,
    pub failed: u64,
    pub duration_ms: u64,
    pub per_collection: Vec<CollectionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CollectionReport {
    pub collection: String,
    pub moved: u64,
    pub kept: u64,
    pub failed: u64,
}

/// Owns the lock + last-sweep telemetry. Cheap to clone (Arc inside).
#[derive(Clone)]
pub struct RebalanceCoordinator {
    pool: ClientPool,
    router: Arc<RwLock<ShardRouter>>,
    cfg: RebalanceConfig,
    in_flight: Arc<AtomicBool>,
    status: Arc<Mutex<RebalanceStatus>>,
}

impl RebalanceCoordinator {
    pub fn new(pool: ClientPool, router: Arc<RwLock<ShardRouter>>, cfg: RebalanceConfig) -> Self {
        Self {
            pool,
            router,
            cfg,
            in_flight: Arc::new(AtomicBool::new(false)),
            status: Arc::new(Mutex::new(RebalanceStatus::default())),
        }
    }

    pub fn config(&self) -> &RebalanceConfig {
        &self.cfg
    }

    pub fn status(&self) -> RebalanceStatus {
        let mut s = self.status.lock().clone();
        s.running = self.in_flight.load(Ordering::SeqCst);
        s
    }

    /// Run one sweep over every collection visible on every shard. Returns a
    /// per-collection summary. Returns an error if a sweep is already running
    /// (we use a single global lock to keep semantics simple).
    pub async fn run_once(&self, dry_run: bool) -> anyhow::Result<RebalanceReport> {
        if self
            .in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            anyhow::bail!("rebalance already in progress");
        }
        let started = Instant::now();
        let started_ms = unix_ms();
        {
            let mut s = self.status.lock();
            s.last_started_unix_ms = Some(started_ms);
            s.last_error = None;
        }

        let result = self.run_inner(dry_run).await;
        let elapsed = started.elapsed();
        let finished_ms = unix_ms();

        {
            let mut s = self.status.lock();
            s.last_finished_unix_ms = Some(finished_ms);
            s.last_duration_ms = Some(elapsed.as_millis() as u64);
            s.total_sweeps += 1;
            match &result {
                Ok(report) => {
                    s.last_moved = report.moved;
                    s.last_kept = report.kept;
                    s.last_failed = report.failed;
                    s.total_moves = s.total_moves.saturating_add(report.moved);
                }
                Err(e) => {
                    s.last_error = Some(e.to_string());
                }
            }
        }
        self.in_flight.store(false, Ordering::SeqCst);
        result
    }

    async fn run_inner(&self, dry_run: bool) -> anyhow::Result<RebalanceReport> {
        let started = Instant::now();
        let (shard_endpoints, shard_count) = {
            let router = self.router.read();
            let eps: Vec<(ShardId, String)> = router
                .shard_endpoints()
                .map(|(s, ep)| (s, ep.to_string()))
                .collect();
            (eps, router.shard_count())
        };

        if shard_endpoints.len() < 2 {
            return Ok(RebalanceReport {
                duration_ms: started.elapsed().as_millis() as u64,
                ..Default::default()
            });
        }

        // List collections via the first reachable shard.
        let mut first_client = self
            .pool
            .get(&shard_endpoints[0].1)
            .await
            .map_err(|e| anyhow::anyhow!("connect first shard: {e}"))?;
        let collections = first_client.list_collections().await?;

        // Per-sweep cache: which (target_endpoint, collection) pairs we've
        // already ensured exist on the target shard, and which ones we know
        // we couldn't create (don't keep retrying). Avoids per-point warn spam
        // and per-point describe/create RPCs.
        let mut ensured_targets: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        let mut unrecoverable_targets: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        let mut total = RebalanceReport {
            duration_ms: 0,
            ..Default::default()
        };
        let cap = self.cfg.max_moves_per_sweep;

        'collections: for collection in collections {
            let mut per = CollectionReport {
                collection: collection.clone(),
                moved: 0,
                kept: 0,
                failed: 0,
            };
            for (shard_id, endpoint) in &shard_endpoints {
                let mut src_client = match self.pool.get(endpoint).await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error = %e, shard = shard_id, "rebalance: cannot reach shard, skipping");
                        continue;
                    }
                };
                let mut cursor = String::new();
                loop {
                    let (points, next) = match src_client
                        .scroll(&collection, &cursor, self.cfg.page_size)
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                shard = shard_id,
                                collection = %collection,
                                "scroll failed; skipping rest of this shard for this sweep"
                            );
                            break;
                        }
                    };
                    if points.is_empty() && next.is_empty() {
                        break;
                    }
                    for p in points {
                        let target = shard_for_point(&p.id, shard_count);
                        if target == *shard_id {
                            per.kept += 1;
                            total.kept += 1;
                            continue;
                        }
                        if dry_run {
                            tracing::debug!(
                                id = %p.id,
                                from = shard_id,
                                to = target,
                                "would migrate"
                            );
                            per.moved += 1;
                            total.moved += 1;
                            if cap > 0 && total.moved >= cap {
                                total.per_collection.push(per.clone());
                                break 'collections;
                            }
                            continue;
                        }
                        let target_ep = {
                            let router = self.router.read();
                            router
                                .endpoint_for_shard(target)
                                .map(|s| s.to_string())
                        };
                        let target_ep = match target_ep {
                            Some(ep) => ep,
                            None => {
                                tracing::warn!(target, "no endpoint for target shard");
                                per.failed += 1;
                                total.failed += 1;
                                continue;
                            }
                        };
                        let mut target_client = match self.pool.get(&target_ep).await {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::warn!(target = %target_ep, error = %e, "connect target");
                                per.failed += 1;
                                total.failed += 1;
                                continue;
                            }
                        };
                        // Ensure the target shard has the collection schema.
                        // Without this, every upsert returns "collection not
                        // found" and we'd never make progress (e.g. when a new
                        // shard is added and `add-shard` doesn't push schemas).
                        let ensure_key = (target_ep.clone(), collection.clone());
                        if unrecoverable_targets.contains(&ensure_key) {
                            // Already gave up on this (collection, target) for
                            // this sweep — don't spam a warn per point.
                            per.failed += 1;
                            total.failed += 1;
                            continue;
                        }
                        if !ensured_targets.contains(&ensure_key) {
                            match ensure_collection_on_target(
                                &mut src_client,
                                &mut target_client,
                                &collection,
                            )
                            .await
                            {
                                Ok(()) => {
                                    ensured_targets.insert(ensure_key.clone());
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target = %target_ep,
                                        collection = %collection,
                                        error = %e,
                                        "rebalance: cannot ensure collection on target; \
                                         skipping migration to this shard for this sweep"
                                    );
                                    unrecoverable_targets.insert(ensure_key);
                                    per.failed += 1;
                                    total.failed += 1;
                                    continue;
                                }
                            }
                        }
                        let id = p.id.clone();
                        match target_client.upsert(&collection, vec![p]).await {
                            Ok(_) => {
                                if let Err(e) =
                                    src_client.delete(&collection, vec![id.clone()]).await
                                {
                                    // Two copies for now; next sweep deletes
                                    // the source. Better to leak a duplicate
                                    // briefly than to lose data.
                                    tracing::warn!(
                                        id = %id,
                                        error = %e,
                                        "upserted to target but source delete failed"
                                    );
                                    per.failed += 1;
                                    total.failed += 1;
                                } else {
                                    per.moved += 1;
                                    total.moved += 1;
                                    if cap > 0 && total.moved >= cap {
                                        total.per_collection.push(per.clone());
                                        break 'collections;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(id = %id, error = %e, "upsert to target failed");
                                per.failed += 1;
                                total.failed += 1;
                            }
                        }
                    }
                    if next.is_empty() {
                        break;
                    }
                    cursor = next;
                }
            }
            total.per_collection.push(per);
        }
        total.duration_ms = started.elapsed().as_millis() as u64;
        if total.moved > 0 || total.failed > 0 {
            tracing::info!(
                moved = total.moved,
                kept = total.kept,
                failed = total.failed,
                duration_ms = total.duration_ms,
                dry_run = dry_run,
                "rebalance sweep complete"
            );
        }
        Ok(total)
    }

    /// Spawn the background sweep loop. Honors `enabled` — returns
    /// immediately when the master switch is off.
    pub fn spawn_loop(self) {
        if !self.cfg.enabled {
            tracing::info!("auto-rebalance disabled");
            return;
        }
        let interval = Duration::from_secs(self.cfg.interval_secs.max(5));
        let idle_skip = self.cfg.idle_skip_after;
        tokio::spawn(async move {
            tracing::info!(
                interval_secs = self.cfg.interval_secs,
                page_size = self.cfg.page_size,
                "auto-rebalance loop started"
            );
            let mut tick = tokio::time::interval(interval);
            // First tick fires immediately; skip it so we don't fire on boot
            // before all shards finish replaying their WALs.
            tick.tick().await;
            let mut idle_streak: u32 = 0;
            loop {
                tick.tick().await;
                // Back off when nothing is moving.
                if idle_skip > 0 && idle_streak >= idle_skip {
                    idle_streak = 0;
                    tracing::debug!("auto-rebalance: idle skip");
                    continue;
                }
                match self.run_once(false).await {
                    Ok(report) => {
                        if report.moved == 0 && report.failed == 0 {
                            idle_streak = idle_streak.saturating_add(1);
                        } else {
                            idle_streak = 0;
                        }
                    }
                    Err(e) => {
                        // run_once only errors when a previous sweep is
                        // still in flight (contention) — log and move on.
                        tracing::debug!(error = %e, "auto-rebalance skipped");
                    }
                }
            }
        });
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Make sure `target` knows about `collection`. Idempotent:
///   - if target already has it, return Ok
///   - if not, fetch the spec from `source` and create it on `target`
///   - if the source doesn't have it either (rare race), return Err
async fn ensure_collection_on_target(
    source: &mut vectordb_client::VectorDbClient,
    target: &mut vectordb_client::VectorDbClient,
    collection: &str,
) -> anyhow::Result<()> {
    if target.describe_collection(collection).await.is_ok() {
        return Ok(());
    }
    let (spec, _count) = source
        .describe_collection(collection)
        .await
        .map_err(|e| anyhow::anyhow!("source describe({collection}): {e}"))?;
    match target.create_collection(spec).await {
        Ok(()) => {
            tracing::info!(
                collection = %collection,
                "rebalance: ensured collection schema on target shard"
            );
            Ok(())
        }
        Err(e) => {
            // "collection exists" races are fine — the target had it after all.
            let msg = e.to_string();
            if msg.contains("already exists") || msg.contains("collection exists") {
                Ok(())
            } else {
                Err(anyhow::anyhow!("create on target: {e}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_skip_default_back_off() {
        let cfg = RebalanceConfig::default();
        assert!(cfg.idle_skip_after >= 1);
        assert!(cfg.interval_secs >= 5);
    }

    #[test]
    fn single_shard_is_no_op() {
        // Build a router with one node / one shard.
        use vectordb_cluster::{ClusterConfig, NodeRole, NodeState};
        let cluster = ClusterConfig {
            replication_factor: 1,
            virtual_nodes_per_shard: 4,
            nodes: vec![NodeState {
                id: "n1".into(),
                advertise_addr: "http://127.0.0.1:6334".into(),
                role: NodeRole::Data,
                shard_ids: vec![0],
                healthy: true,
            }],
        };
        let router = Arc::new(RwLock::new(ShardRouter::from_cluster(&cluster, 1)));
        let coord = RebalanceCoordinator::new(
            ClientPool::default(),
            router,
            RebalanceConfig::default(),
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = rt.block_on(coord.run_once(true)).unwrap();
        assert_eq!(report.moved, 0);
        assert_eq!(report.kept, 0);
        assert_eq!(report.failed, 0);
    }
}
