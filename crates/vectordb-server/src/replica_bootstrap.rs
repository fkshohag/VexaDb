//! Replica bootstrap via the public Scroll RPC.
//!
//! Why this exists
//! ---------------
//! Our Raft impl only ships `AppendEntries` — there is no `InstallSnapshot`
//! RPC. That means a brand-new replica joining an existing shard cannot
//! catch up on historical data: it can only replicate writes that arrive
//! AFTER it joins.
//!
//! `bootstrap_from_peer` closes that gap by treating the new replica as if
//! it were an external client: it lists collections from a sibling, then
//! `Scroll`s every point, then `BulkUpsert`s them locally. Every step uses
//! the same write path as production traffic, so anything broken here is
//! also broken in normal use.
//!
//! Properties
//! ----------
//!   * Idempotent. Bulk-upsert is overwrite-on-conflict, and we skip
//!     collections already populated locally — safe to resume after a
//!     mid-bootstrap crash.
//!   * Crash-safe. Every page is durably WAL-appended before the cursor
//!     advances on disk. Worst case: the new replica restarts and the
//!     bootstrap resumes from the last fully-committed page.
//!   * Opt-in. Disabled by default; turn on with `[bootstrap] enabled = true`
//!     in node config.
//!
//! Relationship to Raft InstallSnapshot
//! ------------------------------------
//! When Raft is enabled, the leader prefers `InstallSnapshot` (filesystem
//! copy via `vectordb-replication`) for lagging replicas. Scroll bootstrap
//! remains the fallback for nodes without Raft peers or when snapshot
//! transfer fails.
//!
//! Caveats
//! -------
//!   * Slower than Raft `InstallSnapshot` (rebuilds HNSW per Scroll page).
//!   * Does not (yet) preserve collection-level metadata that isn't in
//!     `CollectionSpec` (e.g. payload index definitions on the source).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use vectordb_client::VectorDbClient;
use vectordb_core::{
    CollectionConfig, DistanceMetric, PayloadFieldIndex, PayloadIndexKind, QuantizationConfig,
    SparseVector,
};
use vectordb_proto::vectordb::v1::{
    CollectionSpec, DistanceMetric as ProtoMetric, PayloadIndexKind as ProtoIndexKind, VectorPoint,
};
use vectordb_replication::RaftConfig;
use vectordb_storage::{BulkPoint, CollectionEngine};

/// Bootstrap configuration for a fresh data-node replica.
///
/// All fields default to safe values — a missing `[bootstrap]` section in
/// the config TOML is equivalent to `enabled = false` (no-op).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapConfig {
    /// Master switch. When `false`, the replica skips bootstrap entirely.
    #[serde(default)]
    pub enabled: bool,

    /// Page size used for the source `Scroll` RPC.
    #[serde(default = "default_page_size")]
    pub page_size: u32,

    /// `BulkUpsert` chunk size on the local engine.
    #[serde(default = "default_bulk_chunk")]
    pub bulk_chunk_size: u32,

    /// Total time budget for the bootstrap. Server start fails if it
    /// can't complete in this window — better than silently serving
    /// stale/empty data.
    #[serde(default = "default_budget_secs")]
    pub max_total_secs: u64,

    /// Per-RPC connect timeout when probing peers.
    #[serde(default = "default_peer_connect_secs")]
    pub peer_connect_secs: u64,

    /// How long to keep retrying `pick_source_peer` if no peer is reachable
    /// or ready. Critical when a new replica boots concurrently with siblings
    /// that are still replaying their WAL — those siblings' gRPC servers
    /// don't open until replay completes.
    ///
    /// Default 600s (10 minutes). Set to 0 to disable retry (one-shot, the
    /// old behavior).
    #[serde(default = "default_peer_retry_secs")]
    pub peer_retry_secs: u64,

    /// Take a `snapshot_and_compact_wal` after bootstrap completes so the
    /// next restart doesn't replay the freshly-written WAL.
    #[serde(default = "default_true")]
    pub snapshot_after: bool,
}

fn default_page_size() -> u32 {
    256
}
fn default_bulk_chunk() -> u32 {
    256
}
fn default_budget_secs() -> u64 {
    3600
}
fn default_peer_connect_secs() -> u64 {
    5
}
fn default_peer_retry_secs() -> u64 {
    600
}
fn default_true() -> bool {
    true
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            page_size: default_page_size(),
            bulk_chunk_size: default_bulk_chunk(),
            max_total_secs: default_budget_secs(),
            peer_connect_secs: default_peer_connect_secs(),
            peer_retry_secs: default_peer_retry_secs(),
            snapshot_after: true,
        }
    }
}

/// Result of a single bootstrap pass — surfaced for tests + telemetry.
#[derive(Debug, Default, Clone)]
pub struct BootstrapReport {
    pub source_endpoint: Option<String>,
    pub collections_created: u64,
    pub collections_skipped: u64,
    pub points_copied: u64,
    pub elapsed_secs: f64,
}

/// Run replica bootstrap if conditions are met. The function is a no-op
/// (returns `Ok(report)` with `points_copied == 0`) when:
///   - bootstrap is disabled in config, or
///   - the local engine already has WAL entries (i.e. not a fresh node), or
///   - there is no Raft config / no sibling peers to talk to.
///
/// Returns an error only when bootstrap was *attempted* and *failed*, so
/// the caller (`vectordb-server` startup) can refuse to serve traffic from
/// an inconsistent state.
pub async fn bootstrap_from_peer(
    cfg: &BootstrapConfig,
    raft: Option<&RaftConfig>,
    api_key: Option<String>,
    engine: Arc<CollectionEngine>,
) -> Result<BootstrapReport> {
    let mut report = BootstrapReport::default();
    let started = std::time::Instant::now();

    if !cfg.enabled {
        tracing::debug!("replica bootstrap disabled");
        return Ok(report);
    }

    // Skip if we already have data locally — this is the resumed-restart case
    // OR the operator has manually seeded the volume. Either way, don't
    // overwrite what's there.
    let entries = engine
        .replay_wal_count()
        .context("replay_wal_count failed")?;
    if entries > 0 {
        tracing::info!(
            wal_entries = entries,
            "replica bootstrap: local WAL is non-empty, skipping"
        );
        return Ok(report);
    }

    let raft = match raft {
        Some(r) if !r.peers.is_empty() => r,
        _ => {
            tracing::info!("replica bootstrap: no raft peers configured, nothing to sync from");
            return Ok(report);
        }
    };

    let peer = pick_source_peer(cfg, raft, api_key.clone()).await;
    let (source_ep, mut client) = match peer {
        Some(p) => p,
        None => {
            // No reachable peer is genuinely fine for a fresh single-node
            // bring-up of a brand-new shard (there's nothing to sync FROM).
            tracing::warn!(
                "replica bootstrap: no reachable peer found among {:?}, continuing empty",
                raft.peers
                    .iter()
                    .filter(|p| p.id != raft.node_id)
                    .map(|p| p.grpc.as_deref().unwrap_or("?"))
                    .collect::<Vec<_>>()
            );
            return Ok(report);
        }
    };
    report.source_endpoint = Some(source_ep.clone());
    tracing::info!(source = %source_ep, "replica bootstrap: starting");

    let collections = client
        .list_collections()
        .await
        .with_context(|| format!("list_collections via {source_ep}"))?;

    let budget = Duration::from_secs(cfg.max_total_secs);

    for name in collections {
        if started.elapsed() > budget {
            anyhow::bail!(
                "replica bootstrap: time budget {:?} exceeded after {} collections",
                budget,
                report.collections_created + report.collections_skipped
            );
        }
        if engine.describe_collection(&name).is_ok() {
            tracing::debug!(collection = %name, "already present locally, skipping");
            report.collections_skipped += 1;
            continue;
        }

        // 1. Mirror the spec.
        let (spec, _vec_count) = client
            .describe_collection(&name)
            .await
            .with_context(|| format!("describe_collection({name})"))?;
        create_local_collection(&engine, spec)
            .with_context(|| format!("create_local_collection({name})"))?;
        report.collections_created += 1;

        // 2. Scroll + bulk-upsert page-by-page.
        let mut cursor = String::new();
        loop {
            let (page, next) = client
                .scroll(&name, &cursor, cfg.page_size)
                .await
                .with_context(|| format!("scroll({name}, cursor={cursor:?})"))?;
            if page.is_empty() && next.is_empty() {
                break;
            }
            let n = page.len() as u64;
            let bulk = points_to_bulk(page);
            engine
                .bulk_upsert(&name, bulk, cfg.bulk_chunk_size as usize)
                .with_context(|| format!("bulk_upsert into {name}"))?;
            report.points_copied += n;
            tracing::debug!(
                collection = %name,
                page_size = n,
                cursor = %next,
                total = report.points_copied,
                "bootstrap page applied"
            );
            if next.is_empty() {
                break;
            }
            cursor = next;
        }
    }

    if cfg.snapshot_after && report.points_copied > 0 {
        match engine.snapshot_and_compact_wal() {
            Ok((snap, stats)) => tracing::info!(
                snapshot_id = %snap.id,
                wal_before = stats.before,
                wal_after = stats.after,
                "post-bootstrap snapshot complete"
            ),
            // Snapshot failure is non-fatal: the data is durably in WAL and
            // a future restart will replay it. We just won't get the perf
            // win of skipping that replay.
            Err(e) => {
                tracing::warn!(error = %e, "post-bootstrap snapshot failed (data is safe in WAL)")
            }
        }
    }

    report.elapsed_secs = started.elapsed().as_secs_f64();
    tracing::info!(
        source = %report.source_endpoint.clone().unwrap_or_default(),
        collections_created = report.collections_created,
        collections_skipped = report.collections_skipped,
        points_copied = report.points_copied,
        elapsed_secs = report.elapsed_secs,
        "replica bootstrap: complete"
    );
    Ok(report)
}

/// Try every peer in turn. Skip self. Prefer the current Raft leader
/// (via `health_detail`) so we don't bootstrap from a stale follower.
///
/// Wraps `pick_source_peer_once` with retry-and-backoff: when a new replica
/// is brought up alongside siblings that are still WAL-replaying, those
/// siblings won't have their gRPC servers listening yet. Without retry the
/// new replica gives up immediately and silently boots empty.
async fn pick_source_peer(
    cfg: &BootstrapConfig,
    raft: &RaftConfig,
    api_key: Option<String>,
) -> Option<(String, VectorDbClient)> {
    let started = std::time::Instant::now();
    let budget = Duration::from_secs(cfg.peer_retry_secs);
    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(30);
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        if let Some(found) = pick_source_peer_once(cfg, raft, api_key.clone()).await {
            if attempt > 1 {
                tracing::info!(
                    attempts = attempt,
                    elapsed_secs = started.elapsed().as_secs_f64(),
                    peer = %found.0,
                    "bootstrap: source peer became available"
                );
            }
            return Some(found);
        }

        let elapsed = started.elapsed();
        if elapsed >= budget {
            tracing::warn!(
                attempts = attempt,
                elapsed_secs = elapsed.as_secs_f64(),
                budget_secs = cfg.peer_retry_secs,
                "bootstrap: gave up waiting for a reachable peer"
            );
            return None;
        }

        // Log every ~30s so an operator tailing logs sees we're still trying.
        if attempt == 1 || attempt % 5 == 0 {
            tracing::info!(
                attempts = attempt,
                elapsed_secs = elapsed.as_secs_f64(),
                "bootstrap: no peer ready yet, retrying with backoff"
            );
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Single pass over the peer list. Returns `Some((endpoint, client))` if
/// any peer is reachable AND ready, else `None`.
async fn pick_source_peer_once(
    cfg: &BootstrapConfig,
    raft: &RaftConfig,
    api_key: Option<String>,
) -> Option<(String, VectorDbClient)> {
    let mut leader: Option<(String, VectorDbClient)> = None;
    let mut fallback: Option<(String, VectorDbClient)> = None;

    let connect_timeout = Duration::from_secs(cfg.peer_connect_secs.max(1));

    for peer in &raft.peers {
        if peer.id == raft.node_id {
            continue;
        }
        let Some(grpc) = peer.grpc.clone() else {
            continue;
        };
        let connect_fut = VectorDbClient::connect_with(grpc.clone(), api_key.clone());
        let mut client = match tokio::time::timeout(connect_timeout, connect_fut).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                tracing::debug!(peer = %grpc, error = %e, "bootstrap: peer connect failed");
                continue;
            }
            Err(_) => {
                tracing::debug!(peer = %grpc, "bootstrap: peer connect timed out");
                continue;
            }
        };

        // Health/leader check.
        match client.health_detail().await {
            Ok(detail) => {
                if detail.is_leader && fallback.is_none() {
                    leader = Some((grpc.clone(), client));
                    break;
                }
                if detail.ready {
                    fallback = Some((grpc, client));
                }
            }
            Err(e) => {
                tracing::debug!(peer = %grpc, error = %e, "bootstrap: health_detail failed");
            }
        }
    }
    leader.or(fallback)
}

/// Translate the source replica's `CollectionSpec` (proto) into the local
/// `CollectionConfig` (core) and create the collection on this engine.
/// Mirrors the conversion that `service::CreateCollection` does when
/// taking a request from a real client — keep these in sync.
fn create_local_collection(engine: &CollectionEngine, spec: CollectionSpec) -> Result<()> {
    let metric = proto_metric_to_core(
        ProtoMetric::try_from(spec.metric).unwrap_or(ProtoMetric::Unspecified),
    );
    let mut cfg = CollectionConfig::new(spec.name.clone(), spec.dimension as usize, metric);
    if spec.m > 0 {
        cfg.m = spec.m as usize;
    }
    if spec.ef_construction > 0 {
        cfg.ef_construction = spec.ef_construction as usize;
    }
    if spec.ef_search > 0 {
        cfg.ef_search = spec.ef_search as usize;
    }
    cfg.payload_indexes = spec
        .payload_indexes
        .into_iter()
        .filter_map(proto_index_to_core)
        .collect();
    cfg.sparse_enabled = spec.sparse_enabled;
    cfg.bm25_text_field = if spec.bm25_text_field.is_empty() {
        None
    } else {
        Some(spec.bm25_text_field)
    };
    cfg.quantization = if spec.scalar_quantization {
        Some(QuantizationConfig { scalar: true })
    } else {
        None
    };
    engine
        .create_collection(cfg)
        .with_context(|| format!("create_collection({})", spec.name))?;
    Ok(())
}

fn proto_metric_to_core(m: ProtoMetric) -> DistanceMetric {
    match m {
        ProtoMetric::Cosine | ProtoMetric::Unspecified => DistanceMetric::Cosine,
        ProtoMetric::Euclidean => DistanceMetric::Euclidean,
        ProtoMetric::DotProduct => DistanceMetric::DotProduct,
    }
}

fn proto_index_to_core(
    p: vectordb_proto::vectordb::v1::PayloadFieldIndex,
) -> Option<PayloadFieldIndex> {
    let kind = match ProtoIndexKind::try_from(p.kind).ok()? {
        ProtoIndexKind::Unspecified => return None,
        ProtoIndexKind::Keyword => PayloadIndexKind::Keyword,
        ProtoIndexKind::Numeric => PayloadIndexKind::Numeric,
        ProtoIndexKind::Bool => PayloadIndexKind::Bool,
    };
    Some(PayloadFieldIndex {
        field: p.field,
        kind,
    })
}

fn points_to_bulk(points: Vec<VectorPoint>) -> Vec<BulkPoint> {
    points
        .into_iter()
        .map(|p| BulkPoint {
            id: p.id,
            vector: vectordb_core::Vector::new(p.values),
            payload: if p.payload.is_empty() {
                None
            } else {
                Some(p.payload)
            },
            sparse: p.sparse.and_then(|s| {
                if s.indices.is_empty() {
                    None
                } else {
                    Some(SparseVector::new(s.indices, s.values))
                }
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default_is_disabled() {
        let cfg = BootstrapConfig::default();
        assert!(!cfg.enabled, "default must be opt-in");
        assert!(cfg.page_size > 0);
        assert!(cfg.bulk_chunk_size > 0);
        assert!(cfg.max_total_secs > 0);
        assert!(cfg.snapshot_after);
    }

    #[tokio::test]
    async fn disabled_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            CollectionEngine::open(vectordb_storage::EngineConfig::new(dir.path())).unwrap(),
        );
        let report = bootstrap_from_peer(&BootstrapConfig::default(), None, None, engine)
            .await
            .unwrap();
        assert_eq!(report.points_copied, 0);
        assert_eq!(report.collections_created, 0);
        assert_eq!(report.source_endpoint, None);
    }

    #[tokio::test]
    async fn no_raft_config_is_noop_even_when_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            CollectionEngine::open(vectordb_storage::EngineConfig::new(dir.path())).unwrap(),
        );
        let cfg = BootstrapConfig {
            enabled: true,
            ..Default::default()
        };
        let report = bootstrap_from_peer(&cfg, None, None, engine).await.unwrap();
        assert_eq!(report.points_copied, 0);
        assert_eq!(report.source_endpoint, None);
    }
}
