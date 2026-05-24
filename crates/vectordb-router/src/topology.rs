//! Dynamic cluster topology for the router.
//!
//! Merges three sources of truth:
//!   1. Static `[[cluster.nodes]]` from the router TOML (hot-reloaded).
//!   2. Self-registration RPCs from data nodes (`RegisterNode`).
//!   3. Periodic health probes that mark nodes up/down and pick a live
//!      Raft leader (or any ready replica) as the shard primary endpoint.
//!
//! When topology changes (new node, dead node, config edit), the shared
//! `ShardRouter` is rebuilt and the auto-rebalance loop can migrate orphans.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use vectordb_cluster::{ClusterConfig, NodeRole, NodeState, ShardRouter};
use vectordb_proto::vectordb::v1::HealthResponse;

use crate::pool::ClientPool;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConfig {
    /// Probe every node's `/Health` on this interval.
    #[serde(default = "default_health_interval")]
    pub health_interval_secs: u64,
    /// Re-read the router TOML when its mtime changes (also on SIGHUP).
    /// 0 disables polling (SIGHUP still reloads when enabled via signal).
    #[serde(default = "default_config_reload")]
    pub config_reload_interval_secs: u64,
    /// Dynamic registrations expire if not refreshed within this window.
    #[serde(default = "default_registration_ttl")]
    pub registration_ttl_secs: u64,
    /// When true, bump `shard_count` to cover the highest registered shard id.
    #[serde(default = "default_true")]
    pub auto_shard_count: bool,
}

fn default_health_interval() -> u64 {
    10
}
fn default_config_reload() -> u64 {
    30
}
fn default_registration_ttl() -> u64 {
    90
}
fn default_true() -> bool {
    true
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            health_interval_secs: default_health_interval(),
            config_reload_interval_secs: default_config_reload(),
            registration_ttl_secs: default_registration_ttl(),
            auto_shard_count: true,
        }
    }
}

#[derive(Debug, Clone)]
struct LiveRegistration {
    state: NodeState,
    last_seen: Instant,
    shard_count_hint: u32,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TopologyStatus {
    pub shard_count: u32,
    pub replication_factor: usize,
    pub nodes: Vec<NodeStatus>,
    pub last_health_unix_ms: u64,
    pub last_config_reload_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStatus {
    pub id: String,
    pub grpc: String,
    pub shard_ids: Vec<u32>,
    pub healthy: bool,
    pub ready: bool,
    pub is_leader: bool,
    pub source: String,
    pub primary_for_shards: Vec<u32>,
}

/// Shared, atomically-updated routing table.
#[derive(Clone)]
pub struct TopologyManager {
    pool: ClientPool,
    cfg: TopologyConfig,
    config_path: Option<PathBuf>,
    shard_count: Arc<RwLock<u32>>,
    virtual_nodes: u32,
    replication_factor: usize,
    /// Nodes from the on-disk router config.
    static_nodes: Arc<RwLock<Vec<NodeState>>>,
    /// Nodes that called `RegisterNode` (heartbeats).
    dynamic: Arc<RwLock<HashMap<String, LiveRegistration>>>,
    router: Arc<RwLock<ShardRouter>>,
    last_config_mtime: Arc<RwLock<Option<SystemTime>>>,
    status: Arc<RwLock<TopologyStatus>>,
    reload_flag: Arc<std::sync::atomic::AtomicBool>,
}

impl TopologyManager {
    pub fn new(
        pool: ClientPool,
        initial_cluster: &ClusterConfig,
        shard_count: u32,
        cfg: TopologyConfig,
        config_path: Option<PathBuf>,
    ) -> Self {
        let router = Arc::new(RwLock::new(ShardRouter::from_cluster(
            initial_cluster,
            shard_count,
        )));
        let static_nodes: Vec<NodeState> = initial_cluster.nodes.clone();
        let replication_factor = initial_cluster.replication_factor;
        Self {
            pool,
            cfg,
            config_path,
            shard_count: Arc::new(RwLock::new(shard_count)),
            virtual_nodes: initial_cluster.virtual_nodes_per_shard,
            replication_factor,
            static_nodes: Arc::new(RwLock::new(static_nodes)),
            dynamic: Arc::new(RwLock::new(HashMap::new())),
            router,
            last_config_mtime: Arc::new(RwLock::new(None)),
            status: Arc::new(RwLock::new(TopologyStatus {
                shard_count,
                replication_factor,
                nodes: vec![],
                last_health_unix_ms: 0,
                last_config_reload_unix_ms: None,
            })),
            reload_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn router(&self) -> Arc<RwLock<ShardRouter>> {
        self.router.clone()
    }

    pub fn shard_count(&self) -> u32 {
        *self.shard_count.read()
    }

    pub fn status(&self) -> TopologyStatus {
        self.status.read().clone()
    }

    /// Replace the static node list (from hot-reloaded router TOML).
    pub fn apply_static(&self, nodes: Vec<NodeState>, shard_count: u32) {
        *self.static_nodes.write() = nodes;
        if shard_count > 0 {
            *self.shard_count.write() = shard_count;
        }
        tracing::info!(shard_count, nodes = self.static_nodes.read().len(), "topology: static config applied");
    }

    /// Data nodes call this on startup and periodically as a heartbeat.
    pub fn register_node(
        &self,
        node_id: String,
        grpc: String,
        shard_ids: Vec<u32>,
        shard_count_hint: u32,
    ) {
        let state = NodeState {
            id: node_id.clone(),
            advertise_addr: grpc,
            role: NodeRole::Data,
            shard_ids,
            healthy: true,
        };
        self.dynamic.write().insert(
            node_id.clone(),
            LiveRegistration {
                state,
                last_seen: Instant::now(),
                shard_count_hint,
            },
        );
        tracing::info!(node_id = %node_id, "topology: node registered");
    }

    /// Signal handler sets this; the background loop picks it up.
    pub fn request_config_reload(&self) {
        self.reload_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn spawn_background(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let health_every = Duration::from_secs(this.cfg.health_interval_secs.max(3));
        tokio::spawn(async move {
            tracing::info!(
                interval_secs = this.cfg.health_interval_secs,
                "topology health loop started"
            );
            let mut health_tick = tokio::time::interval(health_every);
            health_tick.tick().await;
            loop {
                health_tick.tick().await;
                this.run_health_pass().await;
            }
        });
    }

    #[allow(dead_code)]
    #[cfg(unix)]
    pub fn spawn_sighup_handler(self: &Arc<Self>) {
        use tokio::signal::unix::{signal, SignalKind};
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut stream = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "SIGHUP handler unavailable");
                    return;
                }
            };
            loop {
                if stream.recv().await.is_some() {
                    tracing::info!("SIGHUP: reloading router config");
                    this.request_config_reload();
                }
            }
        });
    }

    #[cfg(not(unix))]
    pub fn spawn_sighup_handler(self: &Arc<Self>) {
        let _ = self;
    }

    /// Reload router TOML if the file changed. Parsing is delegated to the
    /// caller (`vectordb-server`) via [`Self::apply_static`]; this only
    /// tracks mtime and returns whether a reload is due.
    pub fn config_file_changed(&self, path: &Path) -> bool {
        let Ok(meta) = std::fs::metadata(path) else {
            return false;
        };
        let Ok(mtime) = meta.modified() else {
            return false;
        };
        let prev = *self.last_config_mtime.read();
        if prev == Some(mtime) {
            return false;
        }
        *self.last_config_mtime.write() = Some(mtime);
        true
    }

    pub fn mark_config_reloaded(&self) {
        self.status.write().last_config_reload_unix_ms = Some(unix_ms());
    }

    async fn run_health_pass(&self) {
        self.purge_stale_registrations();
        let cluster = self.merged_cluster();
        let shard_count = self.effective_shard_count(&cluster);
        *self.shard_count.write() = shard_count;

        let mut health_by_id: HashMap<String, ProbeResult> = HashMap::new();
        let mut health_by_endpoint: HashMap<String, ProbeResult> = HashMap::new();
        for node in &cluster.nodes {
            let ep = normalize_endpoint(&node.advertise_addr);
            let probe = probe_endpoint(&self.pool, &ep).await;
            health_by_id.insert(node.id.clone(), probe.clone());
            health_by_endpoint.insert(ep, probe);
        }

        let mut cluster_live = cluster.clone();
        for n in &mut cluster_live.nodes {
            if let Some(p) = health_by_id.get(&n.id) {
                n.healthy = p.reachable;
            } else {
                n.healthy = false;
            }
        }

        let mut router = ShardRouter::from_cluster(&cluster_live, shard_count);
        for shard in 0..shard_count {
            if let Some(ep) =
                pick_shard_primary(&router, shard, &health_by_endpoint)
            {
                router.set_shard_primary(shard, ep);
            }
        }
        *self.router.write() = router;

        let primaries: HashMap<u32, String> = self
            .router
            .read()
            .shard_endpoints()
            .map(|(s, ep)| (s, ep.to_string()))
            .collect();

        let nodes: Vec<NodeStatus> = cluster_live
            .nodes
            .iter()
            .map(|n| {
                let p = health_by_id.get(&n.id);
                NodeStatus {
                    id: n.id.clone(),
                    grpc: normalize_endpoint(&n.advertise_addr),
                    shard_ids: n.shard_ids.clone(),
                    healthy: n.healthy,
                    ready: p.map(|x| x.ready).unwrap_or(false),
                    is_leader: p.map(|x| x.is_leader).unwrap_or(false),
                    source: if self.dynamic.read().contains_key(&n.id) {
                        "dynamic".into()
                    } else {
                        "static".into()
                    },
                    primary_for_shards: primaries
                        .iter()
                        .filter(|(_, ep)| **ep == normalize_endpoint(&n.advertise_addr))
                        .map(|(s, _)| *s)
                        .collect(),
                }
            })
            .collect();

        {
            let mut st = self.status.write();
            st.shard_count = shard_count;
            st.replication_factor = self.replication_factor;
            st.nodes = nodes;
            st.last_health_unix_ms = unix_ms();
        }
    }

    fn purge_stale_registrations(&self) {
        let ttl = Duration::from_secs(self.cfg.registration_ttl_secs.max(30));
        let now = Instant::now();
        self.dynamic.write().retain(|id, reg| {
            let ok = now.duration_since(reg.last_seen) < ttl;
            if !ok {
                tracing::info!(node_id = %id, "topology: registration expired");
            }
            ok
        });
    }

    fn merged_cluster(&self) -> ClusterConfig {
        let mut by_id: HashMap<String, NodeState> = self
            .static_nodes
            .read()
            .iter()
            .map(|n| (n.id.clone(), n.clone()))
            .collect();
        for reg in self.dynamic.read().values() {
            by_id.insert(reg.state.id.clone(), reg.state.clone());
        }
        ClusterConfig {
            replication_factor: self.replication_factor,
            virtual_nodes_per_shard: self.virtual_nodes,
            nodes: by_id.into_values().collect(),
        }
    }

    fn effective_shard_count(&self, cluster: &ClusterConfig) -> u32 {
        let configured = *self.shard_count.read();
        if !self.cfg.auto_shard_count {
            return configured.max(1);
        }
        let mut max_shard = configured.saturating_sub(1);
        for n in &cluster.nodes {
            for &s in &n.shard_ids {
                max_shard = max_shard.max(s);
            }
            if n.shard_ids.is_empty() {
                // RF replica without explicit shard — doesn't change count.
            }
        }
        for reg in self.dynamic.read().values() {
            if reg.shard_count_hint > 0 {
                max_shard = max_shard.max(reg.shard_count_hint - 1);
            }
            for &s in &reg.state.shard_ids {
                max_shard = max_shard.max(s);
            }
        }
        max_shard.saturating_add(1).max(1)
    }
}

#[derive(Clone)]
struct ProbeResult {
    reachable: bool,
    ready: bool,
    is_leader: bool,
    leader_endpoint: String,
}

async fn probe_endpoint(pool: &ClientPool, endpoint: &str) -> ProbeResult {
    let mut client = match pool.get(endpoint).await {
        Ok(c) => c,
        Err(_) => {
            return ProbeResult {
                reachable: false,
                ready: false,
                is_leader: false,
                leader_endpoint: String::new(),
            }
        }
    };
    match client.health_detail().await {
        Ok(h) => probe_from_health(&h),
        Err(_) => ProbeResult {
            reachable: false,
            ready: false,
            is_leader: false,
            leader_endpoint: String::new(),
        },
    }
}

fn probe_from_health(h: &HealthResponse) -> ProbeResult {
    ProbeResult {
        reachable: h.status == "ok" || h.ready,
        ready: h.ready,
        is_leader: h.is_leader,
        leader_endpoint: h.leader_endpoint.clone(),
    }
}

/// Prefer Raft leader (ready), then any ready replica, then any reachable node.
fn pick_shard_primary(
    router: &ShardRouter,
    shard: u32,
    health_by_endpoint: &HashMap<String, ProbeResult>,
) -> Option<String> {
    let replicas: Vec<String> = router.replicas_for_shard(shard).to_vec();
    if replicas.is_empty() {
        return router.endpoint_for_shard(shard).map(|s| s.to_string());
    }

    let mut best_ready: Option<String> = None;
    let mut best_leader: Option<String> = None;
    let mut any_up: Option<String> = None;

    for ep in replicas {
        let ep = normalize_endpoint(&ep);
        let Some(probe) = health_by_endpoint.get(&ep) else {
            continue;
        };
        if !probe.reachable {
            continue;
        }
        any_up = Some(ep.clone());
        if probe.is_leader && probe.ready {
            if !probe.leader_endpoint.is_empty() {
                return Some(normalize_endpoint(&probe.leader_endpoint));
            }
            best_leader = Some(ep.clone());
        }
        if probe.ready {
            best_ready = Some(ep);
        }
    }

    best_leader.or(best_ready).or(any_up)
}

fn normalize_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
