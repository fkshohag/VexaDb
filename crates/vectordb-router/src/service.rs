use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::join_all;
use vectordb_cluster::{merge_top_k, ClusterConfig};
use tonic::{Request, Response, Status, Streaming};
use vectordb_proto::vectordb::v1::{
    vector_service_server::VectorService, BulkUpsertRequest, BulkUpsertResponse, CollectionSpec,
    ClusterStatusRequest, ClusterStatusResponse, ClusterNodeStatus, CompactWalRequest,
    CompactWalResponse, CreateCollectionRequest, CreateCollectionResponse,
    CreateSnapshotRequest, CreateSnapshotResponse, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteRequest, DeleteResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DescribeCollectionRequest, DescribeCollectionResponse, GetRequest, GetResponse, HealthRequest,
    HealthResponse, ImportChunk, ImportStreamResponse, ListCollectionsRequest,
    ListCollectionsResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    RebalanceCollectionReport, RebalanceRequest, RebalanceResponse, RebalanceStatusRequest,
    RebalanceStatusResponse, RegisterNodeRequest, RegisterNodeResponse,
    ReindexCollectionRequest, ReindexCollectionResponse, ScrollRequest, ScrollResponse,
    SearchRequest, SearchResponse, UpsertRequest, UpsertResponse, VectorPoint,
};

use crate::pool::ClientPool;
use crate::rebalance::RebalanceCoordinator;
use crate::topology::TopologyManager;
use crate::{RebalanceConfig, TopologyConfig};

pub struct RouterService {
    pool: ClientPool,
    topology: Arc<TopologyManager>,
    node_id: String,
    rebalance: RebalanceCoordinator,
}

impl RouterService {
    pub fn new(node_id: impl Into<String>, cluster: &ClusterConfig, shard_count: u32) -> Self {
        Self::with_rebalance(node_id, cluster, shard_count, RebalanceConfig::default())
    }

    pub fn with_rebalance(
        node_id: impl Into<String>,
        cluster: &ClusterConfig,
        shard_count: u32,
        rebalance_cfg: RebalanceConfig,
    ) -> Self {
        let (svc, _) = Self::with_topology(
            node_id,
            cluster,
            shard_count,
            rebalance_cfg,
            TopologyConfig::default(),
            None,
        );
        svc
    }

  pub fn with_topology(
        node_id: impl Into<String>,
        cluster: &ClusterConfig,
        shard_count: u32,
        rebalance_cfg: RebalanceConfig,
        topology_cfg: TopologyConfig,
        config_path: Option<PathBuf>,
    ) -> (Self, Arc<TopologyManager>) {
        let pool = ClientPool::default();
        let topology = Arc::new(TopologyManager::new(
            pool.clone(),
            cluster,
            shard_count,
            topology_cfg,
            config_path,
        ));
        let rebalance =
            RebalanceCoordinator::new(pool.clone(), topology.router(), rebalance_cfg);
        let svc = Self {
            pool,
            topology: topology.clone(),
            node_id: node_id.into(),
            rebalance,
        };
        (svc, topology)
    }

    pub fn rebalance(&self) -> RebalanceCoordinator {
        self.rebalance.clone()
    }

    pub fn topology(&self) -> Arc<TopologyManager> {
        self.topology.clone()
    }

    async fn clients_for_all_shards(
        &self,
    ) -> Result<Vec<(u32, vectordb_client::VectorDbClient)>, Status> {
        let endpoints: Vec<(u32, String)> = self
            .topology
            .router()
            .read()
            .shard_endpoints()
            .map(|(s, ep)| (s, ep.to_string()))
            .collect();
        if endpoints.is_empty() {
            return Err(Status::failed_precondition(
                "no shard endpoints configured; set cluster.nodes or wait for RegisterNode",
            ));
        }
        let mut out = Vec::new();
        for (shard, ep) in endpoints {
            let client = self
                .pool
                .get(&ep)
                .await
                .map_err(|e| Status::unavailable(format!("shard {shard} at {ep}: {e}")))?;
            out.push((shard, client));
        }
        Ok(out)
    }

    /// Connect to every shard we can reach. Missing shards are logged and
    /// skipped — used for read paths that tolerate partial availability
    /// (list/describe/search during WAL replay or rolling restarts).
    async fn clients_for_shards_best_effort(
        &self,
    ) -> Vec<(u32, vectordb_client::VectorDbClient)> {
        let endpoints: Vec<(u32, String)> = self
            .topology
            .router()
            .read()
            .shard_endpoints()
            .map(|(s, ep)| (s, ep.to_string()))
            .collect();
        let mut out = Vec::new();
        for (shard, ep) in endpoints {
            match self.pool.get(&ep).await {
                Ok(client) => out.push((shard, client)),
                Err(e) => {
                    tracing::warn!(
                        shard,
                        endpoint = %ep,
                        error = %e,
                        "shard unreachable (partial read)"
                    );
                }
            }
        }
        out
    }

    async fn client_for_point(
        &self,
        point_id: &str,
    ) -> Result<vectordb_client::VectorDbClient, Status> {
        let ep = self
            .topology
            .router()
            .read()
            .endpoint_for_point(point_id)
            .map(|s| s.to_string())
            .ok_or_else(|| Status::not_found("no endpoint for point shard"))?;
        self.pool
            .get(&ep)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))
    }
}

#[tonic::async_trait]
impl VectorService for RouterService {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            node_id: self.node_id.clone(),
            shard_count: self.topology.shard_count(),
            is_leader: true,
            raft_role: String::new(),
            leader_endpoint: String::new(),
            ready: true,
        }))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let spec = request
            .into_inner()
            .spec
            .ok_or_else(|| Status::invalid_argument("spec required"))?;
        for (_, mut client) in self.clients_for_all_shards().await? {
            client
                .create_collection(spec.clone())
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(CreateCollectionResponse {}))
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        let name = request.into_inner().name;
        for (_, mut client) in self.clients_for_all_shards().await? {
            client
                .delete_collection(&name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(DeleteCollectionResponse {}))
    }

    async fn list_collections(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let clients = self.clients_for_shards_best_effort().await;
        if clients.is_empty() {
            return Err(Status::unavailable(
                "no shard endpoints reachable; wait for data nodes to finish WAL replay",
            ));
        }

        let mut names = std::collections::BTreeSet::new();
        let mut ok_shards = 0usize;
        let mut failures = 0usize;
        let mut last_err: Option<String> = None;
        for (shard, mut client) in clients {
            match client.list_collections().await {
                Ok(shard_names) => {
                    ok_shards += 1;
                    names.extend(shard_names);
                }
                Err(e) => {
                    failures += 1;
                    last_err = Some(e.to_string());
                    tracing::warn!(
                        shard,
                        error = %e,
                        "list_collections failed on shard (partial read)"
                    );
                }
            }
        }
        if ok_shards == 0 {
            return Err(Status::internal(format!(
                "all reachable shards failed list_collections; last error: {}",
                last_err.unwrap_or_else(|| "unknown".into())
            )));
        }
        if failures > 0 {
            tracing::warn!(
                ok = ok_shards,
                failed = failures,
                collections = names.len(),
                "list_collections returned partial results"
            );
        }
        Ok(Response::new(ListCollectionsResponse {
            names: names.into_iter().collect(),
        }))
    }

    async fn describe_collection(
        &self,
        request: Request<DescribeCollectionRequest>,
    ) -> Result<Response<DescribeCollectionResponse>, Status> {
        let name = request.into_inner().name;
        let clients = self.clients_for_shards_best_effort().await;
        if clients.is_empty() {
            return Err(Status::unavailable(
                "no shard endpoints reachable; wait for data nodes to finish WAL replay",
            ));
        }

        let mut spec: Option<CollectionSpec> = None;
        let mut total = 0u64;
        let mut ok_shards = 0usize;
        let mut failures = 0usize;
        let mut last_err: Option<String> = None;
        for (shard, mut client) in clients {
            match client.describe_collection(&name).await {
                Ok((s, count)) => {
                    ok_shards += 1;
                    if spec.is_none() {
                        spec = Some(s);
                    }
                    total += count;
                }
                Err(e) => {
                    failures += 1;
                    last_err = Some(e.to_string());
                    tracing::warn!(
                        shard,
                        collection = %name,
                        error = %e,
                        "describe_collection failed on shard (partial read)"
                    );
                }
            }
        }
        if ok_shards == 0 {
            return Err(Status::internal(format!(
                "all reachable shards failed describe_collection({name}); last error: {}",
                last_err.unwrap_or_else(|| "unknown".into())
            )));
        }
        if failures > 0 {
            tracing::warn!(
                collection = %name,
                ok = ok_shards,
                failed = failures,
                "describe_collection returned partial results"
            );
        }
        Ok(Response::new(DescribeCollectionResponse {
            spec,
            vector_count: total,
        }))
    }

    async fn upsert(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<UpsertResponse>, Status> {
        let req = request.into_inner();
        let mut by_endpoint: HashMap<String, Vec<VectorPoint>> = HashMap::new();
        for point in req.points {
            let ep = self
                .topology
                .router()
                .read()
                .endpoint_for_point(&point.id)
                .ok_or_else(|| Status::not_found(format!("no shard for {}", point.id)))?
                .to_string();
            by_endpoint.entry(ep).or_default().push(point);
        }
        let mut upserted = 0u64;
        for (ep, points) in by_endpoint {
            let mut client = self
                .pool
                .get(&ep)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            upserted += client
                .upsert(&req.collection, points)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(UpsertResponse { upserted }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let top_k = req.top_k.max(1) as usize;
        let collection = req.collection.clone();
        let query = req.query.clone();
        let filter_ids = req.filter_ids.clone();

        let filter_json = req.filter_json.clone();
        let sparse_query = req.sparse_query.clone();
        let text_query = req.text_query.clone();
        let search_mode = req.search_mode.clone();
        let hybrid_alpha = req.hybrid_alpha;
        let futures: Vec<_> = self
            .clients_for_all_shards()
            .await?
            .into_iter()
            .map(|(_, mut client)| {
                let collection = collection.clone();
                let query = query.clone();
                let filter_ids = filter_ids.clone();
                let filter_json = filter_json.clone();
                let sparse_query = sparse_query.clone();
                let text_query = text_query.clone();
                let search_mode = search_mode.clone();
                async move {
                    let per_shard_k = (top_k * 2).max(top_k) as u32;
                    client
                        .search_hybrid(
                            &collection,
                            query,
                            per_shard_k,
                            filter_ids,
                            filter_json,
                            sparse_query,
                            if text_query.is_empty() {
                                None
                            } else {
                                Some(text_query)
                            },
                            &search_mode,
                            hybrid_alpha,
                        )
                        .await
                }
            })
            .collect();

        // Fan out and tolerate per-shard failures: return whatever shards
        // came back, log the rest. A single dead shard no longer black-holes
        // the entire query.
        let results = join_all(futures).await;
        let total = results.len();
        let mut all_hits = Vec::new();
        let mut failures = 0usize;
        let mut last_err: Option<String> = None;
        for res in results {
            match res {
                Ok(hits) => {
                    for h in hits {
                        all_hits.push((h.id, h.score));
                    }
                }
                Err(e) => {
                    failures += 1;
                    last_err = Some(e.to_string());
                    tracing::warn!(error = %e, "shard search failed (returning partial results)");
                }
            }
        }
        if failures == total {
            // All shards failed → there's no useful answer to return.
            return Err(Status::internal(format!(
                "all {total} shards failed; last error: {}",
                last_err.unwrap_or_else(|| "unknown".into())
            )));
        }
        if failures > 0 {
            tracing::warn!(
                ok = total - failures,
                failed = failures,
                "search returned partial results"
            );
        }
        let merged = merge_top_k(all_hits, top_k);
        Ok(Response::new(SearchResponse {
            hits: merged
                .into_iter()
                .map(|(id, score)| vectordb_proto::vectordb::v1::ScoredPoint { id, score })
                .collect(),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let mut by_endpoint: HashMap<String, Vec<String>> = HashMap::new();
        for id in req.ids {
            let ep = self
                .topology
                .router()
                .read()
                .endpoint_for_point(&id)
                .ok_or_else(|| Status::not_found(format!("no shard for {id}")))?
                .to_string();
            by_endpoint.entry(ep).or_default().push(id);
        }
        let mut deleted = 0u64;
        for (ep, ids) in by_endpoint {
            let mut client = self
                .pool
                .get(&ep)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            deleted += client
                .delete(&req.collection, ids)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(DeleteResponse { deleted }))
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let mut client = self.client_for_point(&req.id).await?;
        let point = client
            .get(&req.collection, &req.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetResponse {
            found: point.is_some(),
            point,
        }))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        // Take snapshot on first shard; production: fan out and aggregate.
        let mut clients = self.clients_for_all_shards().await?;
        let (_, mut first) = clients.remove(0);
        let snap = first
            .create_snapshot()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(snap),
        }))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let mut all = Vec::new();
        for (_, mut client) in self.clients_for_all_shards().await? {
            let mut s = client
                .list_snapshots()
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            all.append(&mut s);
        }
        Ok(Response::new(ListSnapshotsResponse { snapshots: all }))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let id = request.into_inner().id;
        for (_, mut client) in self.clients_for_all_shards().await? {
            let _ = client.delete_snapshot(&id).await;
        }
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn bulk_upsert(
        &self,
        request: Request<BulkUpsertRequest>,
    ) -> Result<Response<BulkUpsertResponse>, Status> {
        let req = request.into_inner();
        let mut by_endpoint: HashMap<String, Vec<VectorPoint>> = HashMap::new();
        for point in req.points {
            let ep = self
                .topology
                .router()
                .read()
                .endpoint_for_point(&point.id)
                .ok_or_else(|| Status::not_found(format!("no shard for {}", point.id)))?
                .to_string();
            by_endpoint.entry(ep).or_default().push(point);
        }
        let chunk_size = req.chunk_size;
        let mut upserted = 0u64;
        for (ep, points) in by_endpoint {
            let mut client = self
                .pool
                .get(&ep)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            upserted += client
                .bulk_upsert(&req.collection, points, chunk_size)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(BulkUpsertResponse { upserted }))
    }

    async fn import_stream(
        &self,
        request: Request<Streaming<ImportChunk>>,
    ) -> Result<Response<ImportStreamResponse>, Status> {
        let mut stream = request.into_inner();
        let mut collection = String::new();
        let mut by_endpoint: HashMap<String, Vec<VectorPoint>> = HashMap::new();
        let mut total = 0u64;

        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            if !chunk.collection.is_empty() {
                collection = chunk.collection.clone();
            }
            for point in chunk.points {
                let ep = self
                    .topology
                    .router()
                    .read()
                    .endpoint_for_point(&point.id)
                    .ok_or_else(|| Status::not_found(format!("no shard for {}", point.id)))?
                    .to_string();
                by_endpoint.entry(ep).or_default().push(point);
            }
            if chunk.finalize {
                for (ep, points) in by_endpoint.drain() {
                    let mut client = self
                        .pool
                        .get(&ep)
                        .await
                        .map_err(|e| Status::unavailable(e.to_string()))?;
                    total += client
                        .bulk_upsert(&collection, points, 500)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
            }
        }
        for (ep, points) in by_endpoint {
            let mut client = self
                .pool
                .get(&ep)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            total += client
                .bulk_upsert(&collection, points, 500)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(ImportStreamResponse { upserted: total }))
    }

    async fn compact_wal(
        &self,
        request: Request<CompactWalRequest>,
    ) -> Result<Response<CompactWalResponse>, Status> {
        let snapshot_first = request.into_inner().snapshot_first;
        let mut clients = self.clients_for_all_shards().await?;
        if clients.is_empty() {
            return Err(Status::failed_precondition("no shards"));
        }
        let (_, mut first) = clients.remove(0);
        let resp = first
            .compact_wal(snapshot_first)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        for (_, mut client) in clients {
            let _ = client.compact_wal(snapshot_first).await;
        }
        Ok(Response::new(CompactWalResponse {
            entries_before: resp.entries_before,
            entries_after: resp.entries_after,
            snapshot: resp.snapshot,
        }))
    }

    async fn reindex_collection(
        &self,
        request: Request<ReindexCollectionRequest>,
    ) -> Result<Response<ReindexCollectionResponse>, Status> {
        let name = request.into_inner().collection;
        let mut total = 0u64;
        for (_, mut client) in self.clients_for_all_shards().await? {
            let resp = client
                .reindex_collection(&name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            total += resp.vectors_reindexed;
        }
        Ok(Response::new(ReindexCollectionResponse {
            vectors_reindexed: total,
        }))
    }

    async fn scroll(
        &self,
        _request: Request<ScrollRequest>,
    ) -> Result<Response<ScrollResponse>, Status> {
        // Scroll is a *per-shard* operation — call shards directly via the
        // rebalance tooling, not through the router. Returning an error
        // keeps the router from accidentally aggregating across shards
        // and breaking cursor semantics.
        Err(Status::failed_precondition(
            "Scroll is per-shard; rebalance tools should connect to shard nodes directly",
        ))
    }

    async fn rebalance(
        &self,
        request: Request<RebalanceRequest>,
    ) -> Result<Response<RebalanceResponse>, Status> {
        let dry_run = request.into_inner().dry_run;
        let report = self
            .rebalance
            .run_once(dry_run)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RebalanceResponse {
            moved: report.moved,
            kept: report.kept,
            failed: report.failed,
            duration_ms: report.duration_ms,
            per_collection: report
                .per_collection
                .into_iter()
                .map(|c| RebalanceCollectionReport {
                    collection: c.collection,
                    moved: c.moved,
                    kept: c.kept,
                    failed: c.failed,
                })
                .collect(),
        }))
    }

    async fn rebalance_status(
        &self,
        _request: Request<RebalanceStatusRequest>,
    ) -> Result<Response<RebalanceStatusResponse>, Status> {
        let s = self.rebalance.status();
        let cfg = self.rebalance.config();
        Ok(Response::new(RebalanceStatusResponse {
            running: s.running,
            enabled: cfg.enabled,
            interval_secs: cfg.interval_secs,
            last_started_unix_ms: s.last_started_unix_ms.unwrap_or(0),
            last_finished_unix_ms: s.last_finished_unix_ms.unwrap_or(0),
            last_duration_ms: s.last_duration_ms.unwrap_or(0),
            last_moved: s.last_moved,
            last_kept: s.last_kept,
            last_failed: s.last_failed,
            last_error: s.last_error.unwrap_or_default(),
            total_moves: s.total_moves,
            total_sweeps: s.total_sweeps,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        if req.node_id.is_empty() || req.grpc.is_empty() {
            return Err(Status::invalid_argument("node_id and grpc required"));
        }
        self.topology.register_node(
            req.node_id,
            req.grpc,
            req.shard_ids,
            req.shard_count,
        );
        Ok(Response::new(RegisterNodeResponse {
            accepted: true,
            shard_count: self.topology.shard_count(),
        }))
    }

    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let st = self.topology.status();
        Ok(Response::new(ClusterStatusResponse {
            shard_count: st.shard_count,
            replication_factor: st.replication_factor as u64,
            last_health_unix_ms: st.last_health_unix_ms,
            last_config_reload_unix_ms: st.last_config_reload_unix_ms.unwrap_or(0),
            nodes: st
                .nodes
                .into_iter()
                .map(|n| ClusterNodeStatus {
                    id: n.id,
                    grpc: n.grpc,
                    shard_ids: n.shard_ids,
                    healthy: n.healthy,
                    ready: n.ready,
                    is_leader: n.is_leader,
                    source: n.source,
                    primary_for_shards: n.primary_for_shards,
                })
                .collect(),
        }))
    }
}
