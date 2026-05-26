use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::join_all;
use vectordb_cluster::{merge_top_k, ClusterConfig};
use tonic::{Request, Response, Status, Streaming};
use vectordb_proto::vectordb::v1::{
    vector_service_server::VectorService, AddPayloadIndexRequest, AddPayloadIndexResponse,
    AliasEntry, AlterDatabaseRequest, AlterDatabaseResponse, ApplyRbacRequest, ApplyRbacResponse,
    BulkUpsertRequest, BulkUpsertResponse, ClusterNodeStatus, ClusterStatusRequest,
    ClusterStatusResponse, CollectionSpec, CompactCollectionRequest, CompactCollectionResponse,
    CompactWalRequest, CompactWalResponse, CreateCollectionRequest, CreateCollectionResponse,
    CreateDatabaseRequest, CreateDatabaseResponse, CreatePartitionRequest, CreatePartitionResponse,
    CreateSnapshotRequest, CreateSnapshotResponse, DeleteCollectionRequest,
    DeleteCollectionResponse, DeleteRequest, DeleteResponse, DeleteSnapshotRequest,
    DeleteSnapshotResponse, DescribeAliasRequest, DescribeAliasResponse, DescribeCollectionRequest,
    DescribeCollectionResponse, DescribeDatabaseRequest, DescribeDatabaseResponse,
    DropDatabaseRequest, DropDatabaseResponse, DropPartitionRequest, DropPartitionResponse,
    DropPayloadIndexRequest, DropPayloadIndexResponse, FlushCollectionRequest,
    FlushCollectionResponse, GetCompactionStateRequest, GetCompactionStateResponse,
    GetPartitionStatsRequest, GetPartitionStatsResponse, GetRbacSnapshotRequest,
    GetRbacSnapshotResponse, GetRequest, GetResponse, HasPartitionRequest, HasPartitionResponse,
    HealthRequest, HealthResponse, ImportChunk, ImportStreamResponse, ListAliasesRequest,
    ListAliasesResponse, ListCollectionsRequest, ListCollectionsResponse, ListDatabasesRequest,
    ListDatabasesResponse, ListPartitionsRequest, ListPartitionsResponse,
    ListPersistentSegmentsRequest, ListPersistentSegmentsResponse, ListSnapshotsRequest,
    ListSnapshotsResponse, MutateCollectionMetaRequest, MutateCollectionMetaResponse, QueryRequest,
    QueryResponse, RebalanceCollectionReport, RebalanceRequest, RebalanceResponse,
    RebalanceStatusRequest, RebalanceStatusResponse, RegisterNodeRequest, RegisterNodeResponse,
    ReindexCollectionRequest, ReindexCollectionResponse, ScrollRequest, ScrollResponse,
    SearchRequest, SearchResponse, StatsRequest, StatsResponse, UpsertRequest, UpsertResponse,
    VectorPoint,
};

use crate::pool::ClientPool;
use crate::rebalance::RebalanceCoordinator;
use crate::topology::TopologyManager;
use vectordb_rbac::{require_collection, Privilege, RbacCache};

use crate::{RebalanceConfig, TopologyConfig};

pub struct RouterService {
    pool: ClientPool,
    topology: Arc<TopologyManager>,
    node_id: String,
    rebalance: RebalanceCoordinator,
    rbac: RbacCache,
}

impl RouterService {
    pub fn set_rbac_cache(&mut self, cache: RbacCache) {
        self.rbac = cache;
    }

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
            rbac: RbacCache::new(vec![]),
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

    /// Fan a database management `MetaOp` payload to every shard. Mirrors the
    /// fan-out logic used by `mutate_collection_meta` so the per-shard meta
    /// DB stays consistent.
    async fn fanout_database_op(
        &self,
        op_json: &[u8],
        rpc: &'static str,
    ) -> Result<(), Status> {
        let bytes = op_json.to_vec();
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_all_shards().await? {
            let bytes = bytes.clone();
            let res = match rpc {
                "create_database" => client.create_database(bytes).await,
                "drop_database" => client.drop_database(bytes).await,
                "alter_database" => client.alter_database(bytes).await,
                _ => unreachable!(),
            };
            match res {
                Ok(()) => ok += 1,
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err
                .unwrap_or_else(|| Status::unavailable(format!("no shard accepted {rpc}"))));
        }
        Ok(())
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
        require_collection(&self.rbac, &request, &request.get_ref().name, Privilege::DropCollection)?;
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
        require_collection(&self.rbac, &request, &request.get_ref().name, Privilege::DescribeCollection)?;
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
        let mut alias_set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for (shard, mut client) in clients {
            match client.describe_collection_full(&name).await {
                Ok((s, count, aliases)) => {
                    ok_shards += 1;
                    if spec.is_none() {
                        spec = Some(s);
                    }
                    total += count;
                    for a in aliases {
                        alias_set.insert(a);
                    }
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
            aliases: alias_set.into_iter().collect(),
        }))
    }

    async fn upsert(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<UpsertResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Upsert)?;
        let req = request.into_inner();
        let collection = req.collection;

        // Bucket points by destination endpoint via consistent-hash ring.
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
        if by_endpoint.is_empty() {
            return Ok(Response::new(UpsertResponse { upserted: 0 }));
        }

        // Fan out concurrently so a slow shard doesn't serialize the whole
        // request, AND so a failure on shard N still attempts shards M, P
        // (matches the search path's resilience model). Errors per shard are
        // logged, then aggregated into a single status if any shard failed —
        // the caller can retry the whole call (upsert is idempotent on point
        // id) without worrying about which shards were already written.
        let fan_out: Vec<_> = by_endpoint
            .into_iter()
            .map(|(ep, points)| {
                let collection = collection.clone();
                let pool = self.pool.clone();
                let count = points.len();
                async move {
                    let mut client = match pool.get(&ep).await {
                        Ok(c) => c,
                        Err(e) => return (ep, count, Err(format!("connect: {e}"))),
                    };
                    // Retry on transient leader-election windows. A shard
                    // that just lost its leader will return either
                    // "not leader; current leader=None" (no redirect target)
                    // or Unavailable while a new election runs. Elections
                    // typically settle within ~1s on a healthy cluster, so
                    // 4 attempts with 100/300/700ms backoff (≈1.1s) is
                    // enough to mask the transient and still bail fast on
                    // a real outage.
                    let mut last: Option<String> = None;
                    for (attempt, delay_ms) in [(1u32, 0u64), (2, 100), (3, 300), (4, 700)] {
                        if delay_ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        }
                        match client.upsert(&collection, points.clone()).await {
                            Ok(n) => return (ep, count, Ok(n)),
                            Err(e) => {
                                let msg = e.to_string();
                                let transient = msg.contains("not leader")
                                    || msg.contains("FailedPrecondition")
                                    || msg.contains("Unavailable")
                                    || msg.contains("failed to reach quorum");
                                last = Some(msg);
                                if !transient {
                                    break;
                                }
                                tracing::debug!(
                                    endpoint = %ep,
                                    collection = %collection,
                                    attempt,
                                    error = %last.as_deref().unwrap_or(""),
                                    "upsert transient error, will retry"
                                );
                            }
                        }
                    }
                    (ep, count, Err(last.unwrap_or_else(|| "unknown".into())))
                }
            })
            .collect();

        let results = join_all(fan_out).await;
        let mut upserted = 0u64;
        let mut failures: Vec<String> = Vec::new();
        let mut failed_shards = 0usize;
        let total_shards = results.len();
        for (ep, count, res) in results {
            match res {
                Ok(n) => upserted += n,
                Err(msg) => {
                    failed_shards += 1;
                    tracing::warn!(
                        endpoint = %ep,
                        collection = %collection,
                        points = count,
                        error = %msg,
                        "upsert failed on shard"
                    );
                    failures.push(format!("{ep}: {msg}"));
                }
            }
        }

        if failed_shards == 0 {
            return Ok(Response::new(UpsertResponse { upserted }));
        }
        // Partial or total failure. Surface every failing shard's error so
        // the caller doesn't have to grep router logs to figure out who
        // rejected the write.
        let summary = format!(
            "upsert failed on {failed_shards}/{total_shards} shards (partial upserted={upserted}); errors: {}",
            failures.join("; ")
        );
        tracing::error!(
            collection = %collection,
            failed_shards,
            total_shards,
            partial_upserted = upserted,
            "upsert returning failure"
        );
        Err(Status::internal(summary))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Search)?;
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
        let output_fields = req.output_fields.clone();
        let with_payload = req.with_payload;
        let with_vector = req.with_vector;
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
                let output_fields = output_fields.clone();
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
                            output_fields,
                            with_payload,
                            with_vector,
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
                .map(|(id, score)| vectordb_proto::vectordb::v1::ScoredPoint {
                    id,
                    score,
                    payload: vec![],
                    vector: vec![],
                })
                .collect(),
        }))
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Query)?;
        let req = request.into_inner();
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let offset = req.offset as usize;
        let collection = req.collection.clone();
        let filter_json = req.filter_json.clone();
        let ids = req.ids.clone();
        let output_fields = req.output_fields.clone();
        let with_payload = req.with_payload;
        let with_vector = req.with_vector;

        let futures: Vec<_> = self
            .clients_for_all_shards()
            .await?
            .into_iter()
            .map(|(_, mut client)| {
                let collection = collection.clone();
                let filter_json = filter_json.clone();
                let ids = ids.clone();
                let output_fields = output_fields.clone();
                async move {
                    client
                        .query(
                            &collection,
                            filter_json,
                            ids,
                            limit.saturating_add(offset) as u32,
                            0,
                            output_fields,
                            with_payload,
                            with_vector,
                        )
                        .await
                }
            })
            .collect();

        let results = join_all(futures).await;
        let mut all_points = Vec::new();
        for res in results {
            if let Ok(resp) = res {
                all_points.extend(resp.points);
            }
        }
        all_points.sort_by(|a, b| a.id.cmp(&b.id));
        all_points.dedup_by(|a, b| a.id == b.id);
        let page: Vec<VectorPoint> = all_points.into_iter().skip(offset).take(limit).collect();
        Ok(Response::new(QueryResponse {
            points: page,
            next_cursor: String::new(),
        }))
    }

    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::CollectionStats)?;
        let name = request.into_inner().collection;
        let clients = self.clients_for_shards_best_effort().await;
        let mut total = 0u64;
        let mut first: Option<StatsResponse> = None;
        for (_, mut client) in clients {
            if let Ok(s) = client.stats(&name).await {
                total += s.vector_count;
                if first.is_none() {
                    first = Some(s);
                }
            }
        }
        let mut out = first.ok_or_else(|| Status::not_found(format!("collection {name}")))?;
        out.vector_count = total;
        Ok(Response::new(out))
    }

    async fn apply_rbac(
        &self,
        request: Request<ApplyRbacRequest>,
    ) -> Result<Response<ApplyRbacResponse>, Status> {
        let req = request.into_inner();
        for (_, mut client) in self.clients_for_all_shards().await? {
            client
                .apply_rbac(req.op_json.clone())
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(ApplyRbacResponse {}))
    }

    async fn get_rbac_snapshot(
        &self,
        _request: Request<GetRbacSnapshotRequest>,
    ) -> Result<Response<GetRbacSnapshotResponse>, Status> {
        let clients = self.clients_for_shards_best_effort().await;
        for (_, mut client) in clients {
            if let Ok(snap) = client.get_rbac_snapshot().await {
                return Ok(Response::new(GetRbacSnapshotResponse { snapshot_json: snap }));
            }
        }
        Err(Status::unavailable("no shard returned an RBAC snapshot"))
    }

    async fn mutate_collection_meta(
        &self,
        request: Request<MutateCollectionMetaRequest>,
    ) -> Result<Response<MutateCollectionMetaResponse>, Status> {
        // Authorize at the router before fan-out so we fail fast and don't
        // partially apply on shards. Per-handler check on each data node
        // is still in place as defense-in-depth.
        let op: vectordb_storage::MetaOp = serde_json::from_slice(&request.get_ref().op_json)
            .map_err(|e| Status::invalid_argument(format!("invalid MetaOp JSON: {e}")))?;
        let (target, priv_kind) = match &op {
            vectordb_storage::MetaOp::RenameCollection { old, database, .. } => {
                (format!("{database}/{old}"), Privilege::AlterCollection)
            }
            vectordb_storage::MetaOp::CreateAlias {
                collection, database, ..
            }
            | vectordb_storage::MetaOp::AlterAlias {
                collection, database, ..
            } => (format!("{database}/{collection}"), Privilege::AlterAlias),
            vectordb_storage::MetaOp::DropAlias { alias, database } => {
                (format!("{database}/{alias}"), Privilege::AlterAlias)
            }
            vectordb_storage::MetaOp::AlterCollectionProperties { name, database, .. } => {
                (format!("{database}/{name}"), Privilege::AlterCollection)
            }
            vectordb_storage::MetaOp::CreateDatabase { .. }
            | vectordb_storage::MetaOp::DropDatabase { .. }
            | vectordb_storage::MetaOp::AlterDatabaseProperties { .. } => {
                return Err(Status::invalid_argument(
                    "database management ops must use the CreateDatabase / DropDatabase / AlterDatabase RPCs",
                ));
            }
            vectordb_storage::MetaOp::AddPayloadIndex { .. }
            | vectordb_storage::MetaOp::DropPayloadIndex { .. } => {
                return Err(Status::invalid_argument(
                    "payload-index ops must use the AddPayloadIndex / DropPayloadIndex RPCs",
                ));
            }
            vectordb_storage::MetaOp::CreatePartition { .. }
            | vectordb_storage::MetaOp::DropPartition { .. } => {
                return Err(Status::invalid_argument(
                    "partition ops must use the CreatePartition / DropPartition RPCs",
                ));
            }
        };
        require_collection(&self.rbac, &request, &target, priv_kind)?;
        let req = request.into_inner();
        // Fan-out to all shards: meta is Raft-replicated but each shard's
        // engine has its own meta-DB (collection list lives per shard).
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_all_shards().await? {
            match client.mutate_collection_meta(req.op_json.clone()).await {
                Ok(()) => ok += 1,
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err.unwrap_or_else(|| {
                Status::unavailable("no shard accepted mutate_collection_meta")
            }));
        }
        Ok(Response::new(MutateCollectionMetaResponse {}))
    }

    async fn list_aliases(
        &self,
        request: Request<ListAliasesRequest>,
    ) -> Result<Response<ListAliasesResponse>, Status> {
        let collection = request.into_inner().collection;
        let clients = self.clients_for_shards_best_effort().await;
        let mut merged: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for (_, mut client) in clients {
            if let Ok(rows) = client.list_aliases(&collection).await {
                for (alias, coll) in rows {
                    merged.entry(alias).or_insert(coll);
                }
            }
        }
        Ok(Response::new(ListAliasesResponse {
            aliases: merged
                .into_iter()
                .map(|(alias, collection)| AliasEntry { alias, collection })
                .collect(),
        }))
    }

    async fn describe_alias(
        &self,
        request: Request<DescribeAliasRequest>,
    ) -> Result<Response<DescribeAliasResponse>, Status> {
        let alias = request.into_inner().alias;
        let clients = self.clients_for_shards_best_effort().await;
        let mut last_err: Option<String> = None;
        for (_, mut client) in clients {
            match client.describe_alias(&alias).await {
                Ok(coll) => return Ok(Response::new(DescribeAliasResponse { collection: coll })),
                Err(e) => last_err = Some(e.to_string()),
            }
        }
        Err(Status::not_found(format!(
            "alias not found: {alias} ({})",
            last_err.unwrap_or_else(|| "no shards reachable".into())
        )))
    }

    // ---- Database management (Milvus parity) -----------------------------
    //
    // Databases are Raft-replicated metadata so a single shard's response is
    // authoritative for reads. Mutations fan out to every shard for the same
    // reason `mutate_collection_meta` does: each shard maintains its own
    // local meta-DB.

    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        self.fanout_database_op(&request.get_ref().op_json, "create_database")
            .await?;
        Ok(Response::new(CreateDatabaseResponse {}))
    }

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        self.fanout_database_op(&request.get_ref().op_json, "drop_database")
            .await?;
        Ok(Response::new(DropDatabaseResponse {}))
    }

    async fn alter_database(
        &self,
        request: Request<AlterDatabaseRequest>,
    ) -> Result<Response<AlterDatabaseResponse>, Status> {
        self.fanout_database_op(&request.get_ref().op_json, "alter_database")
            .await?;
        Ok(Response::new(AlterDatabaseResponse {}))
    }

    async fn list_databases(
        &self,
        _request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let clients = self.clients_for_shards_best_effort().await;
        let mut merged: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for (_, mut client) in clients {
            if let Ok(names) = client.list_databases().await {
                merged.extend(names.into_iter());
            }
        }
        Ok(Response::new(ListDatabasesResponse {
            names: merged.into_iter().collect(),
        }))
    }

    async fn describe_database(
        &self,
        request: Request<DescribeDatabaseRequest>,
    ) -> Result<Response<DescribeDatabaseResponse>, Status> {
        let name = request.into_inner().name;
        let clients = self.clients_for_shards_best_effort().await;
        let mut last_err: Option<String> = None;
        for (_, mut client) in clients {
            match client.describe_database(&name).await {
                Ok(info) => {
                    return Ok(Response::new(DescribeDatabaseResponse {
                        info: Some(info),
                    }))
                }
                Err(e) => last_err = Some(e.to_string()),
            }
        }
        Err(Status::not_found(format!(
            "database not found: {name} ({})",
            last_err.unwrap_or_else(|| "no shards reachable".into())
        )))
    }

    // ---- Management RPCs (Milvus parity) ----------------------------------

    async fn add_payload_index(
        &self,
        request: Request<AddPayloadIndexRequest>,
    ) -> Result<Response<AddPayloadIndexResponse>, Status> {
        let op_json = request.into_inner().op_json;
        let bytes = op_json.clone();
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_all_shards().await? {
            match client.add_payload_index(bytes.clone()).await {
                Ok(()) => ok += 1,
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err
                .unwrap_or_else(|| Status::unavailable("no shard accepted add_payload_index")));
        }
        Ok(Response::new(AddPayloadIndexResponse {}))
    }

    async fn drop_payload_index(
        &self,
        request: Request<DropPayloadIndexRequest>,
    ) -> Result<Response<DropPayloadIndexResponse>, Status> {
        let op_json = request.into_inner().op_json;
        let bytes = op_json.clone();
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_all_shards().await? {
            match client.drop_payload_index(bytes.clone()).await {
                Ok(()) => ok += 1,
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err
                .unwrap_or_else(|| Status::unavailable("no shard accepted drop_payload_index")));
        }
        Ok(Response::new(DropPayloadIndexResponse {}))
    }

    async fn flush_collection(
        &self,
        request: Request<FlushCollectionRequest>,
    ) -> Result<Response<FlushCollectionResponse>, Status> {
        let collection = request.into_inner().collection;
        let mut merged = FlushCollectionResponse {
            collection: collection.clone(),
            flush_ts_ms: 0,
            segment_ids: vec![],
            flushed_segment_ids: vec![],
            wal_entries: 0,
        };
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_all_shards().await? {
            match client.flush_collection(&collection).await {
                Ok(resp) => {
                    ok += 1;
                    if resp.flush_ts_ms > merged.flush_ts_ms {
                        merged.flush_ts_ms = resp.flush_ts_ms;
                    }
                    merged.segment_ids.extend(resp.segment_ids);
                    merged.flushed_segment_ids.extend(resp.flushed_segment_ids);
                    merged.wal_entries += resp.wal_entries;
                }
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err
                .unwrap_or_else(|| Status::unavailable("no shard accepted flush_collection")));
        }
        Ok(Response::new(merged))
    }

    async fn compact_collection(
        &self,
        request: Request<CompactCollectionRequest>,
    ) -> Result<Response<CompactCollectionResponse>, Status> {
        // VexaDb's compaction is per-shard. To honour Milvus's
        // single-ID contract, route to the first reachable shard and
        // return its compaction ID. Clients should call `get_compaction_state`
        // on the same router; the router fans out the lookup until it
        // finds the matching shard.
        let collection = request.into_inner().collection;
        let mut last_err: Option<Status> = None;
        for (_, mut client) in self.clients_for_all_shards().await? {
            match client.compact_collection(&collection).await {
                Ok(id) => return Ok(Response::new(CompactCollectionResponse { compaction_id: id })),
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no shard accepted compact_collection")))
    }

    async fn get_compaction_state(
        &self,
        request: Request<GetCompactionStateRequest>,
    ) -> Result<Response<GetCompactionStateResponse>, Status> {
        let id = request.into_inner().compaction_id;
        let mut last_err: Option<Status> = None;
        for (_, mut client) in self.clients_for_shards_best_effort().await {
            match client.get_compaction_state(id).await {
                Ok(resp) => return Ok(Response::new(resp)),
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::not_found(format!("compaction {id} not found"))))
    }

    async fn list_persistent_segments(
        &self,
        request: Request<ListPersistentSegmentsRequest>,
    ) -> Result<Response<ListPersistentSegmentsResponse>, Status> {
        let collection = request.into_inner().collection;
        let mut merged: Vec<vectordb_proto::vectordb::v1::SegmentEntry> = vec![];
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_shards_best_effort().await {
            match client.list_persistent_segments(&collection).await {
                Ok(resp) => {
                    ok += 1;
                    merged.extend(resp.segments);
                }
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err.unwrap_or_else(|| {
                Status::unavailable("no shard accepted list_persistent_segments")
            }));
        }
        Ok(Response::new(ListPersistentSegmentsResponse { segments: merged }))
    }

    // ---- Partitions (Milvus parity) -----------------------------------------

    async fn create_partition(
        &self,
        request: Request<CreatePartitionRequest>,
    ) -> Result<Response<CreatePartitionResponse>, Status> {
        let bytes = request.into_inner().op_json;
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_all_shards().await? {
            match client.create_partition(bytes.clone()).await {
                Ok(()) => ok += 1,
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err
                .unwrap_or_else(|| Status::unavailable("no shard accepted create_partition")));
        }
        Ok(Response::new(CreatePartitionResponse {}))
    }

    async fn drop_partition(
        &self,
        request: Request<DropPartitionRequest>,
    ) -> Result<Response<DropPartitionResponse>, Status> {
        let bytes = request.into_inner().op_json;
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_all_shards().await? {
            match client.drop_partition(bytes.clone()).await {
                Ok(()) => ok += 1,
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err
                .unwrap_or_else(|| Status::unavailable("no shard accepted drop_partition")));
        }
        Ok(Response::new(DropPartitionResponse {}))
    }

    async fn has_partition(
        &self,
        request: Request<HasPartitionRequest>,
    ) -> Result<Response<HasPartitionResponse>, Status> {
        let req = request.into_inner();
        // All shards share replicated meta — first responder wins.
        let mut last_err: Option<Status> = None;
        for (_, mut client) in self.clients_for_shards_best_effort().await {
            match client.has_partition(&req.collection, &req.partition).await {
                Ok(exists) => return Ok(Response::new(HasPartitionResponse { exists })),
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no shard accepted has_partition")))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let collection = request.into_inner().collection;
        let mut last_err: Option<Status> = None;
        for (_, mut client) in self.clients_for_shards_best_effort().await {
            match client.list_partitions(&collection).await {
                Ok(parts) => {
                    return Ok(Response::new(ListPartitionsResponse { partitions: parts }))
                }
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no shard accepted list_partitions")))
    }

    async fn get_partition_stats(
        &self,
        request: Request<GetPartitionStatsRequest>,
    ) -> Result<Response<GetPartitionStatsResponse>, Status> {
        let req = request.into_inner();
        // Sum row_count across shards; carry non-numeric stats from the first
        // shard that reports them so callers still see partition_name etc.
        let mut total_rows: u64 = 0;
        let mut merged: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut last_err: Option<Status> = None;
        let mut ok = 0usize;
        for (_, mut client) in self.clients_for_shards_best_effort().await {
            match client
                .get_partition_stats(&req.collection, &req.partition)
                .await
            {
                Ok(stats) => {
                    ok += 1;
                    for (k, v) in stats {
                        if k == "row_count" {
                            if let Ok(n) = v.parse::<u64>() {
                                total_rows = total_rows.saturating_add(n);
                            }
                        } else {
                            merged.entry(k).or_insert(v);
                        }
                    }
                }
                Err(e) => last_err = Some(Status::internal(e.to_string())),
            }
        }
        if ok == 0 {
            return Err(last_err
                .unwrap_or_else(|| Status::unavailable("no shard accepted get_partition_stats")));
        }
        merged.insert("row_count".into(), total_rows.to_string());
        Ok(Response::new(GetPartitionStatsResponse { stats: merged }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Delete)?;
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
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Get)?;
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
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Insert)?;
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
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Reindex)?;
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
