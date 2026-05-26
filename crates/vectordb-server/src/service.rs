use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use vectordb_cluster::ShardRouter;
use vectordb_core::{
    CollectionConfig, DistanceMetric, Filter, OutputOptions, PayloadFieldIndex, PayloadIndexKind,
    QuantizationConfig, ScoredPoint, SearchMode, SparseVector,
};
use vectordb_proto::vectordb::v1::{
    vector_service_server::VectorService, AddPayloadIndexRequest, AddPayloadIndexResponse,
    AliasEntry, AlterDatabaseRequest, AlterDatabaseResponse, ApplyRbacRequest, ApplyRbacResponse,
    BulkUpsertRequest, BulkUpsertResponse, ClusterStatusRequest, ClusterStatusResponse,
    CollectionSpec, CompactCollectionRequest, CompactCollectionResponse,
    CompactionState as ProtoCompactionState, CompactWalRequest, CompactWalResponse,
    CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, CreatePartitionRequest, CreatePartitionResponse,
    CreateResourceGroupRequest, CreateResourceGroupResponse, CreateSnapshotRequest,
    CreateSnapshotResponse, DatabaseInfo, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteRequest, DeleteResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DescribeAliasRequest, DescribeAliasResponse, DescribeCollectionRequest,
    DescribeCollectionResponse, DescribeDatabaseRequest, DescribeDatabaseResponse,
    DescribeReplicaRequest, DescribeReplicaResponse, DescribeResourceGroupRequest,
    DescribeResourceGroupResponse, DistanceMetric as ProtoMetric, DropDatabaseRequest,
    DropDatabaseResponse, DropPartitionRequest, DropPartitionResponse, DropPayloadIndexRequest,
    DropPayloadIndexResponse, DropResourceGroupRequest, DropResourceGroupResponse,
    AnalyzerResult, AnalyzerToken, HybridSearchRequest, HybridSearchResponse, RunAnalyzerRequest,
    RunAnalyzerResponse,
    FlushCollectionRequest, FlushCollectionResponse, GetCompactionStateRequest,
    GetCompactionStateResponse, GetPartitionStatsRequest, GetPartitionStatsResponse,
    GetRbacSnapshotRequest, GetRbacSnapshotResponse, GetRequest, GetResponse, HasPartitionRequest,
    HasPartitionResponse, HealthRequest, HealthResponse, ImportChunk, ImportStreamResponse,
    ListAliasesRequest, ListAliasesResponse, ListCollectionsRequest, ListCollectionsResponse,
    ListDatabasesRequest, ListDatabasesResponse, ListPartitionsRequest, ListPartitionsResponse,
    ListPersistentSegmentsRequest, ListPersistentSegmentsResponse, ListResourceGroupsRequest,
    ListResourceGroupsResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    MutateCollectionMetaRequest, MutateCollectionMetaResponse,
    PayloadFieldIndex as ProtoPayloadIndex, PayloadIndexKind as ProtoIndexKind, QueryRequest,
    QueryResponse, RebalanceRequest, RebalanceResponse, RebalanceStatusRequest,
    RebalanceStatusResponse, RegisterNodeRequest, RegisterNodeResponse, ReindexCollectionRequest,
    ReindexCollectionResponse, ResourceGroupConfig as ProtoRgConfig,
    ResourceGroupInfo as ProtoRgInfo, ResourceGroupLimit as ProtoRgLimit,
    ResourceGroupNodeFilter as ProtoRgNodeFilter, ResourceGroupTransfer as ProtoRgTransfer,
    ScrollRequest, ScrollResponse, SearchRequest, SearchResponse, SegmentEntry,
    SegmentState as ProtoSegmentState, SnapshotInfo, StatsRequest, StatsResponse,
    TransferReplicaRequest, TransferReplicaResponse, UpdateResourceGroupRequest,
    UpdateResourceGroupResponse, UpsertRequest, UpsertResponse, VectorPoint,
};
use vectordb_replication::RaftNode;
use vectordb_storage::search::SearchParams;
use vectordb_storage::{CollectionEngine, EngineError};

use crate::authz::{refresh_from_engine, spawn_engine_refresh, require_collection, RbacCache};
use crate::config::ServerConfig;
use crate::leader;
use crate::metrics::RpcTimer;
use crate::replication::ReplicatedEngine;
use vectordb_rbac::{require_global, ObjectType, Privilege};
use vectordb_storage::BulkPoint;

pub struct VectorServiceImpl {
    engine: Arc<CollectionEngine>,
    replicated: Option<Arc<ReplicatedEngine>>,
    router: ShardRouter,
    node_id: String,
    shard_count: u32,
    local_shard: u32,
    vector_endpoint: String,
    readiness_requires_leader: bool,
    rbac: RbacCache,
}

impl VectorServiceImpl {
    pub async fn new(cfg: ServerConfig) -> anyhow::Result<Self> {
        let engine = Arc::new(CollectionEngine::open(cfg.storage.clone())?);
        let vector_endpoint = cfg.vector_endpoint();
        let replicated = if let Some(mut raft_cfg) = cfg.raft.clone() {
            if raft_cfg.vector_endpoint.is_none() {
                raft_cfg.vector_endpoint = Some(vector_endpoint.clone());
            }
            let raft = RaftNode::start(raft_cfg, engine.clone()).await?;
            Some(Arc::new(ReplicatedEngine {
                engine: engine.clone(),
                raft,
            }))
        } else {
            None
        };
        let cluster = cfg.cluster_config();
        let router = ShardRouter::from_cluster(&cluster, cfg.cluster.shard_count);
        let node_id = cfg.cluster.node_id.clone();
        let shard_count = cfg.cluster.shard_count;
        let local_shard = cfg.cluster.shard_id;
        let shard_ids = vec![local_shard];
        let router_grpc = cfg.cluster.router_grpc.clone();

        let rbac = RbacCache::new(cfg.auth.keys.clone());
        refresh_from_engine(&rbac, &engine);
        spawn_engine_refresh(rbac.clone(), engine.clone(), std::time::Duration::from_secs(5));

        let svc = Self {
            engine,
            replicated,
            router,
            node_id: node_id.clone(),
            shard_count,
            local_shard,
            vector_endpoint: vector_endpoint.clone(),
            readiness_requires_leader: cfg.server.readiness_requires_leader,
            rbac,
        };

        if let Some(router_ep) = router_grpc {
            spawn_router_registration(router_ep, node_id, vector_endpoint, shard_ids, shard_count);
        }

        Ok(svc)
    }

    /// Expose the RBAC cache so the binary can wire it into the interceptor.
    pub fn rbac_cache(&self) -> RbacCache {
        self.rbac.clone()
    }

    fn raft_ref(&self) -> Option<&RaftNode> {
        self.replicated.as_ref().map(|r| r.raft.as_ref())
    }

    /// Shared handle to the underlying engine — for background tasks
    /// (auto-snapshot, rebalance, etc.).
    pub fn engine_handle(&self) -> Arc<CollectionEngine> {
        self.engine.clone()
    }

    fn owns_point(&self, point_id: &str) -> bool {
        if self.shard_count <= 1 {
            return true;
        }
        self.router.shard_for_point(point_id) == self.local_shard
    }

    fn health_status(&self) -> String {
        if let Some(rep) = &self.replicated {
            format!("ok;raft={:?}", rep.raft.role())
        } else {
            "ok".into()
        }
    }

    fn health_fields(&self) -> (bool, String, String, bool) {
        if let Some(rep) = &self.replicated {
            let raft = &rep.raft;
            let is_leader = raft.is_leader();
            let role = format!("{:?}", raft.role()).to_lowercase();
            let leader_endpoint = raft
                .leader_vector_endpoint()
                .unwrap_or_else(|| self.vector_endpoint.clone());
            let ready = !self.readiness_requires_leader || is_leader;
            (is_leader, role, leader_endpoint, ready)
        } else {
            (true, String::new(), self.vector_endpoint.clone(), true)
        }
    }
}

#[tonic::async_trait]
impl VectorService for VectorServiceImpl {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let (is_leader, raft_role, leader_endpoint, ready) = self.health_fields();
        Ok(Response::new(HealthResponse {
            status: self.health_status(),
            node_id: self.node_id.clone(),
            shard_count: self.shard_count,
            is_leader,
            raft_role,
            leader_endpoint,
            ready,
        }))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let timer = RpcTimer::start("create_collection");
        let spec = request
            .into_inner()
            .spec
            .ok_or_else(|| Status::invalid_argument("spec required"))?;
        let config = spec_to_config(spec)?;
        let result = if let Some(rep) = &self.replicated {
            rep.create_collection(config)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))
                .map(|_| ())
        } else {
            self.engine
                .create_collection(config)
                .map_err(map_engine_err)
        };
        match result {
            Ok(()) => {
                timer.finish(true);
                Ok(Response::new(CreateCollectionResponse {}))
            }
            Err(e) => {
                timer.finish(false);
                Err(e)
            }
        }
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().name, Privilege::DropCollection)?;
        let name = request.into_inner().name;
        if let Some(rep) = &self.replicated {
            rep.delete_collection(&name)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine
                .delete_collection(&name)
                .map_err(map_engine_err)?;
        }
        Ok(Response::new(DeleteCollectionResponse {}))
    }

    async fn list_collections(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let names = self.engine.list_collections();
        Ok(Response::new(ListCollectionsResponse { names }))
    }

    async fn describe_collection(
        &self,
        request: Request<DescribeCollectionRequest>,
    ) -> Result<Response<DescribeCollectionResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().name, Privilege::DescribeCollection)?;
        let name = request.into_inner().name;
        let resolved = self.engine.resolve_alias(&name);
        let cfg = self
            .engine
            .describe_collection(&resolved)
            .map_err(map_engine_err)?;
        let stats = self.engine.stats(&resolved).map_err(map_engine_err)?;
        let aliases = self.engine.aliases_for(&resolved);
        Ok(Response::new(DescribeCollectionResponse {
            spec: Some(config_to_spec(cfg)),
            vector_count: stats.vector_count as u64,
            aliases,
        }))
    }

    async fn upsert(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<UpsertResponse>, Status> {
        let timer = RpcTimer::start("upsert");
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Upsert)?;
        let req = request.into_inner();
        let collection = req.collection;
        let mut upserted = 0u64;
        for point in req.points {
            if !self.owns_point(&point.id) {
                return Err(Status::failed_precondition(format!(
                    "point {} belongs to another shard; use a router or correct node",
                    point.id
                )));
            }
            let vector = vectordb_core::Vector::new(point.values);
            let payload = if point.payload.is_empty() {
                None
            } else {
                Some(point.payload)
            };
            let sparse = proto_sparse_to_core(point.sparse);
            if let Some(rep) = &self.replicated {
                rep.upsert(&collection, point.id, vector, payload, sparse)
                    .await
                    .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
            } else {
                self.engine
                    .upsert(&collection, point.id, vector, payload, sparse)
                    .map_err(map_engine_err)?;
            }
            upserted += 1;
        }
        timer.finish(true);
        tracing::info!(
            rpc = "upsert",
            collection = %collection,
            upserted,
            "query completed"
        );
        Ok(Response::new(UpsertResponse { upserted }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let timer = RpcTimer::start("search");
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Search)?;
        let req = request.into_inner();
        let filter = parse_filter_json(&req.filter_json)?;
        let output = OutputOptions {
            output_fields: req.output_fields.clone(),
            with_payload: req.with_payload,
            with_vector: req.with_vector,
        };
        let sparse_query = proto_sparse_to_core(req.sparse_query);
        let text_query = if req.text_query.trim().is_empty() {
            None
        } else {
            Some(req.text_query.as_str())
        };
        let mode = SearchMode::parse(&req.search_mode);
        let mut hits = self
            .engine
            .search_params(
                &req.collection,
                SearchParams {
                    query: &req.query,
                    sparse_query: sparse_query.as_ref(),
                    text_query,
                    mode,
                    hybrid_alpha: if req.hybrid_alpha > 0.0 {
                        req.hybrid_alpha
                    } else {
                        0.5
                    },
                    filter: filter.as_ref(),
                },
                req.top_k.max(1) as usize,
                output,
            )
            .map_err(map_engine_err)?;

        if !req.filter_ids.is_empty() {
            let allowed: std::collections::HashSet<_> = req.filter_ids.iter().collect();
            hits.retain(|h| allowed.contains(&h.id));
        }

        let hit_count = hits.len();
        timer.finish(true);
        tracing::info!(
            rpc = "search",
            collection = %req.collection,
            top_k = req.top_k,
            hits = hit_count,
            "query completed"
        );
        Ok(Response::new(SearchResponse {
            hits: hits.into_iter().map(scored_to_proto).collect(),
        }))
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let timer = RpcTimer::start("query");
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Query)?;
        let req = request.into_inner();
        let filter = parse_filter_json(&req.filter_json)?;
        let output = OutputOptions {
            output_fields: req.output_fields.clone(),
            with_payload: req.with_payload,
            with_vector: req.with_vector,
        };
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let offset = req.offset as usize;
        let hits = self
            .engine
            .query(
                &req.collection,
                filter.as_ref(),
                &req.ids,
                limit,
                offset,
                output,
            )
            .map_err(map_engine_err)?;
        timer.finish(true);
        let points: Vec<VectorPoint> = hits
            .into_iter()
            .map(|h| scored_to_vector_point(h, req.with_vector))
            .collect();
        Ok(Response::new(QueryResponse {
            points,
            next_cursor: String::new(),
        }))
    }

    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::CollectionStats)?;
        let req = request.into_inner();
        let s = self
            .engine
            .stats(&req.collection)
            .map_err(map_engine_err)?;
        Ok(Response::new(StatsResponse {
            name: s.name,
            vector_count: s.vector_count as u64,
            dimension: s.dimension as u32,
            metric: core_metric_to_proto(s.metric) as i32,
            sparse_enabled: s.sparse_enabled,
            bm25_text_field: s.bm25_text_field,
            payload_index_count: s.payload_index_count as u32,
            scalar_quantization: s.scalar_quantization,
        }))
    }

    async fn apply_rbac(
        &self,
        request: Request<ApplyRbacRequest>,
    ) -> Result<Response<ApplyRbacResponse>, Status> {
        let req = request.into_inner();
        let op: vectordb_rbac::RbacOp = serde_json::from_slice(&req.op_json)
            .map_err(|e| Status::invalid_argument(format!("invalid RbacOp JSON: {e}")))?;
        if let Some(rep) = &self.replicated {
            rep.apply_rbac(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_rbac(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(ApplyRbacResponse {}))
    }

    async fn get_rbac_snapshot(
        &self,
        _request: Request<GetRbacSnapshotRequest>,
    ) -> Result<Response<GetRbacSnapshotResponse>, Status> {
        let snap = self.engine.rbac().snapshot();
        let json = serde_json::to_vec(&snap)
            .map_err(|e| Status::internal(format!("snapshot serialize: {e}")))?;
        Ok(Response::new(GetRbacSnapshotResponse { snapshot_json: json }))
    }

    async fn mutate_collection_meta(
        &self,
        request: Request<MutateCollectionMetaRequest>,
    ) -> Result<Response<MutateCollectionMetaResponse>, Status> {
        let op: vectordb_storage::MetaOp = serde_json::from_slice(&request.get_ref().op_json)
            .map_err(|e| Status::invalid_argument(format!("invalid MetaOp JSON: {e}")))?;
        // Authorize based on the affected collection. Aliases use their target.
        // The RBAC privilege check uses the fully-qualified `db/name` so that
        // grants on a specific (database, collection) don't accidentally cover
        // a same-named collection in another database.
        let (target, priv_kind) = match &op {
            vectordb_storage::MetaOp::RenameCollection { old, database, .. } => {
                (format!("{database}/{old}"), Privilege::AlterCollection)
            }
            vectordb_storage::MetaOp::CreateAlias {
                collection, database, ..
            }
            | vectordb_storage::MetaOp::AlterAlias {
                collection, database, ..
            } => (
                format!("{database}/{collection}"),
                Privilege::AlterAlias,
            ),
            vectordb_storage::MetaOp::DropAlias { alias, database } => {
                let fq_alias = format!("{database}/{alias}");
                let coll = self
                    .engine
                    .describe_alias(&fq_alias)
                    .unwrap_or(fq_alias);
                (coll, Privilege::AlterAlias)
            }
            vectordb_storage::MetaOp::AlterCollectionProperties { name, database, .. } => {
                (format!("{database}/{name}"), Privilege::AlterCollection)
            }
            // Database management ops belong on the dedicated RPCs; reject
            // them here so a misbehaving client can't bypass the per-RPC RBAC.
            vectordb_storage::MetaOp::CreateDatabase { .. }
            | vectordb_storage::MetaOp::DropDatabase { .. }
            | vectordb_storage::MetaOp::AlterDatabaseProperties { .. } => {
                return Err(Status::invalid_argument(
                    "database management ops must use the CreateDatabase / DropDatabase / AlterDatabase RPCs",
                ));
            }
            // Index management ops belong on AddPayloadIndex / DropPayloadIndex.
            vectordb_storage::MetaOp::AddPayloadIndex { .. }
            | vectordb_storage::MetaOp::DropPayloadIndex { .. } => {
                return Err(Status::invalid_argument(
                    "payload-index ops must use the AddPayloadIndex / DropPayloadIndex RPCs",
                ));
            }
            // Partition ops belong on the dedicated CreatePartition / DropPartition RPCs.
            vectordb_storage::MetaOp::CreatePartition { .. }
            | vectordb_storage::MetaOp::DropPartition { .. } => {
                return Err(Status::invalid_argument(
                    "partition ops must use the CreatePartition / DropPartition RPCs",
                ));
            }
            vectordb_storage::MetaOp::CreateResourceGroup { .. }
            | vectordb_storage::MetaOp::DropResourceGroup { .. }
            | vectordb_storage::MetaOp::UpdateResourceGroup { .. } => {
                return Err(Status::invalid_argument(
                    "resource-group ops must use the CreateResourceGroup / DropResourceGroup / UpdateResourceGroup RPCs",
                ));
            }
        };
        require_collection(&self.rbac, &request, &target, priv_kind)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(MutateCollectionMetaResponse {}))
    }

    async fn list_aliases(
        &self,
        request: Request<ListAliasesRequest>,
    ) -> Result<Response<ListAliasesResponse>, Status> {
        let req = request.into_inner();
        let aliases = if req.collection.is_empty() {
            self.engine.list_aliases()
        } else {
            self.engine
                .aliases_for(&req.collection)
                .into_iter()
                .map(|a| (a, req.collection.clone()))
                .collect()
        };
        Ok(Response::new(ListAliasesResponse {
            aliases: aliases
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
        let collection = self
            .engine
            .describe_alias(&alias)
            .map_err(map_engine_err)?;
        Ok(Response::new(DescribeAliasResponse { collection }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Delete)?;
        let req = request.into_inner();
        let mut deleted = 0u64;
        for id in &req.ids {
            if !self.owns_point(id) {
                return Err(Status::failed_precondition(format!(
                    "point {id} belongs to another shard"
                )));
            }
            if let Some(rep) = &self.replicated {
                rep.delete(&req.collection, id)
                    .await
                    .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
            } else {
                self.engine
                    .delete(&req.collection, id)
                    .map_err(map_engine_err)?;
            }
            deleted += 1;
        }
        // Filter / partition-scoped delete (Milvus parity). Parse the expr
        // here so the engine sees a strongly-typed `Filter`. Empty filter
        // + empty partition + empty ids is treated as a no-op.
        if !req.filter.is_empty() || !req.partition.is_empty() {
            let filter = if req.filter.is_empty() {
                None
            } else {
                Some(vectordb_core::parse_filter_expr(&req.filter).map_err(|e| {
                    Status::invalid_argument(format!("invalid filter expression: {e}"))
                })?)
            };
            let partition = if req.partition.is_empty() {
                None
            } else {
                Some(req.partition.as_str())
            };
            deleted += self
                .engine
                .delete_by_filter(&req.collection, filter.as_ref(), partition)
                .map_err(map_engine_err)?;
        }
        Ok(Response::new(DeleteResponse { deleted }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Get)?;
        let req = request.into_inner();
        if !self.owns_point(&req.id) {
            return Err(Status::failed_precondition(
                "point belongs to another shard",
            ));
        }
        let found = self
            .engine
            .get(&req.collection, &req.id)
            .map_err(map_engine_err)?;
        match found {
            Some((vector, payload)) => {
                let payload_bytes = payload
                    .map(|v| serde_json::to_vec(&v).unwrap_or_default())
                    .unwrap_or_default();
                Ok(Response::new(GetResponse {
                    found: true,
                    point: Some(VectorPoint {
                        id: req.id,
                        values: vector.values,
                        payload: payload_bytes,
                        sparse: None,
                    }),
                }))
            }
            None => Ok(Response::new(GetResponse {
                found: false,
                point: None,
            })),
        }
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let mgr = self.engine.snapshot_manager();
        let meta = mgr
            .create()
            .map_err(|e| Status::internal(format!("snapshot: {e}")))?;
        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(SnapshotInfo {
                id: meta.id,
                created_at_ms: meta.created_at_ms as u64,
                path: meta.data_dir.to_string_lossy().into_owned(),
            }),
        }))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let mgr = self.engine.snapshot_manager();
        let snaps = mgr
            .list()
            .map_err(|e| Status::internal(format!("snapshot: {e}")))?;
        Ok(Response::new(ListSnapshotsResponse {
            snapshots: snaps
                .into_iter()
                .map(|m| SnapshotInfo {
                    id: m.id,
                    created_at_ms: m.created_at_ms as u64,
                    path: m.data_dir.to_string_lossy().into_owned(),
                })
                .collect(),
        }))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let id = request.into_inner().id;
        self.engine
            .snapshot_manager()
            .delete(&id)
            .map_err(|e| Status::internal(format!("snapshot: {e}")))?;
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn bulk_upsert(
        &self,
        request: Request<BulkUpsertRequest>,
    ) -> Result<Response<BulkUpsertResponse>, Status> {
        let timer = RpcTimer::start("bulk_upsert");
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Insert)?;
        let req = request.into_inner();
        let chunk_size = if req.chunk_size == 0 {
            500
        } else {
            req.chunk_size as usize
        };
        let points = proto_points_to_bulk(req.points, &self)?;
        let upserted = if let Some(rep) = &self.replicated {
            rep.bulk_upsert(&req.collection, points, chunk_size)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?
        } else {
            self.engine
                .bulk_upsert(&req.collection, points, chunk_size)
                .map_err(map_engine_err)?
        };
        timer.finish(true);
        Ok(Response::new(BulkUpsertResponse { upserted }))
    }

    async fn import_stream(
        &self,
        request: Request<Streaming<ImportChunk>>,
    ) -> Result<Response<ImportStreamResponse>, Status> {
        let timer = RpcTimer::start("import_stream");
        let principal = request.extensions().get::<vectordb_rbac::Principal>().cloned();
        let mut stream = request.into_inner();
        let mut collection = String::new();
        let mut collection_authorized = false;
        let mut buffer: Vec<BulkPoint> = Vec::new();
        let mut total = 0u64;
        const FLUSH: usize = 500;

        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            if !chunk.collection.is_empty() {
                collection = chunk.collection.clone();
            }
            if collection.is_empty() {
                return Err(Status::invalid_argument("collection required"));
            }
            if !collection_authorized {
                if self.rbac.enabled() {
                    let p = principal
                        .as_ref()
                        .ok_or_else(|| Status::unauthenticated("principal missing"))?;
                    self.rbac
                        .authorize(p, ObjectType::Collection, &collection, Privilege::Insert.as_str())
                        .map_err(|_| {
                            Status::permission_denied(format!(
                                "no Insert privilege on collection {collection}"
                            ))
                        })?;
                }
                collection_authorized = true;
            }
            let mut batch = proto_points_to_bulk(chunk.points, &self)?;
            buffer.append(&mut batch);
            if buffer.len() >= FLUSH || chunk.finalize {
                total += self
                    .flush_import_batch(&collection, &mut buffer, FLUSH)
                    .await?;
            }
        }
        if !buffer.is_empty() {
            total += self
                .flush_import_batch(&collection, &mut buffer, FLUSH)
                .await?;
        }
        timer.finish(true);
        Ok(Response::new(ImportStreamResponse { upserted: total }))
    }

    async fn compact_wal(
        &self,
        request: Request<CompactWalRequest>,
    ) -> Result<Response<CompactWalResponse>, Status> {
        if let Some(rep) = &self.replicated {
            rep.ensure_leader()
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        }
        let snapshot_first = request.into_inner().snapshot_first;
        let (snapshot, stats) = if snapshot_first {
            let (snap, stats) = self
                .engine
                .snapshot_and_compact_wal()
                .map_err(map_engine_err)?;
            (Some(snap), stats)
        } else {
            let stats = self.engine.compact_wal().map_err(map_engine_err)?;
            (None, stats)
        };
        Ok(Response::new(CompactWalResponse {
            entries_before: stats.before as u64,
            entries_after: stats.after as u64,
            snapshot: snapshot.map(|m| SnapshotInfo {
                id: m.id,
                created_at_ms: m.created_at_ms as u64,
                path: m.data_dir.to_string_lossy().into_owned(),
            }),
        }))
    }

    async fn reindex_collection(
        &self,
        request: Request<ReindexCollectionRequest>,
    ) -> Result<Response<ReindexCollectionResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Reindex)?;
        if let Some(rep) = &self.replicated {
            rep.ensure_leader()
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        }
        let name = request.into_inner().collection;
        let n = self
            .engine
            .reindex_collection(&name)
            .map_err(map_engine_err)?;
        Ok(Response::new(ReindexCollectionResponse {
            vectors_reindexed: n,
        }))
    }

    async fn rebalance(
        &self,
        _request: Request<RebalanceRequest>,
    ) -> Result<Response<RebalanceResponse>, Status> {
        Err(Status::failed_precondition(
            "Rebalance must be sent to the router, not a data node",
        ))
    }

    async fn rebalance_status(
        &self,
        _request: Request<RebalanceStatusRequest>,
    ) -> Result<Response<RebalanceStatusResponse>, Status> {
        Err(Status::failed_precondition(
            "RebalanceStatus must be sent to the router, not a data node",
        ))
    }

    async fn scroll(
        &self,
        request: Request<ScrollRequest>,
    ) -> Result<Response<ScrollResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Query)?;
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            256
        } else {
            req.limit as usize
        };
        let filter = if req.filter.is_empty() {
            None
        } else {
            Some(vectordb_core::parse_filter_expr(&req.filter).map_err(|e| {
                Status::invalid_argument(format!("invalid filter expression: {e}"))
            })?)
        };
        let partition = if req.partition.is_empty() {
            None
        } else {
            Some(req.partition.as_str())
        };
        let (rows, next_cursor) = self
            .engine
            .scroll_filtered(&req.collection, &req.cursor, limit, filter.as_ref(), partition)
            .map_err(map_engine_err)?;
        let with_vec = req.with_vector;
        let with_pay = req.with_payload || !req.output_fields.is_empty();
        let points = rows
            .into_iter()
            .map(|(id, vector, payload)| VectorPoint {
                id,
                values: if with_vec {
                    vector.map(|v| v.values).unwrap_or_default()
                } else {
                    Vec::new()
                },
                payload: if with_pay {
                    payload
                        .map(|v| serde_json::to_vec(&v).unwrap_or_default())
                        .unwrap_or_default()
                } else {
                    Vec::new()
                },
                sparse: None,
            })
            .collect();
        Ok(Response::new(ScrollResponse {
            points,
            next_cursor,
        }))
    }

    // ---- Database management (Milvus parity) ------------------------------

    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let op = parse_database_meta_op(&request.get_ref().op_json)?;
        ensure_create_database(&op)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(CreateDatabaseResponse {}))
    }

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        let op = parse_database_meta_op(&request.get_ref().op_json)?;
        ensure_drop_database(&op)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(DropDatabaseResponse {}))
    }

    async fn alter_database(
        &self,
        request: Request<AlterDatabaseRequest>,
    ) -> Result<Response<AlterDatabaseResponse>, Status> {
        let op = parse_database_meta_op(&request.get_ref().op_json)?;
        ensure_alter_database(&op)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(AlterDatabaseResponse {}))
    }

    async fn list_databases(
        &self,
        _request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let names = self.engine.list_databases();
        Ok(Response::new(ListDatabasesResponse { names }))
    }

    async fn describe_database(
        &self,
        request: Request<DescribeDatabaseRequest>,
    ) -> Result<Response<DescribeDatabaseResponse>, Status> {
        let name = request.into_inner().name;
        let cfg = self
            .engine
            .describe_database(&name)
            .map_err(map_engine_err)?;
        Ok(Response::new(DescribeDatabaseResponse {
            info: Some(DatabaseInfo {
                name: cfg.name,
                properties: cfg.properties.into_iter().collect(),
                created_at_ms: cfg.created_at_ms,
            }),
        }))
    }

    // ---- Management: indexes / flush / compact / segments (Milvus parity) -

    async fn add_payload_index(
        &self,
        request: Request<AddPayloadIndexRequest>,
    ) -> Result<Response<AddPayloadIndexResponse>, Status> {
        let op = parse_index_meta_op(&request.get_ref().op_json)?;
        ensure_add_payload_index(&op)?;
        let target = index_op_target(&op);
        require_collection(&self.rbac, &request, &target, Privilege::CreateIndex)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(AddPayloadIndexResponse {}))
    }

    async fn drop_payload_index(
        &self,
        request: Request<DropPayloadIndexRequest>,
    ) -> Result<Response<DropPayloadIndexResponse>, Status> {
        let op = parse_index_meta_op(&request.get_ref().op_json)?;
        ensure_drop_payload_index(&op)?;
        let target = index_op_target(&op);
        require_collection(&self.rbac, &request, &target, Privilege::DropIndex)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(DropPayloadIndexResponse {}))
    }

    async fn flush_collection(
        &self,
        request: Request<FlushCollectionRequest>,
    ) -> Result<Response<FlushCollectionResponse>, Status> {
        let collection = request.into_inner().collection;
        let info = self
            .engine
            .flush_collection(&collection)
            .map_err(map_engine_err)?;
        Ok(Response::new(FlushCollectionResponse {
            collection: info.collection,
            flush_ts_ms: info.flush_ts_ms,
            segment_ids: info.segment_ids,
            flushed_segment_ids: info.flushed_segment_ids,
            wal_entries: info.wal_entries as u64,
        }))
    }

    async fn compact_collection(
        &self,
        request: Request<CompactCollectionRequest>,
    ) -> Result<Response<CompactCollectionResponse>, Status> {
        let collection = request.into_inner().collection;
        let id = self
            .engine
            .compact_collection(&collection)
            .map_err(map_engine_err)?;
        Ok(Response::new(CompactCollectionResponse {
            compaction_id: id,
        }))
    }

    async fn get_compaction_state(
        &self,
        request: Request<GetCompactionStateRequest>,
    ) -> Result<Response<GetCompactionStateResponse>, Status> {
        let id = request.into_inner().compaction_id;
        let s = self
            .engine
            .compaction_state(id)
            .map_err(map_engine_err)?;
        Ok(Response::new(GetCompactionStateResponse {
            compaction_id: s.id,
            collection: s.collection,
            state: core_compaction_state_to_proto(s.state) as i32,
            entries_before: s.entries_before as u64,
            entries_after: s.entries_after as u64,
            started_ms: s.started_ms,
            finished_ms: s.finished_ms,
            error: s.error.unwrap_or_default(),
        }))
    }

    async fn list_persistent_segments(
        &self,
        request: Request<ListPersistentSegmentsRequest>,
    ) -> Result<Response<ListPersistentSegmentsResponse>, Status> {
        let collection = request.into_inner().collection;
        let segs = self
            .engine
            .persistent_segments(&collection)
            .map_err(map_engine_err)?;
        Ok(Response::new(ListPersistentSegmentsResponse {
            segments: segs
                .into_iter()
                .map(|s| SegmentEntry {
                    id: s.id,
                    collection: s.collection,
                    num_rows: s.num_rows,
                    state: core_segment_state_to_proto(s.state) as i32,
                    source: s.source,
                })
                .collect(),
        }))
    }

    // ---- Partitions (Milvus parity) -----------------------------------------

    async fn create_partition(
        &self,
        request: Request<CreatePartitionRequest>,
    ) -> Result<Response<CreatePartitionResponse>, Status> {
        let op = parse_partition_meta_op(&request.get_ref().op_json)?;
        ensure_create_partition(&op)?;
        let target = partition_op_target(&op);
        require_collection(&self.rbac, &request, &target, Privilege::CreatePartition)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(CreatePartitionResponse {}))
    }

    async fn drop_partition(
        &self,
        request: Request<DropPartitionRequest>,
    ) -> Result<Response<DropPartitionResponse>, Status> {
        let op = parse_partition_meta_op(&request.get_ref().op_json)?;
        ensure_drop_partition(&op)?;
        let target = partition_op_target(&op);
        require_collection(&self.rbac, &request, &target, Privilege::DropPartition)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(DropPartitionResponse {}))
    }

    async fn has_partition(
        &self,
        request: Request<HasPartitionRequest>,
    ) -> Result<Response<HasPartitionResponse>, Status> {
        let req = request.into_inner();
        let exists = self
            .engine
            .has_partition(&req.collection, &req.partition)
            .map_err(map_engine_err)?;
        Ok(Response::new(HasPartitionResponse { exists }))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let collection = request.into_inner().collection;
        let partitions = self
            .engine
            .list_partitions(&collection)
            .map_err(map_engine_err)?;
        Ok(Response::new(ListPartitionsResponse { partitions }))
    }

    async fn get_partition_stats(
        &self,
        request: Request<GetPartitionStatsRequest>,
    ) -> Result<Response<GetPartitionStatsResponse>, Status> {
        let req = request.into_inner();
        let stats = self
            .engine
            .partition_stats(&req.collection, &req.partition)
            .map_err(map_engine_err)?;
        Ok(Response::new(GetPartitionStatsResponse {
            stats: stats.into_iter().collect(),
        }))
    }

    // ---- Resource groups (Milvus parity) ------------------------------------

    async fn create_resource_group(
        &self,
        request: Request<CreateResourceGroupRequest>,
    ) -> Result<Response<CreateResourceGroupResponse>, Status> {
        let op = parse_rg_meta_op(&request.get_ref().op_json)?;
        ensure_create_resource_group(&op)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(CreateResourceGroupResponse {}))
    }

    async fn drop_resource_group(
        &self,
        request: Request<DropResourceGroupRequest>,
    ) -> Result<Response<DropResourceGroupResponse>, Status> {
        let op = parse_rg_meta_op(&request.get_ref().op_json)?;
        ensure_drop_resource_group(&op)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(DropResourceGroupResponse {}))
    }

    async fn update_resource_group(
        &self,
        request: Request<UpdateResourceGroupRequest>,
    ) -> Result<Response<UpdateResourceGroupResponse>, Status> {
        let op = parse_rg_meta_op(&request.get_ref().op_json)?;
        ensure_update_resource_group(&op)?;
        if let Some(rep) = &self.replicated {
            rep.apply_meta(op)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
        } else {
            self.engine.commit_meta(op).map_err(map_engine_err)?;
        }
        Ok(Response::new(UpdateResourceGroupResponse {}))
    }

    async fn list_resource_groups(
        &self,
        _request: Request<ListResourceGroupsRequest>,
    ) -> Result<Response<ListResourceGroupsResponse>, Status> {
        Ok(Response::new(ListResourceGroupsResponse {
            names: self.engine.list_resource_groups(),
        }))
    }

    async fn describe_resource_group(
        &self,
        request: Request<DescribeResourceGroupRequest>,
    ) -> Result<Response<DescribeResourceGroupResponse>, Status> {
        let name = request.into_inner().name;
        let info = self
            .engine
            .describe_resource_group(&name)
            .map_err(map_engine_err)?;
        Ok(Response::new(DescribeResourceGroupResponse {
            info: Some(rg_info_to_proto(info)),
        }))
    }

    async fn describe_replica(
        &self,
        request: Request<DescribeReplicaRequest>,
    ) -> Result<Response<DescribeReplicaResponse>, Status> {
        // VexaDb does not yet expose a logical "replica" view on per-node
        // shards, so the data-node handler returns the empty list and lets
        // the router synthesize a single-replica view from topology.
        let _ = request.into_inner().collection;
        Ok(Response::new(DescribeReplicaResponse { replicas: vec![] }))
    }

    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        require_collection(&self.rbac, &request, &request.get_ref().collection, Privilege::Search)?;
        let req = request.into_inner();
        if req.requests.is_empty() {
            return Err(Status::invalid_argument(
                "HybridSearch requires at least one AnnRequest",
            ));
        }
        // Parse per-leg filters up front so the engine sees typed Filters.
        let mut leg_filters: Vec<Option<Filter>> = Vec::with_capacity(req.requests.len());
        let mut sparse_legs: Vec<Option<vectordb_core::SparseVector>> = Vec::with_capacity(req.requests.len());
        for r in &req.requests {
            leg_filters.push(if r.filter.is_empty() {
                None
            } else {
                Some(vectordb_core::parse_filter_expr(&r.filter).map_err(|e| {
                    Status::invalid_argument(format!("invalid leg filter: {e}"))
                })?)
            });
            sparse_legs.push(proto_sparse_to_core(r.sparse_query.clone()));
        }
        let mut ann_requests: Vec<vectordb_storage::search::AnnRequest<'_>> = Vec::with_capacity(req.requests.len());
        for (i, r) in req.requests.iter().enumerate() {
            let query = if !r.dense_query.is_empty() {
                vectordb_storage::search::AnnQuery::Dense(&r.dense_query)
            } else if let Some(s) = sparse_legs[i].as_ref() {
                vectordb_storage::search::AnnQuery::Sparse(s)
            } else if !r.text_query.is_empty() {
                vectordb_storage::search::AnnQuery::Text(r.text_query.as_str())
            } else {
                return Err(Status::invalid_argument(format!(
                    "AnnRequest[{i}] missing dense / sparse / text query"
                )));
            };
            ann_requests.push(vectordb_storage::search::AnnRequest {
                field: r.field.clone(),
                limit: r.limit.max(1) as usize,
                query,
                filter: leg_filters[i].as_ref(),
            });
        }
        let reranker = match req.reranker_kind.as_str() {
            "" | "rrf" => vectordb_storage::search::Reranker::Rrf,
            "weighted" => vectordb_storage::search::Reranker::Weighted(req.reranker_weights.clone()),
            "function" => vectordb_storage::search::Reranker::Function,
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown reranker_kind: {other} (want rrf|weighted|function)"
                )))
            }
        };
        let output = OutputOptions {
            output_fields: req.output_fields.clone(),
            with_payload: req.with_payload,
            with_vector: req.with_vector,
        };
        let hits = self
            .engine
            .hybrid_search_multi(
                &req.collection,
                ann_requests,
                reranker,
                req.limit.max(1) as usize,
                output,
            )
            .map_err(map_engine_err)?;
        Ok(Response::new(HybridSearchResponse {
            hits: hits.into_iter().map(scored_to_proto).collect(),
        }))
    }

    async fn run_analyzer(
        &self,
        request: Request<RunAnalyzerRequest>,
    ) -> Result<Response<RunAnalyzerResponse>, Status> {
        // No collection scope; gated by global Query privilege.
        require_global(&self.rbac, &request, Privilege::Query)?;
        let req = request.into_inner();
        if req.text.is_empty() {
            return Ok(Response::new(RunAnalyzerResponse { results: vec![] }));
        }
        // Pull optional stop_words out of analyzer_params_json. We accept
        // either Milvus's nested `filter: [{type: "stop", stop_words: [...]}]`
        // shape or a flat `stop_words: [...]` for convenience.
        let mut stop_words: Vec<String> = Vec::new();
        if !req.analyzer_params_json.is_empty() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&req.analyzer_params_json) {
                if let Some(arr) = v.get("stop_words").and_then(|x| x.as_array()) {
                    for s in arr {
                        if let Some(s) = s.as_str() {
                            stop_words.push(s.to_string());
                        }
                    }
                }
                if let Some(filters) = v.get("filter").and_then(|x| x.as_array()) {
                    for f in filters {
                        if f.get("type").and_then(|t| t.as_str()) == Some("stop") {
                            if let Some(arr) = f.get("stop_words").and_then(|x| x.as_array()) {
                                for s in arr {
                                    if let Some(s) = s.as_str() {
                                        stop_words.push(s.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let results = self
            .engine
            .analyze_text(&req.text, &stop_words)
            .map_err(map_engine_err)?;
        let results = results
            .into_iter()
            .map(|tokens| AnalyzerResult {
                tokens: tokens
                    .into_iter()
                    .map(|t| AnalyzerToken {
                        token: t.token,
                        start_offset: t.start_offset as u64,
                        end_offset: t.end_offset as u64,
                        position: t.position as u64,
                        hash: t.hash,
                    })
                    .collect(),
            })
            .collect();
        Ok(Response::new(RunAnalyzerResponse { results }))
    }

    async fn transfer_replica(
        &self,
        request: Request<TransferReplicaRequest>,
    ) -> Result<Response<TransferReplicaResponse>, Status> {
        // Registry-only mode: replica assignments are not tracked per RG, so
        // we treat this as a no-op success after validating both groups
        // exist. This matches Milvus's contract: callers can rely on a
        // successful return value but should not assume nodes actually
        // moved.
        let req = request.into_inner();
        for g in [&req.source_group, &req.target_group] {
            self.engine
                .describe_resource_group(g)
                .map_err(map_engine_err)?;
        }
        tracing::info!(
            collection = %req.collection,
            source = %req.source_group,
            target = %req.target_group,
            "transfer_replica acknowledged (registry-only)"
        );
        Ok(Response::new(TransferReplicaResponse {}))
    }

    async fn register_node(
        &self,
        _request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        Err(Status::failed_precondition(
            "RegisterNode must be sent to the router",
        ))
    }

    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        Err(Status::failed_precondition(
            "ClusterStatus must be sent to the router",
        ))
    }
}

impl VectorServiceImpl {
    async fn flush_import_batch(
        &self,
        collection: &str,
        buffer: &mut Vec<BulkPoint>,
        chunk_size: usize,
    ) -> Result<u64, Status> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let points = std::mem::take(buffer);
        if let Some(rep) = &self.replicated {
            rep.bulk_upsert(collection, points, chunk_size)
                .await
                .map_err(|e| leader::map_raft_err(e, self.raft_ref()))
        } else {
            self.engine
                .bulk_upsert(collection, points, chunk_size)
                .map_err(map_engine_err)
        }
    }
}

/// Heartbeat loop: data nodes register with the router on boot and every 30s.
fn spawn_router_registration(
    router_grpc: String,
    node_id: String,
    grpc: String,
    shard_ids: Vec<u32>,
    shard_count: u32,
) {
    tokio::spawn(async move {
        async fn try_register(
            router_grpc: &str,
            node_id: &str,
            grpc: &str,
            shard_ids: &[u32],
            shard_count: u32,
        ) {
            match vectordb_client::VectorDbClient::connect(router_grpc.to_string()).await {
                Ok(mut client) => {
                    if let Err(e) = client
                        .register_node(node_id, grpc, shard_ids.to_vec(), shard_count)
                        .await
                    {
                        tracing::warn!(node_id = %node_id, error = %e, "router registration failed");
                    }
                }
                Err(e) => tracing::warn!(
                    router = %router_grpc,
                    error = %e,
                    "cannot reach router for registration"
                ),
            }
        }

        try_register(&router_grpc, &node_id, &grpc, &shard_ids, shard_count).await;

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.tick().await;
        loop {
            interval.tick().await;
            try_register(&router_grpc, &node_id, &grpc, &shard_ids, shard_count).await;
        }
    });
}

fn parse_filter_json(s: &str) -> Result<Option<Filter>, Status> {
    vectordb_core::parse_filter_input(s)
        .map_err(|e| Status::invalid_argument(e.to_string()))
}

fn scored_to_proto(h: ScoredPoint) -> vectordb_proto::vectordb::v1::ScoredPoint {
    vectordb_proto::vectordb::v1::ScoredPoint {
        id: h.id,
        score: h.score,
        payload: h
            .payload
            .as_ref()
            .map(|p| serde_json::to_vec(p).unwrap_or_default())
            .unwrap_or_default(),
        vector: h.vector.unwrap_or_default(),
    }
}

fn scored_to_vector_point(h: ScoredPoint, with_vector: bool) -> VectorPoint {
    VectorPoint {
        id: h.id,
        values: if with_vector {
            h.vector.unwrap_or_default()
        } else {
            vec![]
        },
        payload: h
            .payload
            .as_ref()
            .map(|p| serde_json::to_vec(p).unwrap_or_default())
            .unwrap_or_default(),
        sparse: None,
    }
}

fn proto_points_to_bulk(
    points: Vec<VectorPoint>,
    svc: &VectorServiceImpl,
) -> Result<Vec<BulkPoint>, Status> {
    let mut out = Vec::with_capacity(points.len());
    for point in points {
        if !svc.owns_point(&point.id) {
            return Err(Status::failed_precondition(format!(
                "point {} belongs to another shard",
                point.id
            )));
        }
        let payload = if point.payload.is_empty() {
            None
        } else {
            Some(point.payload)
        };
        out.push(BulkPoint {
            id: point.id,
            vector: vectordb_core::Vector::new(point.values),
            payload,
            sparse: proto_sparse_to_core(point.sparse),
        });
    }
    Ok(out)
}

/// Parse a MetaOp JSON payload destined for one of the database-management
/// RPCs. Rejects ops that are not database-scoped so a misbehaving client
/// can't reuse the same endpoint to mutate collections.
fn parse_database_meta_op(bytes: &[u8]) -> Result<vectordb_storage::MetaOp, Status> {
    serde_json::from_slice::<vectordb_storage::MetaOp>(bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid MetaOp JSON: {e}")))
}

fn ensure_create_database(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::CreateDatabase { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "expected MetaOp::CreateDatabase",
        ))
    }
}

fn ensure_drop_database(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::DropDatabase { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument("expected MetaOp::DropDatabase"))
    }
}

fn ensure_alter_database(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::AlterDatabaseProperties { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "expected MetaOp::AlterDatabaseProperties",
        ))
    }
}

/// Parse a MetaOp JSON payload destined for the index-management RPCs.
fn parse_index_meta_op(bytes: &[u8]) -> Result<vectordb_storage::MetaOp, Status> {
    serde_json::from_slice::<vectordb_storage::MetaOp>(bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid MetaOp JSON: {e}")))
}

fn ensure_add_payload_index(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::AddPayloadIndex { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "expected MetaOp::AddPayloadIndex",
        ))
    }
}

fn ensure_drop_payload_index(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::DropPayloadIndex { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "expected MetaOp::DropPayloadIndex",
        ))
    }
}

/// FQN of the collection affected by an index MetaOp. Used for per-collection
/// RBAC checks on the management RPCs.
fn index_op_target(op: &vectordb_storage::MetaOp) -> String {
    match op {
        vectordb_storage::MetaOp::AddPayloadIndex {
            collection,
            database,
            ..
        }
        | vectordb_storage::MetaOp::DropPayloadIndex {
            collection,
            database,
            ..
        } => format!("{database}/{collection}"),
        _ => "*".into(),
    }
}

/// Parse a MetaOp JSON payload destined for the partition RPCs.
fn parse_partition_meta_op(bytes: &[u8]) -> Result<vectordb_storage::MetaOp, Status> {
    serde_json::from_slice::<vectordb_storage::MetaOp>(bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid MetaOp JSON: {e}")))
}

fn ensure_create_partition(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::CreatePartition { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument("expected MetaOp::CreatePartition"))
    }
}

fn ensure_drop_partition(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::DropPartition { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument("expected MetaOp::DropPartition"))
    }
}

fn partition_op_target(op: &vectordb_storage::MetaOp) -> String {
    match op {
        vectordb_storage::MetaOp::CreatePartition {
            collection,
            database,
            ..
        }
        | vectordb_storage::MetaOp::DropPartition {
            collection,
            database,
            ..
        } => format!("{database}/{collection}"),
        _ => "*".into(),
    }
}

/// Parse a MetaOp JSON payload destined for the resource-group RPCs.
fn parse_rg_meta_op(bytes: &[u8]) -> Result<vectordb_storage::MetaOp, Status> {
    serde_json::from_slice::<vectordb_storage::MetaOp>(bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid MetaOp JSON: {e}")))
}

fn ensure_create_resource_group(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::CreateResourceGroup { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "expected MetaOp::CreateResourceGroup",
        ))
    }
}

fn ensure_drop_resource_group(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::DropResourceGroup { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "expected MetaOp::DropResourceGroup",
        ))
    }
}

fn ensure_update_resource_group(op: &vectordb_storage::MetaOp) -> Result<(), Status> {
    if matches!(op, vectordb_storage::MetaOp::UpdateResourceGroup { .. }) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "expected MetaOp::UpdateResourceGroup",
        ))
    }
}

fn rg_info_to_proto(info: vectordb_core::ResourceGroupInfo) -> ProtoRgInfo {
    ProtoRgInfo {
        name: info.name,
        config: Some(rg_config_to_proto(info.config)),
        num_available_node: info.num_available_node,
        num_loaded_replica: info.num_loaded_replica.into_iter().collect(),
        num_incoming_node: info.num_incoming_node.into_iter().collect(),
        num_outgoing_node: info.num_outgoing_node.into_iter().collect(),
        created_at_ms: info.created_at_ms,
    }
}

fn rg_config_to_proto(cfg: vectordb_core::ResourceGroupConfig) -> ProtoRgConfig {
    ProtoRgConfig {
        requests: Some(ProtoRgLimit {
            node_num: cfg.requests.node_num,
        }),
        limits: Some(ProtoRgLimit {
            node_num: cfg.limits.node_num,
        }),
        transfer_from: cfg
            .transfer_from
            .into_iter()
            .map(|t| ProtoRgTransfer {
                resource_group: t.resource_group,
            })
            .collect(),
        transfer_to: cfg
            .transfer_to
            .into_iter()
            .map(|t| ProtoRgTransfer {
                resource_group: t.resource_group,
            })
            .collect(),
        node_filter: Some(ProtoRgNodeFilter {
            node_labels: cfg.node_filter.node_labels.into_iter().collect(),
        }),
    }
}

fn core_compaction_state_to_proto(
    s: vectordb_storage::CompactionStateCode,
) -> ProtoCompactionState {
    match s {
        vectordb_storage::CompactionStateCode::Running => ProtoCompactionState::Running,
        vectordb_storage::CompactionStateCode::Completed => ProtoCompactionState::Completed,
        vectordb_storage::CompactionStateCode::Failed => ProtoCompactionState::Failed,
    }
}

fn core_segment_state_to_proto(s: vectordb_storage::SegmentState) -> ProtoSegmentState {
    match s {
        vectordb_storage::SegmentState::Growing => ProtoSegmentState::Growing,
        vectordb_storage::SegmentState::Sealed => ProtoSegmentState::Sealed,
        vectordb_storage::SegmentState::Flushed => ProtoSegmentState::Flushed,
    }
}

fn spec_to_config(spec: CollectionSpec) -> Result<CollectionConfig, Status> {
    let metric = proto_metric_to_core(
        ProtoMetric::try_from(spec.metric).unwrap_or(ProtoMetric::Unspecified),
    );
    let mut cfg = CollectionConfig::new(spec.name, spec.dimension as usize, metric);
    if !spec.database.is_empty() {
        cfg.database = spec.database;
    }
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
    cfg.properties = spec.properties.into_iter().collect();
    cfg.validate()
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    Ok(cfg)
}

fn config_to_spec(cfg: CollectionConfig) -> CollectionSpec {
    CollectionSpec {
        name: cfg.name,
        dimension: cfg.dimension as u32,
        metric: core_metric_to_proto(cfg.metric) as i32,
        m: cfg.m as u32,
        ef_construction: cfg.ef_construction as u32,
        ef_search: cfg.ef_search as u32,
        payload_indexes: cfg
            .payload_indexes
            .into_iter()
            .map(core_index_to_proto)
            .collect(),
        sparse_enabled: cfg.sparse_enabled,
        bm25_text_field: cfg.bm25_text_field.clone().unwrap_or_default(),
        scalar_quantization: cfg.quantization.as_ref().map(|q| q.scalar).unwrap_or(false),
        properties: cfg.properties.into_iter().collect(),
        database: cfg.database,
    }
}

fn proto_sparse_to_core(
    sparse: Option<vectordb_proto::vectordb::v1::SparseVector>,
) -> Option<SparseVector> {
    let s = sparse?;
    if s.indices.is_empty() {
        return None;
    }
    Some(SparseVector::new(s.indices, s.values))
}

fn proto_metric_to_core(m: ProtoMetric) -> DistanceMetric {
    match m {
        ProtoMetric::Cosine | ProtoMetric::Unspecified => DistanceMetric::Cosine,
        ProtoMetric::Euclidean => DistanceMetric::Euclidean,
        ProtoMetric::DotProduct => DistanceMetric::DotProduct,
    }
}

fn core_metric_to_proto(m: DistanceMetric) -> ProtoMetric {
    match m {
        DistanceMetric::Cosine => ProtoMetric::Cosine,
        DistanceMetric::Euclidean => ProtoMetric::Euclidean,
        DistanceMetric::DotProduct => ProtoMetric::DotProduct,
    }
}

fn proto_index_to_core(p: ProtoPayloadIndex) -> Option<PayloadFieldIndex> {
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

fn core_index_to_proto(p: PayloadFieldIndex) -> ProtoPayloadIndex {
    let kind = match p.kind {
        PayloadIndexKind::Keyword => ProtoIndexKind::Keyword,
        PayloadIndexKind::Numeric => ProtoIndexKind::Numeric,
        PayloadIndexKind::Bool => ProtoIndexKind::Bool,
    };
    ProtoPayloadIndex {
        field: p.field,
        kind: kind as i32,
    }
}

fn map_engine_err(e: EngineError) -> Status {
    match e {
        EngineError::CollectionNotFound(n) => Status::not_found(n),
        EngineError::CollectionExists(n) => Status::already_exists(n),
        EngineError::AliasNotFound(n) => Status::not_found(format!("alias not found: {n}")),
        EngineError::AliasExists(n) => Status::already_exists(format!("alias exists: {n}")),
        EngineError::DatabaseNotFound(n) => Status::not_found(format!("database not found: {n}")),
        EngineError::DatabaseExists(n) => Status::already_exists(format!("database exists: {n}")),
        EngineError::DatabaseNotEmpty(n) => Status::failed_precondition(format!(
            "database {n} is not empty (use force=true to cascade drop)"
        )),
        EngineError::PartitionNotFound(n) => {
            Status::not_found(format!("partition not found: {n}"))
        }
        EngineError::PartitionExists(n) => {
            Status::already_exists(format!("partition exists: {n}"))
        }
        EngineError::ResourceGroupNotFound(n) => {
            Status::not_found(format!("resource group not found: {n}"))
        }
        EngineError::ResourceGroupExists(n) => {
            Status::already_exists(format!("resource group exists: {n}"))
        }
        EngineError::InvalidMeta(m) => Status::invalid_argument(m),
        EngineError::Core(c) => Status::invalid_argument(c.to_string()),
        other => Status::internal(other.to_string()),
    }
}
