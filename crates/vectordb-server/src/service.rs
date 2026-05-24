use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use vectordb_cluster::ShardRouter;
use vectordb_core::{
    CollectionConfig, DistanceMetric, Filter, PayloadFieldIndex, PayloadIndexKind,
    QuantizationConfig, SearchMode, SparseVector,
};
use vectordb_proto::vectordb::v1::{
    vector_service_server::VectorService, BulkUpsertRequest, BulkUpsertResponse,
    ClusterStatusRequest, ClusterStatusResponse, CollectionSpec, CompactWalRequest,
    CompactWalResponse, CreateCollectionRequest, CreateCollectionResponse, CreateSnapshotRequest,
    CreateSnapshotResponse, DeleteCollectionRequest, DeleteCollectionResponse, DeleteRequest,
    DeleteResponse, DeleteSnapshotRequest, DeleteSnapshotResponse, DescribeCollectionRequest,
    DescribeCollectionResponse, DistanceMetric as ProtoMetric, GetRequest, GetResponse,
    HealthRequest, HealthResponse, ImportChunk, ImportStreamResponse, ListCollectionsRequest,
    ListCollectionsResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    PayloadFieldIndex as ProtoPayloadIndex, PayloadIndexKind as ProtoIndexKind, RebalanceRequest,
    RebalanceResponse, RebalanceStatusRequest, RebalanceStatusResponse, RegisterNodeRequest,
    RegisterNodeResponse, ReindexCollectionRequest, ReindexCollectionResponse, ScrollRequest,
    ScrollResponse, SearchRequest, SearchResponse, SnapshotInfo, UpsertRequest, UpsertResponse,
    VectorPoint,
};
use vectordb_replication::RaftNode;
use vectordb_storage::search::SearchParams;
use vectordb_storage::{CollectionEngine, EngineError};

use crate::config::ServerConfig;
use crate::leader;
use crate::metrics::RpcTimer;
use crate::replication::ReplicatedEngine;
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

        let svc = Self {
            engine,
            replicated,
            router,
            node_id: node_id.clone(),
            shard_count,
            local_shard,
            vector_endpoint: vector_endpoint.clone(),
            readiness_requires_leader: cfg.server.readiness_requires_leader,
        };

        if let Some(router_ep) = router_grpc {
            spawn_router_registration(router_ep, node_id, vector_endpoint, shard_ids, shard_count);
        }

        Ok(svc)
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
        let name = request.into_inner().name;
        let cfg = self
            .engine
            .describe_collection(&name)
            .map_err(map_engine_err)?;
        let stats = self.engine.stats(&name).map_err(map_engine_err)?;
        Ok(Response::new(DescribeCollectionResponse {
            spec: Some(config_to_spec(cfg)),
            vector_count: stats.vector_count as u64,
        }))
    }

    async fn upsert(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<UpsertResponse>, Status> {
        let timer = RpcTimer::start("upsert");
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
        let req = request.into_inner();
        let filter: Option<Filter> = if req.filter_json.trim().is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&req.filter_json)
                    .map_err(|e| Status::invalid_argument(format!("invalid filter: {e}")))?,
            )
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
            hits: hits
                .into_iter()
                .map(|h| vectordb_proto::vectordb::v1::ScoredPoint {
                    id: h.id,
                    score: h.score,
                })
                .collect(),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let mut deleted = 0u64;
        for id in req.ids {
            if !self.owns_point(&id) {
                return Err(Status::failed_precondition(format!(
                    "point {id} belongs to another shard"
                )));
            }
            if let Some(rep) = &self.replicated {
                rep.delete(&req.collection, &id)
                    .await
                    .map_err(|e| leader::map_raft_err(e, self.raft_ref()))?;
            } else {
                self.engine
                    .delete(&req.collection, &id)
                    .map_err(map_engine_err)?;
            }
            deleted += 1;
        }
        Ok(Response::new(DeleteResponse { deleted }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
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
        let mut stream = request.into_inner();
        let mut collection = String::new();
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
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            256
        } else {
            req.limit as usize
        };
        let (rows, next_cursor) = self
            .engine
            .scroll(&req.collection, &req.cursor, limit)
            .map_err(map_engine_err)?;
        let points = rows
            .into_iter()
            .map(|(id, vector, payload)| VectorPoint {
                id,
                values: vector.values,
                payload: payload
                    .map(|v| serde_json::to_vec(&v).unwrap_or_default())
                    .unwrap_or_default(),
                sparse: None,
            })
            .collect();
        Ok(Response::new(ScrollResponse {
            points,
            next_cursor,
        }))
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

fn spec_to_config(spec: CollectionSpec) -> Result<CollectionConfig, Status> {
    let metric = proto_metric_to_core(
        ProtoMetric::try_from(spec.metric).unwrap_or(ProtoMetric::Unspecified),
    );
    let mut cfg = CollectionConfig::new(spec.name, spec.dimension as usize, metric);
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
        EngineError::Core(c) => Status::invalid_argument(c.to_string()),
        other => Status::internal(other.to_string()),
    }
}
