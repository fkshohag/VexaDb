use std::sync::Arc;

use tonic::{Request, Response, Status};
use vectordb_cluster::ShardRouter;
use vectordb_core::{CollectionConfig, DistanceMetric};
use vectordb_proto::vectordb::v1::{
    vector_service_server::VectorService, CollectionSpec, CreateCollectionRequest,
    CreateCollectionResponse, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteRequest, DeleteResponse, DescribeCollectionRequest, DescribeCollectionResponse,
    DistanceMetric as ProtoMetric, GetRequest, GetResponse, HealthRequest, HealthResponse,
    ListCollectionsRequest, ListCollectionsResponse, SearchRequest, SearchResponse,
    UpsertRequest, UpsertResponse, VectorPoint,
};
use vectordb_storage::{CollectionEngine, EngineError};

use crate::config::ServerConfig;

pub struct VectorServiceImpl {
    engine: Arc<CollectionEngine>,
    router: ShardRouter,
    node_id: String,
    shard_count: u32,
    local_shard: u32,
}

impl VectorServiceImpl {
    pub fn new(cfg: ServerConfig) -> anyhow::Result<Self> {
        let engine = CollectionEngine::open(cfg.storage.clone())?;
        let cluster = cfg.cluster_config();
        let router = ShardRouter::from_cluster(&cluster, cfg.cluster.shard_count);
        Ok(Self {
            engine: Arc::new(engine),
            router,
            node_id: cfg.cluster.node_id,
            shard_count: cfg.cluster.shard_count,
            local_shard: cfg.cluster.shard_id,
        })
    }

    fn owns_point(&self, point_id: &str) -> bool {
        if self.shard_count <= 1 {
            return true;
        }
        self.router.shard_for_point(point_id) == self.local_shard
    }
}

#[tonic::async_trait]
impl VectorService for VectorServiceImpl {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            node_id: self.node_id.clone(),
            shard_count: self.shard_count,
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
        let config = spec_to_config(spec)?;
        self.engine
            .create_collection(config)
            .map_err(map_engine_err)?;
        Ok(Response::new(CreateCollectionResponse {}))
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        let name = request.into_inner().name;
        self.engine
            .delete_collection(&name)
            .map_err(map_engine_err)?;
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
            self.engine
                .upsert(&collection, point.id, vector, payload)
                .map_err(map_engine_err)?;
            upserted += 1;
        }
        Ok(Response::new(UpsertResponse { upserted }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let filter = if req.filter_ids.is_empty() {
            None
        } else {
            Some(req.filter_ids)
        };
        let hits = self
            .engine
            .search(
                &req.collection,
                &req.query,
                req.top_k.max(1) as usize,
                filter.as_deref(),
            )
            .map_err(map_engine_err)?;
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
            self.engine
                .delete(&req.collection, &id)
                .map_err(map_engine_err)?;
            deleted += 1;
        }
        Ok(Response::new(DeleteResponse { deleted }))
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
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
            Some((vector, payload)) => Ok(Response::new(GetResponse {
                found: true,
                point: Some(VectorPoint {
                    id: req.id,
                    values: vector.values,
                    payload: payload.unwrap_or_default(),
                }),
            })),
            None => Ok(Response::new(GetResponse {
                found: false,
                point: None,
            })),
        }
    }
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
    }
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

fn map_engine_err(e: EngineError) -> Status {
    match e {
        EngineError::CollectionNotFound(n) => Status::not_found(n),
        EngineError::CollectionExists(n) => Status::already_exists(n),
        EngineError::Core(c) => Status::invalid_argument(c.to_string()),
        other => Status::internal(other.to_string()),
    }
}
