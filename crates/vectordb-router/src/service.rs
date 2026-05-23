use std::collections::HashMap;

use futures::future::join_all;
use vectordb_cluster::{merge_top_k, ClusterConfig, ShardRouter};
use tonic::{Request, Response, Status, Streaming};
use vectordb_proto::vectordb::v1::{
    vector_service_server::VectorService, BulkUpsertRequest, BulkUpsertResponse, CollectionSpec,
    CompactWalRequest, CompactWalResponse, CreateCollectionRequest, CreateCollectionResponse,
    CreateSnapshotRequest, CreateSnapshotResponse, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteRequest, DeleteResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DescribeCollectionRequest, DescribeCollectionResponse, GetRequest, GetResponse, HealthRequest,
    HealthResponse, ImportChunk, ImportStreamResponse, ListCollectionsRequest,
    ListCollectionsResponse, ListSnapshotsRequest, ListSnapshotsResponse, ReindexCollectionRequest,
    ReindexCollectionResponse, SearchRequest, SearchResponse, UpsertRequest, UpsertResponse,
    VectorPoint,
};

use crate::pool::ClientPool;

pub struct RouterService {
    pool: ClientPool,
    router: ShardRouter,
    node_id: String,
}

impl RouterService {
    pub fn new(node_id: impl Into<String>, cluster: &ClusterConfig, shard_count: u32) -> Self {
        Self {
            pool: ClientPool::default(),
            router: ShardRouter::from_cluster(cluster, shard_count),
            node_id: node_id.into(),
        }
    }

    async fn clients_for_all_shards(
        &self,
    ) -> Result<Vec<(u32, vectordb_client::VectorDbClient)>, Status> {
        let mut out = Vec::new();
        for (shard, ep) in self.router.shard_endpoints() {
            let client = self
                .pool
                .get(ep)
                .await
                .map_err(|e| Status::unavailable(format!("shard {shard} at {ep}: {e}")))?;
            out.push((shard, client));
        }
        if out.is_empty() {
            return Err(Status::failed_precondition(
                "no shard endpoints configured; set cluster.nodes with advertise_addr",
            ));
        }
        Ok(out)
    }

    async fn client_for_point(
        &self,
        point_id: &str,
    ) -> Result<vectordb_client::VectorDbClient, Status> {
        let ep = self
            .router
            .endpoint_for_point(point_id)
            .ok_or_else(|| Status::not_found("no endpoint for point shard"))?;
        self.pool
            .get(ep)
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
            shard_count: self.router.shard_count(),
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
        let clients = self.clients_for_all_shards().await?;
        let mut names = Vec::new();
        if let Some((_, mut first)) = clients.into_iter().next() {
            names = first
                .list_collections()
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(ListCollectionsResponse { names }))
    }

    async fn describe_collection(
        &self,
        request: Request<DescribeCollectionRequest>,
    ) -> Result<Response<DescribeCollectionResponse>, Status> {
        let name = request.into_inner().name;
        let clients = self.clients_for_all_shards().await?;
        let mut spec: Option<CollectionSpec> = None;
        let mut total = 0u64;
        for (_, mut client) in clients {
            let (s, count) = client
                .describe_collection(&name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            if spec.is_none() {
                spec = Some(s);
            }
            total += count;
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
                .router
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

        let results = join_all(futures).await;
        let mut all_hits = Vec::new();
        for res in results {
            let hits = res.map_err(|e| Status::internal(e.to_string()))?;
            for h in hits {
                all_hits.push((h.id, h.score));
            }
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
                .router
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
                .router
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
                    .router
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
}
