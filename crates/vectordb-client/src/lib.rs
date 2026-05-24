mod leader;

use anyhow::Context;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;
use tonic::{Request, Status};
use vectordb_auth::HEADER_API_KEY;
use vectordb_proto::vectordb::v1::{
    BulkUpsertRequest, CollectionSpec, CompactWalRequest, CompactWalResponse, CreateCollectionRequest,
    CreateSnapshotRequest, DeleteCollectionRequest, DeleteRequest, DeleteSnapshotRequest,
    DescribeCollectionRequest, DistanceMetric, GetRequest, HealthRequest, HealthResponse,
    ImportChunk, ListCollectionsRequest, ListSnapshotsRequest, ReindexCollectionRequest,
    ReindexCollectionResponse, SearchRequest, SnapshotInfo, UpsertRequest, VectorPoint,
};
use vectordb_proto::VectorServiceClient;

pub use leader::{is_not_leader, leader_from_status};

/// High-level Rust client for VectorDB gRPC API.
#[derive(Clone)]
pub struct VectorDbClient {
    inner: VectorServiceClient<Channel>,
    endpoint: String,
    api_key: Option<String>,
}

impl VectorDbClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Self::connect_with(endpoint, None).await
    }

    pub async fn connect_with(
        endpoint: impl Into<String>,
        api_key: Option<String>,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())
            .context("invalid endpoint")?
            .connect()
            .await
            .context("failed to connect")?;
        Ok(Self {
            inner: VectorServiceClient::new(channel),
            endpoint,
            api_key,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn authed<T>(&self, payload: T) -> Request<T> {
        let mut req = Request::new(payload);
        if let Some(key) = &self.api_key {
            if let Ok(v) = MetadataValue::<Ascii>::try_from(key.as_str()) {
                req.metadata_mut().insert(HEADER_API_KEY, v);
            }
        }
        req
    }

    async fn redirect_on_leader<F, Fut, T>(&self, op: F) -> Result<T, Status>
    where
        F: Fn(VectorDbClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        match op(self.clone()).await {
            Ok(v) => Ok(v),
            Err(status) if leader::is_not_leader(&status) => {
                let ep = leader::leader_from_status(&status)
                    .ok_or_else(|| status.clone())?;
                let leader =
                    VectorDbClient::connect_with(ep, self.api_key.clone())
                        .await
                        .map_err(|e| Status::unavailable(e.to_string()))?;
                op(leader).await
            }
            Err(e) => Err(e),
        }
    }

    pub async fn health(&mut self) -> anyhow::Result<String> {
        Ok(self.health_detail().await?.status)
    }

    pub async fn health_detail(&mut self) -> anyhow::Result<HealthResponse> {
        let resp = self
            .inner
            .health(self.authed(HealthRequest {}))
            .await?
            .into_inner();
        Ok(resp)
    }

    pub async fn create_collection(&mut self, spec: CollectionSpec) -> anyhow::Result<()> {
        self.redirect_on_leader(|mut c| {
            let spec = spec.clone();
            async move {
                c.inner
                    .create_collection(c.authed(CreateCollectionRequest { spec: Some(spec) }))
                    .await?;
                Ok(())
            }
        })
        .await
        .map_err(|s| anyhow::anyhow!("{s}"))
    }

    pub async fn delete_collection(&mut self, name: &str) -> anyhow::Result<()> {
        let name = name.to_string();
        self.redirect_on_leader(|mut c| {
            let name = name.clone();
            async move {
                c.inner
                    .delete_collection(c.authed(DeleteCollectionRequest { name }))
                    .await?;
                Ok(())
            }
        })
        .await
        .map_err(|s| anyhow::anyhow!("{s}"))
    }

    pub async fn list_collections(&mut self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .inner
            .list_collections(self.authed(ListCollectionsRequest {}))
            .await?
            .into_inner()
            .names)
    }

    pub async fn describe_collection(
        &mut self,
        name: &str,
    ) -> anyhow::Result<(CollectionSpec, u64)> {
        let resp = self
            .inner
            .describe_collection(self.authed(DescribeCollectionRequest {
                name: name.into(),
            }))
            .await?
            .into_inner();
        Ok((resp.spec.context("missing spec")?, resp.vector_count))
    }

    pub async fn upsert(
        &mut self,
        collection: &str,
        points: Vec<VectorPoint>,
    ) -> anyhow::Result<u64> {
        let collection = collection.to_string();
        self.redirect_on_leader(|mut c| {
            let collection = collection.clone();
            let points = points.clone();
            async move {
                let resp = c
                    .inner
                    .upsert(c.authed(UpsertRequest {
                        collection,
                        points,
                    }))
                    .await?
                    .into_inner();
                Ok(resp.upserted)
            }
        })
        .await
        .map_err(|s| anyhow::anyhow!("{s}"))
    }

    pub async fn search(
        &mut self,
        collection: &str,
        query: Vec<f32>,
        top_k: u32,
    ) -> anyhow::Result<Vec<vectordb_proto::vectordb::v1::ScoredPoint>> {
        self.search_with(collection, query, top_k, vec![], String::new())
            .await
    }

    pub async fn search_with(
        &mut self,
        collection: &str,
        query: Vec<f32>,
        top_k: u32,
        filter_ids: Vec<String>,
        filter_json: String,
    ) -> anyhow::Result<Vec<vectordb_proto::vectordb::v1::ScoredPoint>> {
        self.search_hybrid(collection, query, top_k, filter_ids, filter_json, None, None, "", 0.5)
            .await
    }

    /// Full search including sparse/BM25/hybrid modes.
    pub async fn search_hybrid(
        &mut self,
        collection: &str,
        query: Vec<f32>,
        top_k: u32,
        filter_ids: Vec<String>,
        filter_json: String,
        sparse_query: Option<vectordb_proto::vectordb::v1::SparseVector>,
        text_query: Option<String>,
        search_mode: &str,
        hybrid_alpha: f32,
    ) -> anyhow::Result<Vec<vectordb_proto::vectordb::v1::ScoredPoint>> {
        Ok(self
            .inner
            .search(self.authed(SearchRequest {
                collection: collection.into(),
                query,
                top_k,
                filter_ids,
                filter_json,
                sparse_query,
                text_query: text_query.unwrap_or_default(),
                search_mode: search_mode.into(),
                hybrid_alpha,
            }))
            .await?
            .into_inner()
            .hits)
    }

    pub async fn delete(&mut self, collection: &str, ids: Vec<String>) -> anyhow::Result<u64> {
        let collection = collection.to_string();
        self.redirect_on_leader(|mut c| {
            let collection = collection.clone();
            let ids = ids.clone();
            async move {
                let resp = c
                    .inner
                    .delete(c.authed(DeleteRequest { collection, ids }))
                    .await?
                    .into_inner();
                Ok(resp.deleted)
            }
        })
        .await
        .map_err(|s| anyhow::anyhow!("{s}"))
    }

    pub async fn get(
        &mut self,
        collection: &str,
        id: &str,
    ) -> anyhow::Result<Option<VectorPoint>> {
        let resp = self
            .inner
            .get(self.authed(GetRequest {
                collection: collection.into(),
                id: id.into(),
            }))
            .await?
            .into_inner();
        Ok(if resp.found { resp.point } else { None })
    }

    pub async fn create_snapshot(&mut self) -> anyhow::Result<SnapshotInfo> {
        let resp = self
            .inner
            .create_snapshot(self.authed(CreateSnapshotRequest {}))
            .await?
            .into_inner();
        resp.snapshot.context("missing snapshot")
    }

    pub async fn list_snapshots(&mut self) -> anyhow::Result<Vec<SnapshotInfo>> {
        Ok(self
            .inner
            .list_snapshots(self.authed(ListSnapshotsRequest {}))
            .await?
            .into_inner()
            .snapshots)
    }

    pub async fn delete_snapshot(&mut self, id: &str) -> anyhow::Result<()> {
        self.inner
            .delete_snapshot(self.authed(DeleteSnapshotRequest { id: id.into() }))
            .await?;
        Ok(())
    }

    /// Page through every point on this *one* node's local shard.
    /// `cursor` is opaque; pass an empty string to start. The returned cursor
    /// is empty when the iteration is complete.
    pub async fn scroll(
        &mut self,
        collection: &str,
        cursor: &str,
        limit: u32,
    ) -> anyhow::Result<(Vec<VectorPoint>, String)> {
        let resp = self
            .inner
            .scroll(self.authed(vectordb_proto::vectordb::v1::ScrollRequest {
                collection: collection.into(),
                cursor: cursor.into(),
                limit,
            }))
            .await?
            .into_inner();
        Ok((resp.points, resp.next_cursor))
    }

    /// Trigger a one-shot rebalance sweep on the router.
    pub async fn rebalance(
        &mut self,
        dry_run: bool,
    ) -> anyhow::Result<vectordb_proto::vectordb::v1::RebalanceResponse> {
        let resp = self
            .inner
            .rebalance(self.authed(vectordb_proto::vectordb::v1::RebalanceRequest { dry_run }))
            .await?
            .into_inner();
        Ok(resp)
    }

    /// Read auto-rebalance status from the router.
    pub async fn rebalance_status(
        &mut self,
    ) -> anyhow::Result<vectordb_proto::vectordb::v1::RebalanceStatusResponse> {
        Ok(self
            .inner
            .rebalance_status(self.authed(
                vectordb_proto::vectordb::v1::RebalanceStatusRequest {},
            ))
            .await?
            .into_inner())
    }

    /// Register this data node with the cluster router (heartbeat).
    pub async fn register_node(
        &mut self,
        node_id: &str,
        grpc: &str,
        shard_ids: Vec<u32>,
        shard_count: u32,
    ) -> anyhow::Result<vectordb_proto::vectordb::v1::RegisterNodeResponse> {
        let resp = self
            .inner
            .register_node(self.authed(vectordb_proto::vectordb::v1::RegisterNodeRequest {
                node_id: node_id.into(),
                grpc: grpc.into(),
                shard_ids,
                shard_count,
            }))
            .await?
            .into_inner();
        Ok(resp)
    }

    /// Cluster topology snapshot from the router.
    pub async fn cluster_status(
        &mut self,
    ) -> anyhow::Result<vectordb_proto::vectordb::v1::ClusterStatusResponse> {
        Ok(self
            .inner
            .cluster_status(self.authed(
                vectordb_proto::vectordb::v1::ClusterStatusRequest {},
            ))
            .await?
            .into_inner())
    }

    pub async fn bulk_upsert(
        &mut self,
        collection: &str,
        points: Vec<VectorPoint>,
        chunk_size: u32,
    ) -> anyhow::Result<u64> {
        let collection = collection.to_string();
        self.redirect_on_leader(|mut c| {
            let collection = collection.clone();
            let points = points.clone();
            async move {
                let resp = c
                    .inner
                    .bulk_upsert(c.authed(BulkUpsertRequest {
                        collection,
                        points,
                        chunk_size,
                    }))
                    .await?
                    .into_inner();
                Ok(resp.upserted)
            }
        })
        .await
        .map_err(|s| anyhow::anyhow!("{s}"))
    }

    /// Stream-imports chunks to the connected node.
    ///
    /// Note: streaming RPCs are not transparently redirected to the leader
    /// because the request stream can only be consumed once. Connect directly
    /// to the leader endpoint when calling this.
    pub async fn import_stream(
        &mut self,
        chunks: Vec<ImportChunk>,
    ) -> anyhow::Result<u64> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            for chunk in chunks {
                if tx.send(chunk).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let resp = self
            .inner
            .import_stream(self.authed(stream))
            .await?
            .into_inner();
        Ok(resp.upserted)
    }

    pub async fn compact_wal(&mut self, snapshot_first: bool) -> anyhow::Result<CompactWalResponse> {
        self.redirect_on_leader(|mut c| async move {
            Ok(c.inner
                .compact_wal(c.authed(CompactWalRequest { snapshot_first }))
                .await?
                .into_inner())
        })
        .await
        .map_err(|s| anyhow::anyhow!("{s}"))
    }

    pub async fn reindex_collection(
        &mut self,
        collection: &str,
    ) -> anyhow::Result<ReindexCollectionResponse> {
        let collection = collection.to_string();
        self.redirect_on_leader(|mut c| {
            let collection = collection.clone();
            async move {
                Ok(c.inner
                    .reindex_collection(c.authed(ReindexCollectionRequest { collection }))
                    .await?
                    .into_inner())
            }
        })
        .await
        .map_err(|s| anyhow::anyhow!("{s}"))
    }
}

pub fn cosine_collection(name: &str, dimension: u32) -> CollectionSpec {
    CollectionSpec {
        name: name.into(),
        dimension,
        metric: DistanceMetric::Cosine as i32,
        m: 16,
        ef_construction: 200,
        ef_search: 64,
        payload_indexes: Vec::new(),
        sparse_enabled: false,
        bm25_text_field: String::new(),
        scalar_quantization: false,
    }
}
