use anyhow::Context;
use tonic::transport::Channel;
use vectordb_proto::vectordb::v1::{
    CollectionSpec, CreateCollectionRequest, DeleteCollectionRequest, DeleteRequest,
    DescribeCollectionRequest, DistanceMetric, GetRequest, HealthRequest, ListCollectionsRequest,
    SearchRequest, UpsertRequest, VectorPoint,
};
use vectordb_proto::VectorServiceClient;

/// High-level Rust client for VectorDB gRPC API.
#[derive(Clone)]
pub struct VectorDbClient {
    inner: VectorServiceClient<Channel>,
}

impl VectorDbClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())
            .context("invalid endpoint")?
            .connect()
            .await
            .context("failed to connect")?;
        Ok(Self {
            inner: VectorServiceClient::new(channel),
        })
    }

    pub async fn health(&mut self) -> anyhow::Result<String> {
        let resp = self
            .inner
            .health(HealthRequest {})
            .await?
            .into_inner();
        Ok(resp.status)
    }

    pub async fn create_collection(&mut self, spec: CollectionSpec) -> anyhow::Result<()> {
        self.inner
            .create_collection(CreateCollectionRequest { spec: Some(spec) })
            .await?;
        Ok(())
    }

    pub async fn delete_collection(&mut self, name: &str) -> anyhow::Result<()> {
        self.inner
            .delete_collection(DeleteCollectionRequest {
                name: name.into(),
            })
            .await?;
        Ok(())
    }

    pub async fn list_collections(&mut self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .inner
            .list_collections(ListCollectionsRequest {})
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
            .describe_collection(DescribeCollectionRequest {
                name: name.into(),
            })
            .await?
            .into_inner();
        Ok((resp.spec.context("missing spec")?, resp.vector_count))
    }

    pub async fn upsert(
        &mut self,
        collection: &str,
        points: Vec<VectorPoint>,
    ) -> anyhow::Result<u64> {
        let resp = self
            .inner
            .upsert(UpsertRequest {
                collection: collection.into(),
                points,
            })
            .await?
            .into_inner();
        Ok(resp.upserted)
    }

    pub async fn search(
        &mut self,
        collection: &str,
        query: Vec<f32>,
        top_k: u32,
    ) -> anyhow::Result<Vec<vectordb_proto::vectordb::v1::ScoredPoint>> {
        Ok(self
            .inner
            .search(SearchRequest {
                collection: collection.into(),
                query,
                top_k,
                filter_ids: vec![],
            })
            .await?
            .into_inner()
            .hits)
    }

    pub async fn delete(&mut self, collection: &str, ids: Vec<String>) -> anyhow::Result<u64> {
        let resp = self
            .inner
            .delete(DeleteRequest {
                collection: collection.into(),
                ids,
            })
            .await?
            .into_inner();
        Ok(resp.deleted)
    }

    pub async fn get(
        &mut self,
        collection: &str,
        id: &str,
    ) -> anyhow::Result<Option<VectorPoint>> {
        let resp = self
            .inner
            .get(GetRequest {
                collection: collection.into(),
                id: id.into(),
            })
            .await?
            .into_inner();
        Ok(if resp.found { resp.point } else { None })
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
    }
}
