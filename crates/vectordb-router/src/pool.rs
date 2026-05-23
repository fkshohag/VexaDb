use std::sync::Arc;

use dashmap::DashMap;
use vectordb_client::VectorDbClient;

/// Lazily connected gRPC clients keyed by endpoint URL.
#[derive(Clone, Default)]
pub struct ClientPool {
    clients: Arc<DashMap<String, VectorDbClient>>,
}

impl ClientPool {
    pub async fn get(&self, endpoint: &str) -> anyhow::Result<VectorDbClient> {
        if let Some(c) = self.clients.get(endpoint) {
            return Ok(c.clone());
        }
        let client = VectorDbClient::connect(endpoint).await?;
        self.clients.insert(endpoint.to_string(), client.clone());
        Ok(client)
    }
}
