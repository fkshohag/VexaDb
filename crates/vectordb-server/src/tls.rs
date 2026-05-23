use std::fs;

use serde::{Deserialize, Serialize};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_file: String,
    pub key_file: String,
    #[serde(default)]
    pub client_ca_file: Option<String>,
}

impl TlsConfig {
    pub fn server_tls_config(&self) -> anyhow::Result<ServerTlsConfig> {
        let cert = fs::read(&self.cert_file)?;
        let key = fs::read(&self.key_file)?;
        let identity = Identity::from_pem(cert, key);
        let mut cfg = ServerTlsConfig::new().identity(identity);
        if let Some(ca_path) = &self.client_ca_file {
            let ca = fs::read(ca_path)?;
            cfg = cfg.client_ca_root(Certificate::from_pem(ca));
        }
        Ok(cfg)
    }

}
