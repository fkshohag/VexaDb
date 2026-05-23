use std::sync::Arc;

use tonic::{Request, Status};
use vectordb_auth::{AuthConfig, AuthError, HEADER_API_KEY, HEADER_AUTHORIZATION};

#[derive(Clone)]
pub struct ApiKeyInterceptor {
    auth: Arc<AuthConfig>,
    exempt_health: bool,
}

impl ApiKeyInterceptor {
    pub fn new(auth: AuthConfig, exempt_health: bool) -> Self {
        Self {
            auth: Arc::new(auth),
            exempt_health,
        }
    }
}

impl tonic::service::Interceptor for ApiKeyInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        if !self.auth.is_enabled() {
            return Ok(request);
        }

        if self.exempt_health {
            let path = request.uri().path();
            if path.ends_with("/Health") {
                return Ok(request);
            }
        }

        let md = request.metadata();
        let authorization = md
            .get(HEADER_AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        let api_key = md.get(HEADER_API_KEY).and_then(|v| v.to_str().ok());

        self.auth
            .check_headers(authorization, api_key)
            .map_err(|e| match e {
                AuthError::MissingKey => Status::unauthenticated("missing API key"),
                AuthError::InvalidKey => Status::unauthenticated("invalid API key"),
            })?;

        Ok(request)
    }
}
