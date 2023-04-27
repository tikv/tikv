// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{str, sync::Arc};

use azure_core::{
    auth::{TokenCredential, TokenResponse},
    HttpClient,
};
use azure_identity::{ClientSecretCredential, TokenCredentialOptions};

use super::utils::{TokenCache, TokenCredentialWithCache};

/// Enables authentication to Azure Active Directory using a client secret that
/// was generated for an App Registration. It will automatically cache the
/// latest token from Azure Active Directory.
pub struct ClientSecretCredentialWithCache {
    credential: ClientSecretCredential,

    /// Cached token
    cached_token: TokenCache,
}

impl ClientSecretCredentialWithCache {
    /// Create a new ClientSecretCredential
    pub fn new(
        http_client: Arc<dyn HttpClient>,
        tenant_id: String,
        client_id: String,
        client_secret: String,
        options: TokenCredentialOptions,
    ) -> Self {
        Self {
            credential: ClientSecretCredential::new(
                http_client,
                tenant_id,
                client_id,
                client_secret,
                options,
            ),
            cached_token: TokenCache::default(),
        }
    }
}

impl TokenCredentialWithCache for ClientSecretCredentialWithCache {
    fn need_update_cached_token(&self) -> bool {
        self.cached_token.need_update_token()
    }

    fn update_cached_token(&self, token: TokenResponse) {
        self.cached_token.update_token(token);
    }

    fn get_cached_token(&self) -> TokenResponse {
        self.cached_token.get_token()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for ClientSecretCredentialWithCache {
    async fn get_token(&self, resource: &str) -> azure_core::Result<TokenResponse> {
        if !self.need_update_cached_token() {
            return Ok(self.get_cached_token());
        }
        let token = self.credential.get_token(resource).await?;
        // Update cached token
        self.update_cached_token(token.clone());
        Ok(token)
    }
}
