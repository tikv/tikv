// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use azure_core::auth::TokenResponse;
use time::OffsetDateTime;

/// Refresh time to use in seconds
pub(crate) const DEFAULT_REFRESH_TIME: i64 = 300;

pub(crate) struct TokenCache {
    /// Cached token
    cached_token: Arc<RwLock<Option<TokenResponse>>>,
}

impl Default for TokenCache {
    fn default() -> Self {
        Self {
            cached_token: Arc::new(RwLock::new(None)),
        }
    }
}

impl TokenCache {
    pub fn need_update_token(&self) -> bool {
        if let Some(token) = self.cached_token.read().unwrap().as_ref() {
            let current_time = OffsetDateTime::now_utc().unix_timestamp();
            let expiry_time = current_time + DEFAULT_REFRESH_TIME;
            return expiry_time > token.expires_on.unix_timestamp();
        }
        true
    }

    pub fn update_token(&self, token: TokenResponse) {
        let mut cached_token = self.cached_token.write().unwrap();
        *cached_token = Some(token);
    }

    pub fn get_token(&self) -> TokenResponse {
        let cached_token = self.cached_token.read().unwrap();
        assert!(cached_token.is_some());
        cached_token.clone().unwrap()
    }
}

pub trait TokenCredentialWithCache {
    fn need_update_cached_token(&self) -> bool;

    fn update_cached_token(&self, token: TokenResponse);

    fn get_cached_token(&self) -> TokenResponse;
}
