// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex, RwLock};

use azure_core::auth::TokenResponse;
use time::OffsetDateTime;

/// Refresh time to use in seconds
pub(crate) const DEFAULT_REFRESH_TIME: i64 = 300;
/// Timeout on refreshing token in seconds
const DEFAULT_REFRESH_TOKEN_TIMEOUT: i64 = 10;

pub(crate) struct TokenCache {
    /// Cached token
    cached_token: Arc<RwLock<Option<TokenResponse>>>,
    /// Waited to be refreshed
    wait_refreshed: Arc<Mutex<bool>>,
}

impl Default for TokenCache {
    fn default() -> Self {
        Self {
            cached_token: Arc::new(RwLock::new(None)),
            wait_refreshed: Arc::new(Mutex::new(false)),
        }
    }
}

impl TokenCache {
    fn is_fresh(&self) -> bool {
        if let Some(token) = self.cached_token.read().unwrap().as_ref() {
            let current_time = OffsetDateTime::now_utc().unix_timestamp();
            let expiry_time = current_time + DEFAULT_REFRESH_TIME;
            return expiry_time <= token.expires_on.unix_timestamp();
        }
        false
    }

    pub fn need_update_token(&self) -> bool {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        loop {
            if self.is_fresh() {
                return false;
            }
            if let Ok(ref mut mtx) = self.wait_refreshed.lock() {
                // Only allowed he first one who gets the lock to refresh the token.
                if !**mtx {
                    **mtx = true;
                    break;
                }
                // Timeout to forcely trigger refreshing token.
                if OffsetDateTime::now_utc().unix_timestamp() - now >= DEFAULT_REFRESH_TOKEN_TIMEOUT
                {
                    break;
                }
            }
        }
        true
    }

    pub fn update_token(&self, token: TokenResponse) {
        let mut cached_token = self.cached_token.write().unwrap();
        *cached_token = Some(token);
        // Mark `wait_refresed` to be FALSE, making it waited for next refreshing.
        let mut wait_refreshed = self.wait_refreshed.lock().unwrap();
        *wait_refreshed = false;
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
