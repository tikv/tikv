// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex, RwLock};

use azure_core::auth::TokenResponse;
use time::OffsetDateTime;

/// Refresh time to use in seconds
pub(crate) const DEFAULT_REFRESH_TIME: i64 = 300;
/// Timeout on refreshing token in seconds
const DEFAULT_REFRESH_TOKEN_TIMEOUT: i64 = 10;

fn config_default_refresh_token_timeout() -> i64 {
    fail::fail_point!("on_config_default_refresh_token_timeout", |_| 1);
    DEFAULT_REFRESH_TOKEN_TIMEOUT
}

#[inline]
fn is_token_fresh(token: &TokenResponse) -> bool {
    let refresh_ts = OffsetDateTime::now_utc().unix_timestamp() + DEFAULT_REFRESH_TIME;
    refresh_ts <= token.expires_on.unix_timestamp()
}

#[derive(Clone)]
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
    #[inline]
    fn is_fresh(&self) -> bool {
        fail::fail_point!("on_is_fresh_return_true", |_| { true });
        if let Ok(token) = self.cached_token.read() {
            if let Some(token) = token.as_ref() {
                return is_token_fresh(token);
            }
        }
        false
    }

    /// Check whether need to update the cached token.
    ///
    /// This function is set to permit only one WRITE thread to update the
    /// cached token, unless the WRITE thread is time out for updating token.
    #[inline]
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
                if OffsetDateTime::now_utc().unix_timestamp() - now
                    >= config_default_refresh_token_timeout()
                {
                    break;
                }
            }
        }
        true
    }

    #[inline]
    pub fn update_token(&self, token: TokenResponse) {
        if let Ok(mut cached_token) = self.cached_token.write() {
            *cached_token = Some(token);
            // Mark `wait_refresed` to be FALSE, making it waited for next refreshing.
            let mut wait_refreshed = self.wait_refreshed.lock().unwrap();
            *wait_refreshed = false;
        }
    }

    #[inline]
    pub fn get_token(&self) -> Option<TokenResponse> {
        match self.cached_token.read() {
            Ok(cached_token) => {
                let token = cached_token.clone();
                debug_assert!(token.is_some());
                if is_token_fresh(token.as_ref().unwrap()) {
                    token
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}

pub trait TokenCredentialWithCache {
    fn need_update_cached_token(&self) -> bool;

    fn update_cached_token(&self, token: TokenResponse);

    fn get_cached_token(&self) -> Option<TokenResponse>;
}

#[cfg(test)]
mod tests {
    use azure_core::auth::AccessToken;

    use super::*;

    #[test]
    fn test_token_cache_basement() {
        let acces_token = AccessToken::new("test_token");
        let expire_on =
            OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 1)
                .unwrap();
        let token1 = TokenResponse::new(acces_token.clone(), expire_on);
        let token2 = TokenResponse::new(
            acces_token,
            expire_on + std::time::Duration::from_secs(DEFAULT_REFRESH_TIME as u64),
        );
        assert!(!is_token_fresh(&token1));
        assert!(is_token_fresh(&token2));

        let token_cache1 = TokenCache {
            cached_token: Arc::new(RwLock::new(Some(token1))),
            ..TokenCache::default()
        };
        let token_cache2 = TokenCache {
            cached_token: Arc::new(RwLock::new(Some(token2))),
            ..TokenCache::default()
        };
        assert!(token_cache1.need_update_token());
        assert!(!token_cache2.need_update_token());
        assert!(token_cache1.get_token().is_none());
        let cached_token2 = token_cache2.get_token().unwrap();
        assert!(is_token_fresh(&cached_token2));

        token_cache1.update_token(cached_token2);
        assert!(token_cache1.get_token().is_some());
        assert!(!token_cache1.need_update_token());
        std::thread::sleep(std::time::Duration::from_secs(2));
        assert!(token_cache1.need_update_token());
        assert!(token_cache1.get_token().is_none());
    }

    #[test]
    #[cfg(feature = "failpoints")]
    fn test_concurrently_refresh_token_cache() {
        let token_cache = super::TokenCache::default();
        assert!(!token_cache.is_fresh());
        let (token1, token2) = (token_cache.clone(), token_cache.clone());
        let thread1 = std::thread::spawn(move || {
            assert!(token1.need_update_token());
            std::thread::sleep(std::time::Duration::from_secs(2));
        });
        let thread2 = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            let _f =
                fail::FailGuard::new("on_config_default_refresh_token_timeout", "return").unwrap();
            // forcely exit as timeout.
            assert!(token2.need_update_token());
        });
        let thread3 = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(2023)); // sleep enough
            // mock that token is refreshed.
            let _f = fail::FailGuard::new("on_is_fresh_return_true", "return").unwrap();
            assert!(!token_cache.need_update_token());
        });
        thread1.join().unwrap();
        thread2.join().unwrap();
        thread3.join().unwrap();
    }
}
