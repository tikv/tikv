// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate tikv_util;

mod config;
pub use config::*;
mod errors;
pub use errors::*;
mod tso;
pub use tso::*;
mod metrics;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
pub use metrics::*;
#[cfg(any(test, feature = "testexport"))]
use test_pd_client::TestPdClient;
use txn_types::TimeStamp;

pub use crate::errors::Result;
/// Trait of causal timestamp provider.
#[async_trait]
#[enum_dispatch]
pub trait CausalTsProvider: Send + Sync {
    /// Get a new timestamp.
    async fn async_get_ts(&self) -> Result<TimeStamp>;

    /// Flush (cached) timestamps and return first timestamp to keep causality
    /// on some events, such as "leader transfer".
    async fn async_flush(&self) -> Result<TimeStamp>;
}

#[enum_dispatch(CausalTsProvider)]
pub enum CausalTsProviderImpl {
    BatchTsoProvider(BatchTsoProvider<pd_client::RpcClient>),
    #[cfg(any(test, feature = "testexport"))]
    BatchTsoProviderTest(BatchTsoProvider<TestPdClient>),
    TestProvider(tests::TestProvider),
}

pub mod tests {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    use super::*;

    /// for TEST purpose.
    #[derive(Clone)]
    pub struct TestProvider {
        ts: Arc<AtomicU64>,
    }

    impl Default for TestProvider {
        fn default() -> Self {
            Self {
                // Note that `ts` should not start from 0. See `ApiV2::encode_raw_key`.
                ts: Arc::new(AtomicU64::new(100)),
            }
        }
    }

    #[async_trait]
    impl CausalTsProvider for TestProvider {
        async fn async_get_ts(&self) -> Result<TimeStamp> {
            Ok(self.ts.fetch_add(1, Ordering::Relaxed).into())
        }

        // This is used for unit test. Add 100 from current.
        // Do not modify this value as several test cases depend on it.
        async fn async_flush(&self) -> Result<TimeStamp> {
            self.ts.fetch_add(100, Ordering::Relaxed);
            self.async_get_ts().await
        }
    }
}
