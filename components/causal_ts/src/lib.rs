// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(map_first_last)] // For `BTreeMap::pop_first`.
#![feature(div_duration)]

#[macro_use]
extern crate tikv_util;

mod config;
pub use config::*;
mod errors;
pub use errors::*;
mod tso;
pub use tso::*;
mod metrics;
pub use metrics::*;
mod observer;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use futures::executor::block_on;
pub use observer::*;
use txn_types::TimeStamp;

pub use crate::errors::Result;
/// Trait of causal timestamp provider.
#[async_trait]
#[enum_dispatch]
pub trait CausalTsProvider: Send + Sync {
    /// Get a new timestamp.
    fn get_ts(&self) -> Result<TimeStamp> {
        block_on(self.async_get_ts())
    }

    /// Flush (cached) timestamps to keep causality on some events, such as
    /// "leader transfer".
    fn flush(&self) -> Result<()> {
        block_on(self.async_flush())
    }

    async fn async_get_ts(&self) -> Result<TimeStamp>;

    async fn async_flush(&self) -> Result<()>;
}

#[enum_dispatch(CausalTsProvider)]
pub enum CausalTsProviderImpl {
    BatchTsoProvider(BatchTsoProvider<pd_client::RpcClient>),
    #[cfg(any(test, feature = "testexport"))]
    BatchTsoProviderTest(BatchTsoProvider<test_pd_client::TestPdClient>),
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
        async fn async_flush(&self) -> Result<()> {
            self.ts.fetch_add(100, Ordering::Relaxed);
            Ok(())
        }
    }
}
