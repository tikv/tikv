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
pub use observer::*;
use txn_types::TimeStamp;

pub use crate::errors::Result;

/// Trait of causal timestamp provider.
pub trait CausalTsProvider: Send + Sync {
    /// Get a new timestamp.
    fn get_ts(&self) -> Result<TimeStamp>;

    /// Flush (cached) timestamps to keep causality on some events, such as
    /// "leader transfer".
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub trait RawTsTracker: Send + Sync + Clone {
    fn track_ts(&self, region_id: u64, ts: TimeStamp) -> Result<()>;
}

pub mod tests {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    use super::*;

    /// for TEST purpose.
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

    impl CausalTsProvider for TestProvider {
        fn get_ts(&self) -> Result<TimeStamp> {
            Ok(self.ts.fetch_add(1, Ordering::Relaxed).into())
        }

        // This is used for unit test. Add 100 from current.
        // Do not modify this value as several test cases depend on it.
        fn flush(&self) -> Result<()> {
            self.ts.fetch_add(100, Ordering::Relaxed);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    pub struct DummyRawTsTracker {}

    impl RawTsTracker for DummyRawTsTracker {
        fn track_ts(&self, _region_id: u64, _ts: TimeStamp) -> Result<()> {
            Ok(())
        }
    }
}
