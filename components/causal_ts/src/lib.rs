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
pub use metrics::*;
mod observer;
pub use observer::*;

use crate::errors::Result;
use txn_types::TimeStamp;

/// Trait of causal timestamp provider.
pub trait CausalTsProvider: Send + Sync {
    /// Get a new timestamp.
    fn get_ts(&self) -> Result<TimeStamp>;

    /// Flush (cached) timestamps to keep causality on some events, such as "leader transfer".
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

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
    }
}
