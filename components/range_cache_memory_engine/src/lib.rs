// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(assert_matches)]
#![feature(let_chains)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(slice_pattern)]
#![feature(trait_alias)]

use std::sync::Arc;

use futures::future::ready;
use pd_client::PdClient;
use tikv_util::config::VersionTrack;

mod background;
pub mod config;
mod engine;
mod keys;
mod memory_controller;
mod metrics;
mod perf_context;
#[cfg(test)]
mod prop_test;
mod range_manager;
mod range_stats;
mod read;
mod region_label;
mod statistics;
pub mod test_util;
mod write_batch;

pub use background::{BackgroundRunner, BackgroundTask, GcTask};
pub use config::RangeCacheEngineConfig;
pub use engine::{RangeCacheMemoryEngine, SkiplistHandle};
pub use keys::{
    decode_key, encode_key_for_boundary_without_mvcc, encoding_for_filter, InternalBytes,
    InternalKey, ValueType,
};
pub use metrics::flush_range_cache_engine_statistics;
pub use range_manager::{RangeCacheStatus, RegionState};
pub use read::RangeCacheSnapshot;
pub use statistics::Statistics as RangeCacheMemoryEngineStatistics;
use txn_types::TimeStamp;
pub use write_batch::RangeCacheWriteBatch;

pub struct RangeCacheEngineContext {
    config: Arc<VersionTrack<RangeCacheEngineConfig>>,
    statistics: Arc<RangeCacheMemoryEngineStatistics>,
    pd_client: Arc<dyn PdClient>,
}

impl RangeCacheEngineContext {
    pub fn new(
        config: Arc<VersionTrack<RangeCacheEngineConfig>>,
        pd_client: Arc<dyn PdClient>,
    ) -> RangeCacheEngineContext {
        RangeCacheEngineContext {
            config,
            statistics: Arc::default(),
            pd_client,
        }
    }

    pub fn new_for_tests(
        config: Arc<VersionTrack<RangeCacheEngineConfig>>,
    ) -> RangeCacheEngineContext {
        struct MockPdClient;
        impl PdClient for MockPdClient {
            fn get_tso(&self) -> pd_client::PdFuture<txn_types::TimeStamp> {
                Box::pin(ready(Ok(TimeStamp::compose(TimeStamp::physical_now(), 0))))
            }
        }
        RangeCacheEngineContext {
            config,
            statistics: Arc::default(),
            pd_client: Arc::new(MockPdClient),
        }
    }

    pub fn statistics(&self) -> Arc<RangeCacheMemoryEngineStatistics> {
        self.statistics.clone()
    }
}
