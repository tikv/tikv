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
mod cross_check;
mod engine;
mod keys;
mod memory_controller;
#[cfg(test)]
mod memory_usage_test;
mod metrics;
mod perf_context;
#[cfg(test)]
mod prop_test;
mod read;
mod region_label;
mod region_manager;
mod region_stats;
mod statistics;
pub mod test_util;
mod write_batch;

pub use background::{BackgroundRunner, BackgroundTask, GcTask};
pub use config::InMemoryEngineConfig;
pub use engine::{RegionCacheMemoryEngine, SkiplistHandle};
pub use keys::{
    decode_key, encode_key_for_boundary_without_mvcc, encoding_for_filter, InternalBytes,
    InternalKey, ValueType,
};
pub use metrics::flush_in_memory_engine_statistics;
pub use read::RegionCacheSnapshot;
pub use region_manager::{RegionCacheStatus, RegionState};
pub use statistics::Statistics as InMemoryEngineStatistics;
use txn_types::TimeStamp;
pub use write_batch::RegionCacheWriteBatch;

#[derive(Clone)]
pub struct InMemoryEngineContext {
    config: Arc<VersionTrack<InMemoryEngineConfig>>,
    statistics: Arc<InMemoryEngineStatistics>,
    pd_client: Arc<dyn PdClient>,
}

impl InMemoryEngineContext {
    pub fn new(
        config: Arc<VersionTrack<InMemoryEngineConfig>>,
        pd_client: Arc<dyn PdClient>,
    ) -> InMemoryEngineContext {
        InMemoryEngineContext {
            config,
            statistics: Arc::default(),
            pd_client,
        }
    }

    pub fn new_for_tests(config: Arc<VersionTrack<InMemoryEngineConfig>>) -> InMemoryEngineContext {
        struct MockPdClient;
        impl PdClient for MockPdClient {
            fn get_tso(&self) -> pd_client::PdFuture<txn_types::TimeStamp> {
                Box::pin(ready(Ok(TimeStamp::compose(TimeStamp::physical_now(), 0))))
            }
        }
        InMemoryEngineContext {
            config,
            statistics: Arc::default(),
            pd_client: Arc::new(MockPdClient),
        }
    }

    pub fn pd_client(&self) -> Arc<dyn PdClient> {
        self.pd_client.clone()
    }

    pub fn config(&self) -> &Arc<VersionTrack<InMemoryEngineConfig>> {
        &self.config
    }

    pub fn statistics(&self) -> Arc<InMemoryEngineStatistics> {
        self.statistics.clone()
    }
}
