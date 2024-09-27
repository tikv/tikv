// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(assert_matches)]
#![feature(let_chains)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(slice_pattern)]
#![feature(trait_alias)]

use std::{sync::Arc, time::Duration};

use futures::future::ready;
use online_config::OnlineConfig;
use pd_client::PdClient;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tikv_util::config::{ReadableDuration, ReadableSize, VersionTrack};

mod background;
pub mod config;
mod cross_check;
mod engine;
mod keys;
mod memory_controller;
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

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, OnlineConfig)]
#[serde(default, rename_all = "kebab-case")]
pub struct InMemoryEngineConfig {
    pub enabled: bool,
    pub gc_interval: ReadableDuration,
    pub load_evict_interval: ReadableDuration,
    // TODO(SpadeA): ultimately we only expose one memory limit to user.
    // When memory usage reaches this amount, no further load will be performed.
    pub stop_load_limit_threshold: Option<ReadableSize>,
    // When memory usage reaches this amount, we start to pick some regions to evict.
    pub soft_limit_threshold: Option<ReadableSize>,
    pub hard_limit_threshold: Option<ReadableSize>,
    pub expected_region_size: Option<ReadableSize>,
    // used in getting top regions to filter those with less mvcc amplification. Here, we define
    // mvcc amplification to be '(next + prev) / processed_keys'.
    pub mvcc_amplification_threshold: usize,
    // Cross check is only for test usage and should not be turned on in production
    // environment. Interval 0 means it is turned off, which is the default value.
    #[online_config(skip)]
    pub cross_check_interval: ReadableDuration,
}

impl Default for InMemoryEngineConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gc_interval: ReadableDuration(Duration::from_secs(180)),
            stop_load_limit_threshold: None,
            // Each load/evict operation should run within five minutes.
            load_evict_interval: ReadableDuration(Duration::from_secs(300)),
            soft_limit_threshold: None,
            hard_limit_threshold: None,
            expected_region_size: None,
            mvcc_amplification_threshold: 100,
            cross_check_interval: ReadableDuration(Duration::from_secs(0)),
        }
    }
}

impl InMemoryEngineConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.enabled {
            return Ok(());
        }

        Ok(self.sanitize()?)
    }

    pub fn sanitize(&mut self) -> Result<(), Error> {
        if self.soft_limit_threshold.is_none() || self.hard_limit_threshold.is_none() {
            return Err(Error::InvalidArgument(
                "soft-limit-threshold or hard-limit-threshold not set".to_string(),
            ));
        }

        if self.stop_load_limit_threshold.is_none() {
            self.stop_load_limit_threshold = self.soft_limit_threshold;
        }

        if self.stop_load_limit_threshold.as_ref().unwrap()
            > self.soft_limit_threshold.as_ref().unwrap()
        {
            return Err(Error::InvalidArgument(format!(
                "stop-load-limit-threshold {:?} is larger to soft-limit-threshold {:?}",
                self.stop_load_limit_threshold.as_ref().unwrap(),
                self.soft_limit_threshold.as_ref().unwrap()
            )));
        }

        if self.soft_limit_threshold.as_ref().unwrap()
            >= self.hard_limit_threshold.as_ref().unwrap()
        {
            return Err(Error::InvalidArgument(format!(
                "soft-limit-threshold {:?} is larger or equal to hard-limit-threshold {:?}",
                self.soft_limit_threshold.as_ref().unwrap(),
                self.hard_limit_threshold.as_ref().unwrap()
            )));
        }

        Ok(())
    }

    pub fn stop_load_limit_threshold(&self) -> usize {
        self.stop_load_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn soft_limit_threshold(&self) -> usize {
        self.soft_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn hard_limit_threshold(&self) -> usize {
        self.hard_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn expected_region_size(&self) -> usize {
        self.expected_region_size.map_or(
            raftstore::coprocessor::config::SPLIT_SIZE.0 as usize,
            |r: ReadableSize| r.0 as usize,
        )
    }

    pub fn config_for_test() -> InMemoryEngineConfig {
        InMemoryEngineConfig {
            enabled: true,
            gc_interval: ReadableDuration(Duration::from_secs(180)),
            load_evict_interval: ReadableDuration(Duration::from_secs(300)), /* Should run within
                                                                              * five minutes */
            stop_load_limit_threshold: Some(ReadableSize::gb(1)),
            soft_limit_threshold: Some(ReadableSize::gb(1)),
            hard_limit_threshold: Some(ReadableSize::gb(2)),
            expected_region_size: Some(ReadableSize::mb(20)),
            mvcc_amplification_threshold: 10,
            cross_check_interval: ReadableDuration(Duration::from_secs(0)),
        }
    }
}

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
