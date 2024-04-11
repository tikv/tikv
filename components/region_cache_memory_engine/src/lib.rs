// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(let_chains)]
#![feature(slice_pattern)]

use std::time::Duration;

use tikv_util::config::ReadableSize;

mod background;
mod engine;
pub mod keys;
mod memory_controller;
mod metrics;
pub mod perf_context;
pub mod range_manager;
mod read;
pub mod region_label;
mod write_batch;

pub use background::{BackgroundRunner, GcTask};
pub use engine::RangeCacheMemoryEngine;
pub use write_batch::RangeCacheWriteBatch;

pub struct EngineConfig {
    gc_interval: Duration,
    soft_limit_threshold: usize,
    hard_limit_threshold: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            gc_interval: Duration::from_secs(180),
            soft_limit_threshold: ReadableSize::gb(10).0 as usize,
            hard_limit_threshold: ReadableSize::gb(15).0 as usize,
        }
    }
}

impl EngineConfig {
    pub fn new(
        gc_interval: Duration,
        soft_limit_threshold: usize,
        hard_limit_threshold: usize,
    ) -> Self {
        Self {
            gc_interval,
            soft_limit_threshold,
            hard_limit_threshold,
        }
    }

    pub fn config_for_test() -> EngineConfig {
        EngineConfig::new(
            Duration::from_secs(600),
            ReadableSize::gb(1).0 as usize,
            ReadableSize::gb(2).0 as usize,
        )
    }
}
