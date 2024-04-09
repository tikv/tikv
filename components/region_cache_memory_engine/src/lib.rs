// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(let_chains)]
#![feature(slice_pattern)]

mod background;
mod engine;
pub mod keys;
use std::time::Duration;

pub mod region_label;
pub use engine::RangeCacheMemoryEngine;
pub mod range_manager;
mod write_batch;
use tikv_util::config::ReadableSize;
pub use write_batch::RangeCacheWriteBatch;
mod memory_controller;
pub use background::{BackgroundRunner, GcTask};
mod metrics;

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
