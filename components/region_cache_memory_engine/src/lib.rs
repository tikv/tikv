// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(let_chains)]
#![feature(slice_pattern)]

mod background;
mod engine;
pub mod keys;
pub mod region_label;
pub use engine::RangeCacheMemoryEngine;
pub mod range_manager;
mod write_batch;
pub use write_batch::RangeCacheWriteBatch;
mod memory_limiter;
pub use background::{BackgroundRunner, GcTask};
mod metrics;
