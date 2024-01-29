// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(dead_code)]
#![allow(unused_variables)]
#![feature(let_chains)]
#![feature(slice_pattern)]

mod engine;
mod gc;
pub mod keys;
pub use engine::RangeCacheMemoryEngine;
pub mod range_manager;
mod write_batch;
pub use write_batch::RangeCacheWriteBatch;
mod memory_limiter;
