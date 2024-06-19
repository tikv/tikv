// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_rocks::{util::new_engine, RocksEngine};
use engine_traits::{RangeCacheEngine, Result, CF_DEFAULT, CF_LOCK, CF_WRITE};
use region_cache_memory_engine::{
    RangeCacheEngineConfig, RangeCacheEngineContext, RangeCacheMemoryEngine,
};
use tempfile::{Builder, TempDir};
use tikv_util::config::VersionTrack;

use crate::HybridEngine;

/// Create a [`HybridEngine`] using temporary storage in `prefix`.
/// Once the memory engine is created, runs `configure_memory_engine_fn`.
/// Returns the handle to temporary directory and HybridEngine.
/// # Example
///
/// ```
/// use hybrid_engine::util::hybrid_engine_for_tests;
/// let (_path, _hybrid_engine) = hybrid_engine_for_tests("temp", |memory_engine| {
///     let range = engine_traits::CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
///     memory_engine.new_range(range.clone());
///     {
///         let mut core = memory_engine.core().write().unwrap();
///         core.mut_range_manager().set_range_readable(&range, true);
///         core.mut_range_manager().set_safe_ts(&range, 10);
///     }
/// })
/// .unwrap();
/// ```
pub fn hybrid_engine_for_tests<F>(
    prefix: &str,
    config: RangeCacheEngineConfig,
    configure_memory_engine_fn: F,
) -> Result<(TempDir, HybridEngine<RocksEngine, RangeCacheMemoryEngine>)>
where
    F: FnOnce(&RangeCacheMemoryEngine),
{
    let path = Builder::new().prefix(prefix).tempdir()?;
    let disk_engine = new_engine(
        path.path().to_str().unwrap(),
        &[CF_DEFAULT, CF_LOCK, CF_WRITE],
    )?;
    let mut memory_engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(
        Arc::new(VersionTrack::new(config)),
    ));
    memory_engine.set_disk_engine(disk_engine.clone());
    configure_memory_engine_fn(&memory_engine);
    let handler = std::thread::spawn(|| {});
    let hybrid_engine = HybridEngine::new(disk_engine, memory_engine, handler);
    Ok((path, hybrid_engine))
}
