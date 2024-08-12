// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_rocks::{util::new_engine, RocksEngine};
use engine_traits::{RangeCacheEngine, Result, CF_DEFAULT, CF_LOCK, CF_WRITE};
use range_cache_memory_engine::{
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
/// use range_cache_memory_engine::RangeCacheEngineConfig;
/// let mut config = RangeCacheEngineConfig::default();
/// config.enabled = true;
/// let (_path, _hybrid_engine) = hybrid_engine_for_tests("temp", config, |memory_engine| {
///     let region = {
///         let mut r = kvproto::metapb::Region::default();
///         r.id = 1;
///         r.start_key = b"".into();
///         r.end_key = b"z".into();
///         r.mut_peers().push(kvproto::metapb::Peer::default());
///         r
///     };
///     memory_engine.new_region(region.clone());
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
    let hybrid_engine = HybridEngine::new(disk_engine, memory_engine);
    Ok((path, hybrid_engine))
}
