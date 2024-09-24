// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_rocks::{util::new_engine, RocksEngine};
use engine_traits::{RegionCacheEngine, Result, CF_DEFAULT, CF_LOCK, CF_WRITE};
use in_memory_engine::{InMemoryEngineConfig, InMemoryEngineContext, RegionCacheMemoryEngine};
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
/// use in_memory_engine::{test_util::new_region, RegionCacheEngineConfig};
/// let mut config = RegionCacheEngineConfig::default();
/// config.enabled = true;
/// let (_path, _hybrid_engine) = hybrid_engine_for_tests("temp", config, |memory_engine| {
///     let region = new_region(1, b"", b"z");
///     memory_engine.new_region(region);
/// })
/// .unwrap();
/// ```
pub fn hybrid_engine_for_tests<F>(
    prefix: &str,
    config: InMemoryEngineConfig,
    configure_memory_engine_fn: F,
) -> Result<(TempDir, HybridEngine<RocksEngine, RegionCacheMemoryEngine>)>
where
    F: FnOnce(&RegionCacheMemoryEngine),
{
    let path = Builder::new().prefix(prefix).tempdir()?;
    let disk_engine = new_engine(
        path.path().to_str().unwrap(),
        &[CF_DEFAULT, CF_LOCK, CF_WRITE],
    )?;
    let mut memory_engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(
        Arc::new(VersionTrack::new(config)),
    ));
    memory_engine.set_disk_engine(disk_engine.clone());
    configure_memory_engine_fn(&memory_engine);
    let hybrid_engine = HybridEngine::new(disk_engine, memory_engine);
    Ok((path, hybrid_engine))
}
