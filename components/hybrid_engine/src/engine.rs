// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    CacheRegion, FailedReason, KvEngine, Mutable, Peekable, ReadOptions, RegionCacheEngine, Result,
    SnapshotContext, SnapshotMiscExt, SyncMutable, WriteBatch, WriteBatchExt,
};
use keys::DATA_PREFIX_KEY;
use kvproto::metapb::{self, RegionEpoch};

use crate::{
    metrics::{
        IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC, SNAPSHOT_TYPE_COUNT_STATIC,
    },
    snapshot::HybridEngineSnapshot,
};

/// This engine is structured with both a disk engine and an region cache
/// engine. The disk engine houses the complete database data, whereas the
/// region cache engine functions as a region cache, selectively caching certain
/// regions (in a better performance storage device such as NVME or RAM) to
/// enhance read performance. For the regions that are cached, region cache
/// engine retains all data that has not been garbage collected.
#[derive(Clone, Debug)]
pub struct HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    disk_engine: EK,
    region_cache_engine: EC,
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    pub fn disk_engine(&self) -> &EK {
        &self.disk_engine
    }

    pub fn mut_disk_engine(&mut self) -> &mut EK {
        &mut self.disk_engine
    }

    pub fn region_cache_engine(&self) -> &EC {
        &self.region_cache_engine
    }

    pub fn mut_region_cache_engine(&mut self) -> &mut EC {
        &mut self.region_cache_engine
    }
}

pub fn new_in_memory_snapshot<EC: RegionCacheEngine>(
    region_cache_engine: &EC,
    region: CacheRegion,
    read_ts: u64,
    sequence_number: u64,
) -> Option<EC::Snapshot> {
    match region_cache_engine.snapshot(region, read_ts, sequence_number) {
        Ok(snap) => {
            SNAPSHOT_TYPE_COUNT_STATIC.region_cache_engine.inc();
            Some(snap)
        }
        Err(FailedReason::TooOldRead) => {
            IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                .too_old_read
                .inc();
            SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
            None
        }
        Err(FailedReason::NotCached) => {
            IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                .not_cached
                .inc();
            SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
            None
        }
        Err(FailedReason::EpochNotMatch) => {
            IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                .epoch_not_match
                .inc();
            SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
            None
        }
    }
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    pub fn new(disk_engine: EK, region_cache_engine: EC) -> Self {
        Self {
            disk_engine,
            region_cache_engine,
        }
    }

    pub fn new_snapshot(&self, ctx: Option<SnapshotContext>) -> HybridEngineSnapshot<EK, EC> {
        let disk_snap = self.disk_engine.snapshot();
        let region_cache_snap = if !self.region_cache_engine.enabled() {
            None
        } else if let Some(ctx) = ctx {
            new_in_memory_snapshot(
                &self.region_cache_engine,
                ctx.region.unwrap(),
                ctx.read_ts,
                disk_snap.sequence_number(),
            )
        } else {
            None
        };
        HybridEngineSnapshot::new(disk_snap, region_cache_snap)
    }
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
    HybridEngine<EK, EC>: WriteBatchExt,
{
    fn sync_write<F>(&self, key: &[u8], f: F) -> Result<()>
    where
        F: FnOnce(&mut <Self as WriteBatchExt>::WriteBatch) -> Result<()>,
    {
        let mut batch = self.write_batch();
        if let Some(cached_region) = self.region_cache_engine.get_region_for_key(key) {
            // CacheRegion does not contains enough information for Region so the transfer
            // is not accurate but this method is not called in production code.
            let mut region = metapb::Region::default();
            region.set_id(cached_region.id);
            region.set_start_key(cached_region.start[DATA_PREFIX_KEY.len()..].to_vec());
            region.set_end_key(cached_region.end[DATA_PREFIX_KEY.len()..].to_vec());
            let mut epoch = RegionEpoch::default();
            epoch.version = cached_region.epoch_version;
            region.set_region_epoch(epoch);
            batch.prepare_for_region(&region);
        }
        f(&mut batch)?;
        let _ = batch.write()?;
        Ok(())
    }
}

// todo: implement KvEngine methods as well as it's super traits.
impl<EK, EC> KvEngine for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
    HybridEngine<EK, EC>: WriteBatchExt,
{
    type Snapshot = HybridEngineSnapshot<EK, EC>;

    fn snapshot(&self) -> Self::Snapshot {
        unreachable!()
    }

    fn sync(&self) -> engine_traits::Result<()> {
        self.disk_engine.sync()
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        self.disk_engine.bad_downcast()
    }

    #[cfg(feature = "testexport")]
    fn inner_refcount(&self) -> usize {
        self.disk_engine.inner_refcount()
    }
}

impl<EK, EC> Peekable for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type DbVector = EK::DbVector;

    // region cache engine only supports peekable trait in the snapshot of it
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        self.disk_engine.get_value_opt(opts, key)
    }

    // region cache engine only supports peekable trait in the snapshot of it
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        self.disk_engine.get_value_cf_opt(opts, cf, key)
    }
}

impl<EK, EC> SyncMutable for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
    HybridEngine<EK, EC>: WriteBatchExt,
{
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.sync_write(key, |b| b.put(key, value))
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.sync_write(key, |b| b.put_cf(cf, key, value))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.sync_write(key, |b| b.delete(key))
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.sync_write(key, |b| b.delete_cf(cf, key))
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.sync_write(begin_key, |b| b.delete_range(begin_key, end_key))
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.sync_write(begin_key, |b| b.delete_range_cf(cf, begin_key, end_key))
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use engine_rocks::util::new_engine;
    use engine_traits::{CacheRegion, SnapshotContext, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use in_memory_engine::{
        config::InMemoryEngineConfigManager, test_util::new_region, InMemoryEngineConfig,
        InMemoryEngineContext, RegionCacheMemoryEngine,
    };
    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use tempfile::Builder;
    use tikv_util::config::VersionTrack;

    use crate::HybridEngine;

    #[test]
    fn test_engine() {
        let path = Builder::new().prefix("temp").tempdir().unwrap();
        let disk_engine = new_engine(
            path.path().to_str().unwrap(),
            &[CF_DEFAULT, CF_LOCK, CF_WRITE],
        )
        .unwrap();
        let config = Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test()));
        let memory_engine =
            RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(config.clone()));

        let region = new_region(1, b"k00", b"k10");
        let range = CacheRegion::from_region(&region);
        memory_engine.new_region(region.clone());
        memory_engine
            .core()
            .region_manager()
            .set_safe_point(region.id, 10);

        let hybrid_engine = HybridEngine::new(disk_engine, memory_engine.clone());
        let s = hybrid_engine.new_snapshot(None);
        assert!(!s.region_cache_snapshot_available());

        let mut snap_ctx = SnapshotContext {
            read_ts: 15,
            region: Some(range.clone()),
        };
        let s = hybrid_engine.new_snapshot(Some(snap_ctx.clone()));
        assert!(s.region_cache_snapshot_available());

        snap_ctx.read_ts = 5;
        let s = hybrid_engine.new_snapshot(Some(snap_ctx.clone()));
        assert!(!s.region_cache_snapshot_available());

        let mut config_manager = InMemoryEngineConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enable"), ConfigValue::Bool(false));
        config_manager.dispatch(config_change).unwrap();
        assert!(!config.value().enable);
        snap_ctx.read_ts = 15;
        let s = hybrid_engine.new_snapshot(Some(snap_ctx));
        assert!(!s.region_cache_snapshot_available());
    }
}
