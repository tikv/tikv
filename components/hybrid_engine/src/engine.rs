// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    FailedReason, KvEngine, Mutable, Peekable, RangeCacheEngine, ReadOptions, Result,
    SnapshotContext, SnapshotMiscExt, SyncMutable, WriteBatch, WriteBatchExt,
};

use crate::{
    metrics::{
        RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC, SNAPSHOT_TYPE_COUNT_STATIC,
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
    EC: RangeCacheEngine,
{
    disk_engine: EK,
    range_cache_engine: EC,
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    pub fn disk_engine(&self) -> &EK {
        &self.disk_engine
    }

    pub fn mut_disk_engine(&mut self) -> &mut EK {
        &mut self.disk_engine
    }

    pub fn range_cache_engine(&self) -> &EC {
        &self.range_cache_engine
    }

    pub fn mut_range_cache_engine(&mut self) -> &mut EC {
        &mut self.range_cache_engine
    }
}

pub fn new_in_memory_snapshot<EC: RangeCacheEngine>(
    range_cache_engine: &EC,
    ctx: SnapshotContext,
    sequence_number: u64,
) -> Option<EC::Snapshot> {
    match range_cache_engine.snapshot(ctx.region.unwrap(), ctx.read_ts, sequence_number) {
        Ok(snap) => {
            SNAPSHOT_TYPE_COUNT_STATIC.range_cache_engine.inc();
            Some(snap)
        }
        Err(FailedReason::TooOldRead) => {
            RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                .too_old_read
                .inc();
            SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
            None
        }
        Err(FailedReason::NotCached) => {
            RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                .not_cached
                .inc();
            SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
            None
        }
        Err(FailedReason::EpochNotMatch) => {
            RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
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
    EC: RangeCacheEngine,
{
    pub fn new(disk_engine: EK, range_cache_engine: EC) -> Self {
        Self {
            disk_engine,
            range_cache_engine,
        }
    }
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
    HybridEngine<EK, EC>: WriteBatchExt,
{
    fn sync_write<F>(&self, key: &[u8], f: F) -> Result<()>
    where
        F: FnOnce(&mut <Self as WriteBatchExt>::WriteBatch) -> Result<()>,
    {
        let mut batch = self.write_batch();
        if let Some(region) = self.range_cache_engine.get_region_for_key(key) {
            batch.prepare_for_region(region);
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
    EC: RangeCacheEngine,
    HybridEngine<EK, EC>: WriteBatchExt,
{
    type Snapshot = HybridEngineSnapshot<EK, EC>;

    fn snapshot(&self, ctx: Option<SnapshotContext>) -> Self::Snapshot {
        let disk_snap = self.disk_engine.snapshot(ctx.clone());
        let range_cache_snap = if !self.range_cache_engine.enabled() {
            None
        } else if let Some(ctx) = ctx {
            new_in_memory_snapshot(&self.range_cache_engine, ctx, disk_snap.sequence_number())
        } else {
            RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                .no_read_ts
                .inc();
            SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
            None
        };
        HybridEngineSnapshot::new(disk_snap, range_cache_snap)
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
    EC: RangeCacheEngine,
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
    EC: RangeCacheEngine,
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
    use engine_traits::{CacheRegion, KvEngine, SnapshotContext, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use range_cache_memory_engine::{
        config::RangeCacheConfigManager, test_util::new_region, RangeCacheEngineConfig,
        RangeCacheEngineContext, RangeCacheMemoryEngine,
    };
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
        let config = Arc::new(VersionTrack::new(RangeCacheEngineConfig::config_for_test()));
        let memory_engine =
            RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(config.clone()));

        let region = new_region(1, b"k00", b"k10");
        let range = CacheRegion::from_region(&region);
        memory_engine.new_region(region.clone());
        {
            let mut core = memory_engine.core().write();
            core.mut_range_manager().set_safe_point(region.id, 10);
        }

        let hybrid_engine = HybridEngine::new(disk_engine, memory_engine.clone());
        let s = hybrid_engine.snapshot(None);
        assert!(!s.range_cache_snapshot_available());

        let mut snap_ctx = SnapshotContext {
            read_ts: 15,
            region: Some(range.clone()),
        };
        let s = hybrid_engine.snapshot(Some(snap_ctx.clone()));
        assert!(s.range_cache_snapshot_available());

        snap_ctx.read_ts = 5;
        let s = hybrid_engine.snapshot(Some(snap_ctx.clone()));
        assert!(!s.range_cache_snapshot_available());

        let mut config_manager = RangeCacheConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enabled"), ConfigValue::Bool(false));
        config_manager.dispatch(config_change).unwrap();
        assert!(!config.value().enabled);
        snap_ctx.read_ts = 15;
        let s = hybrid_engine.snapshot(Some(snap_ctx));
        assert!(!s.range_cache_snapshot_available());
    }
}
