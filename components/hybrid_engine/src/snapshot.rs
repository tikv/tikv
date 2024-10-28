// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
};

use engine_traits::{
    is_data_cf, CfNamesExt, IterOptions, Iterable, KvEngine, Peekable, ReadOptions,
    RegionCacheEngine, Result, Snapshot, SnapshotMiscExt, CF_DEFAULT,
};
use in_memory_engine::RegionCacheMemoryEngine;
use raftstore::coprocessor::ObservedSnapshot;

use crate::{
    db_vector::HybridDbVector, engine_iterator::HybridEngineIterator,
    observer::RegionCacheSnapshotPin,
};

pub struct HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    disk_snap: EK::Snapshot,
    region_cache_snap: Option<EC::Snapshot>,
}

impl<EK, EC> HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    pub fn new(disk_snap: EK::Snapshot, region_cache_snap: Option<EC::Snapshot>) -> Self {
        HybridEngineSnapshot {
            disk_snap,
            region_cache_snap,
        }
    }

    pub fn region_cache_snapshot_available(&self) -> bool {
        self.region_cache_snap.is_some()
    }

    pub fn region_cache_snap(&self) -> Option<&EC::Snapshot> {
        self.region_cache_snap.as_ref()
    }

    pub fn disk_snap(&self) -> &EK::Snapshot {
        &self.disk_snap
    }
}

impl<EK> HybridEngineSnapshot<EK, RegionCacheMemoryEngine>
where
    EK: KvEngine,
{
    pub fn from_observed_snapshot(
        disk_snap: EK::Snapshot,
        snap_pin: Option<Box<dyn ObservedSnapshot>>,
    ) -> Self {
        let mut region_cache_snap = None;
        if let Some(snap_pin) = snap_pin {
            let snap_any: Box<dyn Any> = snap_pin;
            let mut region_cache_snap_pin: Box<RegionCacheSnapshotPin> =
                snap_any.downcast().unwrap();
            region_cache_snap = region_cache_snap_pin.take();
        }
        HybridEngineSnapshot {
            disk_snap,
            region_cache_snap,
        }
    }
}

impl<EK, EC> Snapshot for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn in_memory_engine_hit(&self) -> bool {
        self.region_cache_snap.is_some()
    }
}

impl<EK, EC> Debug for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Hybrid Engine Snapshot Impl")
    }
}

impl<EK, EC> Iterable for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type Iterator = HybridEngineIterator<EK, EC>;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        Ok(match self.region_cache_snap() {
            Some(region_cache_snap) if is_data_cf(cf) => {
                HybridEngineIterator::region_cache_engine_iterator(
                    region_cache_snap.iterator_opt(cf, opts)?,
                )
            }
            _ => HybridEngineIterator::disk_engine_iterator(self.disk_snap.iterator_opt(cf, opts)?),
        })
    }
}

impl<EK, EC> Peekable for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type DbVector = HybridDbVector<EK, EC>;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        match self.region_cache_snap() {
            Some(region_cache_snap) if is_data_cf(cf) => {
                Self::DbVector::try_from_cache_snap(region_cache_snap, opts, cf, key)
            }
            _ => Self::DbVector::try_from_disk_snap(&self.disk_snap, opts, cf, key),
        }
    }
}

impl<EK, EC> CfNamesExt for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn cf_names(&self) -> Vec<&str> {
        self.disk_snap.cf_names()
    }
}

impl<EK, EC> SnapshotMiscExt for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn sequence_number(&self) -> u64 {
        self.disk_snap.sequence_number()
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{
        CacheRegion, IterOptions, Iterable, Iterator, Mutable, WriteBatch, WriteBatchExt,
        CF_DEFAULT,
    };
    use in_memory_engine::{test_util::new_region, InMemoryEngineConfig, RegionCacheStatus};
    use raftstore::coprocessor::WriteBatchWrapper;

    use crate::{
        engine::SnapshotContext, observer::RegionCacheWriteBatchObserver,
        util::hybrid_engine_for_tests,
    };

    #[test]
    fn test_iterator() {
        let region = new_region(1, b"", b"z");
        let cache_region = CacheRegion::from_region(&region);
        let mut iter_opt = IterOptions::default();
        iter_opt.set_upper_bound(&cache_region.end, 0);
        iter_opt.set_lower_bound(&cache_region.start, 0);

        let region_clone = region.clone();
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            InMemoryEngineConfig::config_for_test(),
            move |memory_engine| {
                memory_engine.new_region(region_clone);
                memory_engine.core().region_manager().set_safe_point(1, 5);
            },
        )
        .unwrap();
        let snap = hybrid_engine.new_snapshot(None);
        {
            let mut iter = snap.iterator_opt(CF_DEFAULT, iter_opt.clone()).unwrap();
            assert!(!iter.seek_to_first().unwrap());
        }
        let engine = hybrid_engine.region_cache_engine().clone();
        let observer = RegionCacheWriteBatchObserver::new(engine.clone());
        let mut ob_wb = observer.new_observable_write_batch();
        ob_wb.cache_write_batch.prepare_for_region(&region);
        ob_wb
            .cache_write_batch
            .set_region_cache_status(RegionCacheStatus::Cached);
        let mut write_batch = WriteBatchWrapper::new(
            hybrid_engine.disk_engine().write_batch(),
            Some(Box::new(ob_wb)),
        );
        write_batch.put(b"zhello", b"world").unwrap();
        let seq = write_batch.write().unwrap();
        assert!(seq > 0);
        let ctx = SnapshotContext {
            region: Some(cache_region.clone()),
            read_ts: 10,
        };
        let snap = hybrid_engine.new_snapshot(Some(ctx));
        {
            let mut iter = snap.iterator_opt(CF_DEFAULT, iter_opt).unwrap();
            assert!(iter.seek_to_first().unwrap());
            let actual_key = iter.key();
            let actual_value = iter.value();
            assert_eq!(actual_key, b"zhello");
            assert_eq!(actual_value, b"world");
        }
    }
}
