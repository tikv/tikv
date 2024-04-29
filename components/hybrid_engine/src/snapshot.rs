// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};

use engine_traits::{
    is_data_cf, CfNamesExt, IterOptions, Iterable, KvEngine, Peekable, RangeCacheEngine,
    ReadOptions, Result, Snapshot, SnapshotMiscExt, CF_DEFAULT,
};

use crate::{db_vector::HybridDbVector, engine_iterator::HybridEngineIterator};

pub struct HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    disk_snap: EK::Snapshot,
    region_cache_snap: Option<EC::Snapshot>,
}

impl<EK, EC> HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
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

impl<EK, EC> Snapshot for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
}

impl<EK, EC> Debug for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Hybrid Engine Snapshot Impl")
    }
}

impl<EK, EC> Iterable for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
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
    EC: RangeCacheEngine,
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
    EC: RangeCacheEngine,
{
    fn cf_names(&self) -> Vec<&str> {
        self.disk_snap.cf_names()
    }
}

impl<EK, EC> SnapshotMiscExt for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn sequence_number(&self) -> u64 {
        self.disk_snap.sequence_number()
    }
}

#[cfg(test)]
mod tests {

    use engine_traits::{
        CacheRange, IterOptions, Iterable, Iterator, KvEngine, Mutable, SnapshotContext,
        WriteBatch, WriteBatchExt, CF_DEFAULT,
    };
    use region_cache_memory_engine::{RangeCacheEngineConfig, RangeCacheStatus};

    use crate::util::hybrid_engine_for_tests;

    #[test]
    fn test_iterator() {
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        let mut iter_opt = IterOptions::default();
        iter_opt.set_upper_bound(&range.end, 0);
        iter_opt.set_lower_bound(&range.start, 0);

        let range_clone = range.clone();
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            RangeCacheEngineConfig::config_for_test(),
            move |memory_engine| {
                memory_engine.new_range(range_clone.clone());
                {
                    let mut core = memory_engine.core().write();
                    core.mut_range_manager().set_safe_point(&range_clone, 5);
                }
            },
        )
        .unwrap();
        let snap = hybrid_engine.snapshot(None);
        {
            let mut iter = snap.iterator_opt(CF_DEFAULT, iter_opt.clone()).unwrap();
            assert!(!iter.seek_to_first().unwrap());
        }
        let mut write_batch = hybrid_engine.write_batch();
        write_batch
            .cache_write_batch
            .set_range_cache_status(RangeCacheStatus::Cached);
        write_batch.put(b"hello", b"world").unwrap();
        let seq = write_batch.write().unwrap();
        assert!(seq > 0);
        let ctx = SnapshotContext {
            range: Some(range.clone()),
            read_ts: 10,
        };
        let snap = hybrid_engine.snapshot(Some(ctx));
        {
            let mut iter = snap.iterator_opt(CF_DEFAULT, iter_opt).unwrap();
            assert!(iter.seek_to_first().unwrap());
            let actual_key = iter.key();
            let actual_value = iter.value();
            assert_eq!(actual_key, b"hello");
            assert_eq!(actual_value, b"world");
        }
    }
}
