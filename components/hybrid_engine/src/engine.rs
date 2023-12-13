// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Peekable, ReadOptions, RegionCacheEngine, Result, SyncMutable};

use crate::snapshot::HybridEngineSnapshot;

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
}

// todo: implement KvEngine methods as well as it's super traits.
impl<EK, EC> KvEngine for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type Snapshot = HybridEngineSnapshot<EK, EC>;

    fn snapshot(&self) -> Self::Snapshot {
        unimplemented!()
    }

    fn sync(&self) -> engine_traits::Result<()> {
        unimplemented!()
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        unimplemented!()
    }

    #[cfg(feature = "testexport")]
    fn inner_refcount(&self) -> usize {
        unimplemented!()
    }
}

impl<EK, EC> Peekable for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type DbVector = EK::DbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        unimplemented!()
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        unimplemented!()
    }
}

impl<EK, EC> SyncMutable for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        unimplemented!()
    }
}
