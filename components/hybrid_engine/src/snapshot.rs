// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};

use engine_traits::{
    CfNamesExt, IterOptions, Iterable, KvEngine, Peekable, ReadOptions, RegionCacheEngine, Result,
    Snapshot, SnapshotMiscExt,
};
use tikv_util::Either;

use crate::engine_iterator::HybridEngineIterator;

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

    pub fn snap(&self) -> Either<&EK::Snapshot, &EC::Snapshot> {
        if self.region_cache_snap.is_none() {
            Either::Left(&self.disk_snap)
        } else {
            Either::Right(self.region_cache_snap.as_ref().unwrap())
        }
    }
}

impl<EK, EC> Snapshot for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
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
        unimplemented!()
    }
}

impl<EK, EC> Peekable for HybridEngineSnapshot<EK, EC>
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
