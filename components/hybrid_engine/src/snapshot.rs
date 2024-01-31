// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    ops::Deref,
};

use engine_traits::{
    CfNamesExt, DbVector, IterOptions, Iterable, KvEngine, Peekable, RangeCacheEngine, ReadOptions,
    Result, Snapshot, SnapshotMiscExt, CF_DEFAULT,
};

use crate::engine_iterator::HybridEngineIterator;

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
        unimplemented!()
    }
}

/// TODO: May be possible to replace this with an Either.
pub struct HybridDbVector(Box<dyn DbVector>);

impl DbVector for HybridDbVector {}

impl Deref for HybridDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for HybridDbVector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for HybridDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

impl<EK, EC> Peekable for HybridEngineSnapshot<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type DbVector = HybridDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        self.region_cache_snap.as_ref().map_or_else(
            || {
                self.disk_snap
                    .get_value_cf_opt(opts, cf, key)
                    .map(|r| r.map(|e| HybridDbVector(Box::new(e))))
            },
            |cache_snapshot| {
                cache_snapshot
                    .get_value_cf_opt(opts, cf, key)
                    .map(|r| r.map(|e| HybridDbVector(Box::new(e))))
            },
        )
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
