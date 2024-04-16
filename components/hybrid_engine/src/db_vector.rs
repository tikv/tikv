// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    ops::Deref,
};

use engine_traits::{DbVector, KvEngine, Peekable, RangeCacheEngine, ReadOptions, Result};
use tikv_util::Either;

pub struct HybridDbVector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    db_vec: Either<<EK::Snapshot as Peekable>::DbVector, <EC::Snapshot as Peekable>::DbVector>,
}

impl<EK, EC> DbVector for HybridDbVector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
}

impl<EK, EC> HybridDbVector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    pub fn try_from_disk_snap(
        snap: &EK::Snapshot,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self>> {
        Ok(snap
            .get_value_cf_opt(opts, cf, key)?
            .map(|e| HybridDbVector {
                db_vec: Either::Left(e),
            }))
    }

    pub fn try_from_cache_snap(
        snap: &EC::Snapshot,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self>> {
        Ok(snap
            .get_value_cf_opt(opts, cf, key)?
            .map(|e| HybridDbVector {
                db_vec: Either::Right(e),
            }))
    }
}

impl<EK, EC> Deref for HybridDbVector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self.db_vec {
            Either::Left(ref db_vec) => db_vec,
            Either::Right(ref db_vec) => db_vec,
        }
    }
}

impl<EK, EC> Debug for HybridDbVector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a, EK, EC> PartialEq<&'a [u8]> for HybridDbVector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
