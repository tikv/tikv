// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
};

use engine_traits::{
    CfNamesExt, IterOptions, Iterable, KvEngine, MemoryEngine, Peekable, ReadOptions, Result,
    Snapshot, SnapshotMiscExt,
};

use crate::engine_iterator::HybridEngineIterator;

pub struct HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    disk_snap: EK::Snapshot,

    phantom: PhantomData<EM>,
}

impl<EK, EM> Snapshot for HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
}

impl<EK, EM> Debug for HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Hybrid Engine Snapshot Impl")
    }
}

impl<EK, EM> Drop for HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn drop(&mut self) {
        unimplemented!()
    }
}

impl<EK, EM> Iterable for HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type Iterator = HybridEngineIterator<EK, EM>;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!()
    }
}

impl<EK, EM> Peekable for HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    // The trait is not supported and should not be called by `in-memory engine`
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

impl<EK, EM> CfNamesExt for HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn cf_names(&self) -> Vec<&str> {
        self.disk_snap.cf_names()
    }
}

impl<EK, EM> SnapshotMiscExt for HybridSnapshot<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn sequence_number(&self) -> u64 {
        unimplemented!()
    }
}
