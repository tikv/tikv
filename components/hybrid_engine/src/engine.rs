// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, MemoryEngine, Peekable, ReadOptions, Result, SyncMutable};

use crate::snapshot::HybridSnapshot;

#[derive(Clone, Debug)]
pub struct HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    pub disk_engine: EK,
    pub memory_engine: EM,
}

impl<EK, EM> KvEngine for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type Snapshot = HybridSnapshot<EK, EM>;

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

impl<EK, EM> Peekable for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
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

impl<EK, EM> SyncMutable for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
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
