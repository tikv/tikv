// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Iterator, KvEngine, RegionCacheEngine, Result};
use tikv_util::Either;

pub struct HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    iter: Either<EK::Iterator, EC::Iterator>,
}

impl<EK, EC> Iterator for HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn seek(&mut self, _key: &[u8]) -> Result<bool> {
        unimplemented!()
    }

    fn seek_for_prev(&mut self, _key: &[u8]) -> Result<bool> {
        unimplemented!()
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn prev(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn next(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    fn valid(&self) -> Result<bool> {
        unimplemented!()
    }
}
